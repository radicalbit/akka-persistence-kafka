package akka.persistence.kafka.journal

import akka.persistence.AtomicWrite

import scala.collection.immutable.Seq
import scala.concurrent.{ExecutionContext, Promise}
import scala.util.Try

import akka.persistence.journal.AsyncWriteJournal
import akka.serialization.{Serialization, SerializationExtension}

import kafka.producer._

import scala.concurrent.Future
import scala.concurrent.duration._

import akka.actor._
import akka.pattern.ask
import akka.persistence.PersistentRepr
import akka.persistence.kafka._
import akka.persistence.kafka.MetadataConsumer.Broker
import akka.persistence.kafka.BrokerWatcher.BrokersUpdated
import akka.util.Timeout

class KafkaJournal extends AsyncWriteJournal with MetadataConsumer {
  import context.dispatcher

  type Deletions = Map[String, (Long, Boolean)]

  val serialization = SerializationExtension(context.system)
  val config = new KafkaJournalConfig(context.system.settings.config.getConfig("kafka-journal"))

  val brokerWatcher = new BrokerWatcher(config.zookeeperConfig, self)
  var brokers = brokerWatcher.start()

  override def postStop(): Unit = {
    brokerWatcher.stop()
    super.postStop()
  }

  override def receivePluginInternal: Receive = localReceive

  private def localReceive: Receive = {
    case BrokersUpdated(newBrokers) if newBrokers != brokers =>
      brokers = newBrokers
      journalProducerConfig = config.journalProducerConfig(brokers)
      eventProducerConfig = config.eventProducerConfig(brokers)
      writers.foreach(_ ! UpdateKafkaJournalWriterConfig(writerConfig))
  }

  // --------------------------------------------------------------------------------------
  //  Journal writes
  // --------------------------------------------------------------------------------------

  var journalProducerConfig = config.journalProducerConfig(brokers)
  var eventProducerConfig = config.eventProducerConfig(brokers)

  var writers: Vector[ActorRef] = Vector.fill(config.writeConcurrency)(writer())
  val writeTimeout = Timeout(journalProducerConfig.requestTimeoutMs.millis)

  // Transient deletions only to pass TCK (persistent not supported)
  var deletions: Deletions = Map.empty

  private def asyncWriteMessagesLegacy(messages: Seq[PersistentRepr]): Future[Seq[Try[Unit]]] = {
    implicit class RichFuture[T](future: Future[T]) {
      def lift(implicit executor: ExecutionContext): Future[Try[T]] = {
        val promise = Promise[Try[T]]()
        future.onComplete(promise.success)
        promise.future
      }
    }
    // TODO: don't preserve the order of messages... we have to change KafkaJournalWriter
    val sends = messages.groupBy(_.persistenceId).map {
      case (pid, msgs) =>
        val res = writerFor(pid).ask(msgs)(writeTimeout).map(_ => ())
        List.fill(msgs.size)(res)
    }.toList.flatten
    Future.traverse(sends)(_.lift)
  }

  private def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long, permanent: Boolean): Future[Unit] =
    Future.successful(deleteMessagesTo(persistenceId, toSequenceNr, permanent))

  private def deleteMessagesTo(persistenceId: String, toSequenceNr: Long, permanent: Boolean): Unit =
    deletions = deletions + (persistenceId -> (toSequenceNr, permanent))

  private def writerFor(persistenceId: String): ActorRef =
    writers(math.abs(persistenceId.hashCode) % config.writeConcurrency)

  private def writer(): ActorRef = {
    context.actorOf(Props(new KafkaJournalWriter(writerConfig)).withDispatcher(config.pluginDispatcher))
  }

  private def writerConfig = {
    KafkaJournalWriterConfig(journalProducerConfig, eventProducerConfig, config.eventTopicMapper, serialization)
  }

  override def asyncWriteMessages(messages: Seq[AtomicWrite]): Future[Seq[Try[Unit]]] = {
    // TODO : manage persistAll -> messages.payload.size > 1 -> unsupported exception
    val reprs = messages.flatMap(_.payload)
    asyncWriteMessagesLegacy(reprs)
  }

  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] =
    asyncDeleteMessagesTo(persistenceId, toSequenceNr, false)

  // --------------------------------------------------------------------------------------
  //  Journal reads
  // --------------------------------------------------------------------------------------

  def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] =
    Future(readHighestSequenceNr(persistenceId, fromSequenceNr))

  def readHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Long = {
    val topic = journalTopic(persistenceId)
    leaderFor(topic, brokers) match {
      case Some(Broker(host, port)) => offsetFor(host, port, topic, config.partition)
      case None                     => fromSequenceNr
    }
  }

  def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(replayCallback: PersistentRepr => Unit): Future[Unit] = {
    val deletions = this.deletions
    Future(replayMessages(persistenceId, fromSequenceNr, toSequenceNr, max, deletions, replayCallback))
  }

  def replayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long, deletions: Deletions, callback: PersistentRepr => Unit): Unit = {
    val (deletedTo, permanent) = deletions.getOrElse(persistenceId, (0L, false))

    val adjustedFromTmp = if (permanent) math.max(deletedTo + 1L, fromSequenceNr) else fromSequenceNr
    // TODO: WORKAROUND to pass tests with fromSequenceNr = 0
    val adjustedFrom = if (adjustedFromTmp == 0) 1 else adjustedFromTmp

    val adjustedNum = toSequenceNr - adjustedFrom + 1L
    val adjustedTo = if (max < adjustedNum) adjustedFrom + max - 1L else toSequenceNr

    val lastSequenceNr = leaderFor(journalTopic(persistenceId), brokers) match {
      case None => 0L // topic for persistenceId doesn't exist yet
      case Some(MetadataConsumer.Broker(host, port)) =>
        val iter = persistentIterator(host, port, journalTopic(persistenceId), adjustedFrom - 1L)
        iter.map(p => if (!permanent && p.sequenceNr <= deletedTo) p.update(deleted = true) else p).foldLeft(adjustedFrom) {
          case (snr, p) => if (p.sequenceNr >= adjustedFrom && p.sequenceNr <= adjustedTo) callback(p); p.sequenceNr
        }
    }
  }

  def persistentIterator(host: String, port: Int, topic: String, offset: Long): Iterator[PersistentRepr] = {
    new MessageIterator(host, port, topic, config.partition, offset, config.consumerConfig).map { m =>
      serialization.deserialize(MessageUtil.payloadBytes(m), classOf[PersistentRepr]).get
    }
  }

}

private case class KafkaJournalWriterConfig(
  journalProducerConfig: ProducerConfig,
  eventProducerConfig: ProducerConfig,
  evtTopicMapper: EventTopicMapper,
  serialization: Serialization)

private case class UpdateKafkaJournalWriterConfig(config: KafkaJournalWriterConfig)

private class KafkaJournalWriter(var config: KafkaJournalWriterConfig) extends Actor {
  var msgProducer = createMessageProducer()
  var evtProducer = createEventProducer()

  def receive = {
    case UpdateKafkaJournalWriterConfig(newConfig) =>
      msgProducer.close()
      evtProducer.close()
      config = newConfig
      msgProducer = createMessageProducer()
      evtProducer = createEventProducer()

    case messages: Seq[PersistentRepr] =>
      writeMessages(messages)
      sender ! ()
  }

  def writeMessages(messages: Seq[PersistentRepr]): Unit = {
    val keyedMsgs = for {
      m <- messages
    } yield new KeyedMessage[String, Array[Byte]](journalTopic(m.persistenceId), "static", config.serialization.serialize(m).get)

    val keyedEvents = for {
      m <- messages
      e = Event(m.persistenceId, m.sequenceNr, m.payload)
      t <- config.evtTopicMapper.topicsFor(e)
    } yield new KeyedMessage(t, e.persistenceId, config.serialization.serialize(e).get)

    msgProducer.send(keyedMsgs: _*)
    evtProducer.send(keyedEvents: _*)
  }

  override def postStop(): Unit = {
    msgProducer.close()
    evtProducer.close()
    super.postStop()
  }

  private def createMessageProducer() = new Producer[String, Array[Byte]](config.journalProducerConfig)

  private def createEventProducer() = new Producer[String, Array[Byte]](config.eventProducerConfig)
}
