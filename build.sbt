organization := "com.github.krasserm"

name := "akka-persistence-kafka"

version := "0.5-SNAPSHOT"

scalaVersion := "2.11.6"

crossScalaVersions := Seq("2.10.4", "2.11.6")

scalacOptions += "-target:jvm-1.8"

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

//resolvers += "krasserm at bintray" at "http://dl.bintray.com/krasserm/maven"

parallelExecution in Test := false

publishArtifact in Test := true

libraryDependencies ++= Seq(
  "com.google.protobuf"  % "protobuf-java"                 % "2.5.0",
  "com.typesafe.akka"   %% "akka-persistence" % "2.5.2",
//"com.github.krasserm" %% "akka-persistence-testkit"      % "0.3.4"    % Test,
  "com.typesafe.akka" %% "akka-persistence-tck" % "2.5.2" % Test,
  "commons-io"           % "commons-io"                    % "2.4"      % Test,
  //"org.apache.kafka"    %% "kafka"                         % "0.8.2.1",
  "org.apache.kafka"    %% "kafka"                         % "0.10.2.1",
  "org.apache.curator"   % "curator-test"                  % "2.7.1"    % Test
)
