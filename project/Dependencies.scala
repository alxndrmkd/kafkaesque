import sbt._

object Dependencies {

  lazy val cats = {
    "org.typelevel" %% "cats-core" % Version.cats
  }

  lazy val effect = {
    "org.typelevel" %% "cats-effect" % Version.effect
  }

  lazy val fs2 = {
    "co.fs2" %% "fs2-core" % Version.fs2
  }

  lazy val kafka = {
    "org.apache.kafka" % "kafka-clients" % Version.kafka
  }
}
