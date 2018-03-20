lazy val root =
  project.in(file("."))
    .settings(name := "kafkaesque")
    .settings(Common.settings)
    .aggregate(core)

lazy val core =
  module("core")
    .settings(libraryDependencies += Dependencies.fs2)
    .settings(libraryDependencies += Dependencies.kafka)

def module(name: String) =
  Project(name, file(s"modules/$name"))
    .settings(Common.settings)
    .settings(libraryDependencies ++= Common.dependencies)

