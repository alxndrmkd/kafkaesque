import sbt._
import sbt.Keys._
import sbt.librarymanagement.Resolver

object Common {
  val settings = Seq(
    version := "0.1",
    scalaVersion := "2.12.4",
    scalacOptions += "-Ypartial-unification",
    scalacOptions += "-Xlog-implicits",
    scalacOptions += "-language:postfixOps",
    scalacOptions += "-language:higherKinds",
    scalacOptions += "-language:implicitConversions",
    resolvers += Resolver.sonatypeRepo("releases"),
    addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.9.4")
  )

  val dependencies = Seq(
    Dependencies.cats,
    Dependencies.effect
  )
}
