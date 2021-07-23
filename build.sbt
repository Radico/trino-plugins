//version := "1.0"

val projectName = "trino-plugins"
val trinoVersion = "354"

// Should the com.simondata.trino.Run object be exported in the jar?
val addEntryPoint = true

name := projectName

// Synchronized with the version of Trino we are supporting
version := trinoVersion

scalaVersion := "2.13.1"

// https://mvnrepository.com/artifact/io.trino/trino-spi
if (addEntryPoint) {
  libraryDependencies += "io.trino" % "trino-spi" % trinoVersion
} else {
  libraryDependencies += "io.trino" % "trino-spi" % trinoVersion % "provided"
}

// https://mvnrepository.com/artifact/com.typesafe.play/play-json
libraryDependencies += "com.typesafe.play" %% "play-json" % "2.8.1"

// https://mvnrepository.com/artifact/commons-codec/commons-codec
libraryDependencies += "commons-codec" % "commons-codec" % "1.15"

// https://mvnrepository.com/artifact/org.scalatest/scalatest
libraryDependencies += "org.scalatest" %% "scalatest" % "3.1.1" % Test

// Helpful when testing (recommended by scalatest)
logBuffered in Test := false

// The single Java source acts as the entry point for our plugin
compileOrder := CompileOrder.ScalaThenJava

// Target Java SE 11
scalacOptions += "-target:jvm-11"
javacOptions ++= Seq("-source", "11", "-target", "11", "-Xlint")

val dateTime = {
  import java.util.{Date, TimeZone}
  import java.text.SimpleDateFormat

  val dateFormat = new SimpleDateFormat("yyyyMMdd_hhmmss")
  dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"))
  dateFormat.format(new Date())
}

val gitInfo = {
  import scala.sys.process._

  val gitHash: String = ("git rev-parse --verify HEAD" !!) trim
  val gitDirty: Boolean = "git diff --quiet" ! match {
    case 0 => false
    case _ => true
  }

  (gitHash, gitDirty)
}

def buildArtifactName(extension: String = ".jar") = {
  val (gitHash, gitDirty) = gitInfo
  val dirtyStr = if (gitDirty) "-dirty" else ""
  val name = s"${projectName}-${trinoVersion}-${gitHash}${dirtyStr}${extension}"
  println(s"TRINO_PLUGINS_ARTIFACT: ${name}")

  name
}

assemblyMergeStrategy in assembly := {
  case PathList("io", "trino", "spi", "license", "LicenseManager.class") => MergeStrategy.discard
  case PathList("META-INF", "services", "io.trino.spi.Plugin") => MergeStrategy.first
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

// Do not include the LicenseManager (required during compile only, supplied by Starburst)
mappings in (Compile, packageBin) ~= { (ms: Seq[(File, String)]) =>
  ms filter { case (_, toPath) =>
    (addEntryPoint, toPath) match {
      case (false, "com/simondata/trino/Run.class") => false
      case _ => true
    }
  }
}

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  buildArtifactName(s".${artifact.extension}")
}

assemblyJarName in assembly := {
  buildArtifactName()
}
