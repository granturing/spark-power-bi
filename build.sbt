name := "spark-power-bi"

organization := "com.granturing"

publishMavenStyle := true

version := "1.4.0_0.0.7"

scalaVersion := "2.10.5"

sparkVersion := "1.4.0"

sparkComponents ++= Seq("core", "streaming", "sql")

spName := "granturing/spark-power-bi"

val dispatchVersion = "0.11.2"

libraryDependencies ++= Seq(
  ("com.microsoft.azure" % "adal4j" % "1.0.0").
    exclude("commons-codec", "commons-codec")
    exclude("org.slf4j", "slf4j-api"),
  ("net.databinder.dispatch" %% "dispatch-core" % dispatchVersion).
    exclude("io.netty", "netty")
    exclude("org.slf4j", "slf4j-api"),
  "org.scalatest" %% "scalatest" % "2.2.4" % "test"
)

pomIncludeRepository := { x => false }

// publish settings
publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

licenses := Seq("Apache License 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html"))

homepage := Some(url("https://github.com/granturing/spark-power-bi"))

pomExtra := (
    <scm>
      <url>git@github.com:granturing/spark-power-bi.git</url>
      <connection>scm:git:git@github.com:granturing/spark-power-bi.git</connection>
    </scm>
    <developers>
      <developer>
        <id>granturing</id>
        <name>Silvio Fiorito</name>
        <url>https://github.com/granturing</url>
      </developer>
    </developers>)

credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")