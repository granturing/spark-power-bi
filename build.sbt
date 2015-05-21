name := "spark-power-bi"

organization := "com.granturing"

version := "0.0.1"

scalaVersion := "2.10.4"

val sparkVersion = "1.2.0"

val dispatchVersion = "0.11.2"

libraryDependencies ++= Seq(
  ("com.microsoft.azure" % "adal4j" % "1.0.0").
    exclude("commons-codec", "commons-codec")
    exclude("org.slf4j", "slf4j-api"),
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  ("net.databinder.dispatch" %% "dispatch-core" % dispatchVersion).
    exclude("io.netty", "netty")
    exclude("org.slf4j", "slf4j-api"),
  "org.scalatest" %% "scalatest" % "2.2.4" % "test"
)

pomExtra := (
  <url>https://github.com/granturing/spark-power-bi</url>
    <licenses>
      <license>
        <name>Apache License, Verision 2.0</name>
        <url>http://www.apache.org/licenses/LICENSE-2.0.html</url>
        <distribution>repo</distribution>
      </license>
    </licenses>
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