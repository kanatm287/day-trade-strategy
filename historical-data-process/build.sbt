name := "historical-data-process"

crossPaths := false

mainClass in (Compile, run) := Some("main.scala.fiveMinute.Core")

fork in run := true

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.3",
  "org.apache.spark" %% "spark-sql" % "2.4.3",
  "org.apache.spark" %% "spark-mllib" % "2.4.3",
  "org.postgresql" % "postgresql" % "42.2.6",
  "net.liftweb" %% "lift-json" % "3.3.0"
)

javaOptions in run ++= Seq("-Dibdb.host=localhost",
                           "-Dibdb.name=my_database",
                           "-Dibdb.port=5432", 
                           "-Dibdb.user=postgres", 
                           "-Dibdb.password=postgres",
                           "-Dib.app-root=/app-root")

javaOptions in ThisBuild ++= Seq ("-Xms512m","-Xmx4G")
