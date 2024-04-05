val sparkVersion         = "3.4.1"
val deps = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-avro" % sparkVersion
)
lazy val root = (project in file("."))
  .settings(
    name := "ssce-spark-sandbox",
    scalaVersion := "2.12.13",
    libraryDependencies := deps
  )
