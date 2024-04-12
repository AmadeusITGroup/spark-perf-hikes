// Spark: 3.5.1
// Local: --executor-memory 1G --driver-memory 1G --executor-cores 1 --master local[1]
// Databricks: ...

// COMMAND ----------

/*
This scripts simply prepares the input dataset, filtering the airports with non null IATA code and location type A.
Only the most recent entry for each IATA code is kept.
*/

// COMMAND ----------

import java.util.UUID
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

val input = "/tmp/amadeus-spark-lab/datasets/optd_por_public.csv"
val tmpPath = "/tmp/amadeus-spark-lab/sandbox/" + UUID.randomUUID()
val tmpFiltered = tmpPath + "/filtered"
val filtered = "/tmp/amadeus-spark-lab/datasets/optd_por_public_filtered.csv"

val spark: SparkSession = SparkSession.active

// COMMAND ----------

spark.sparkContext.setJobDescription("Create input dataset")

val rawCsv = spark.read.option("delimiter","^").option("header","true").csv(input)
val projected = rawCsv.select("iata_code", "envelope_id", "name", "latitude", "longitude", "date_from", "date_until", "comment", "country_code", "country_name", "continent_name", "timezone", "wiki_link")
projected.where(col("location_type")==="A" and col("iata_code").isNotNull).createOrReplaceTempView("table")
val airports = spark.sql("SELECT row_number() OVER (PARTITION BY iata_code ORDER BY envelope_id, date_from DESC) as r, * FROM table").where(col("r") === 1).drop("r")

airports.coalesce(1).write.mode("overwrite").option("header", "true").csv(tmpFiltered)

println(s"cp $tmpFiltered/*.csv $filtered")
// e.g. cp /tmp/amadeus-spark-lab/sandbox/e1263c35-e920-4151-922e-4a1ad9d51f1c/filtered/*.csv /tmp/amadeus-spark-lab/datasets/optd_por_public_filtered.csv

import java.nio.file.{Files, Paths, StandardCopyOption}
import scala.collection.JavaConverters._

val sourceDir = Paths.get(tmpFiltered)
val targetDir = Paths.get("/tmp/amadeus-spark-lab/datasets")

Files.list(sourceDir).iterator().asScala.filter(_.toString.endsWith(".csv")).foreach { path =>
  println(s"Moving $path to $targetDir")
  val targetPath = targetDir.resolve(sourceDir.relativize(path))
  Files.move(path, targetPath, StandardCopyOption.REPLACE_EXISTING)
}