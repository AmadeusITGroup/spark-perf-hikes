// Spark: 3.4.2
// Local: --executor-memory 1G --driver-memory 1G --executor-cores 1 --master local[2] --packages io.delta:delta-core_2.12:2.4.0,org.apache.spark:spark-avro_2.12:3.3.2 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
// Databricks: ...

// COMMAND ----------

/*
TODO
*/

import java.util.UUID
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit}

val spark: SparkSession = SparkSession.active

val input = "/tmp/amadeus-spark-lab/datasets/optd_por_public_all.csv"
val tmpPath = "/tmp/amadeus-spark-lab/sandbox/" + UUID.randomUUID()
val deltaDir = tmpPath + "/output"

spark.sparkContext.setJobDescription("Read CSV")
val inputCsv = spark.read.option("delimiter","^").option("header","true").csv(input)
spark.sparkContext.setJobDescription("Initialize Delta table")
inputCsv.write.format("delta").save(deltaDir)

def buildDataframeToMerge(countryCode: String, newCode: String): DataFrame =
  inputCsv.where(col("country_code") === countryCode).drop("iata_code").withColumn("iata_code", lit(newCode))

def merge(df: DataFrame) = {
  DeltaTable
    .forPath(deltaDir).as("o")
    .merge(df.as("t"), "t.iata_code == o.iata_code and t.geoname_id == o.geoname_id")
    .whenMatched.updateAll.whenNotMatched.insertAll.execute()
}

merge(buildDataframeToMerge("Argentina", "newcode1"))