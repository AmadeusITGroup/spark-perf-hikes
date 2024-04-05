// Spark: 3.5.1
// Local: --executor-memory 1G --driver-memory 1G --executor-cores 1 --master local[2] --packages io.delta:delta-spark_2.12:3.1.0 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
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

spark.sparkContext.setJobDescription("Read CSV")
val inputCsv = spark.read.option("delimiter","^").option("header","true").csv(input).where(col("geoname_id").isNotNull).where(col("icao_code").isNotNull)

spark.sparkContext.setJobDescription("Initialize Delta tables")
val deltaWithoutDvDir = tmpPath + "/delta-without-dv"
inputCsv.write.format("delta").save(deltaWithoutDvDir)
spark.sql(s"ALTER TABLE delta.`$deltaWithoutDvDir` SET TBLPROPERTIES ( delta.enableDeletionVectors = false)") // DV disabled
val deltaWithDvDir = tmpPath + "/delta-with-dv"
inputCsv.write.format("delta").save(deltaWithDvDir)
spark.sql(s"ALTER TABLE delta.`$deltaWithDvDir`    SET TBLPROPERTIES ( delta.enableDeletionVectors = true)") // DV enabled

def buildDataframeToMerge(countryCode: String, newCode: String): DataFrame = {
  val df = inputCsv.where(col("country_code") === countryCode).drop("iata_code").withColumn("iata_code", lit(newCode))
  println(s"A total of ${df.count()} records found that will be merged (matching country_code=$countryCode)")
  df
}

def mergeOntoDeltaTable(target: DeltaTable, df: DataFrame) = {
  target.as("t")
    .merge(df.as("df"), "df.geoname_id == t.geoname_id")
    .whenMatched.updateAll.whenNotMatched.insertAll.execute()
}

def showDeltaTableHistory(target: DeltaTable): Unit = {
  target.history().select("version", "operation",
    "operationMetrics.numTargetFilesAdded", "operationMetrics.numTargetChangeFilesAdded", "operationMetrics.numTargetFilesRemoved",
    "operationMetrics.numTargetBytesAdded", "operationMetrics.numTargetBytesRemoved"
  ).show(false)
}

spark.sparkContext.setJobDescription("Merge without DV")
mergeOntoDeltaTable(DeltaTable.forPath(deltaWithoutDvDir), buildDataframeToMerge("AR", "newcode1"))

spark.sparkContext.setJobDescription("Merge with DV")
mergeOntoDeltaTable(DeltaTable.forPath(deltaWithDvDir), buildDataframeToMerge("AR", "newcode1"))

spark.sparkContext.setJobDescription("Show stats")
println("WITHOUT DV")
showDeltaTableHistory(DeltaTable.forPath(deltaWithoutDvDir))
println("WITH DV")
showDeltaTableHistory(DeltaTable.forPath(deltaWithDvDir))

