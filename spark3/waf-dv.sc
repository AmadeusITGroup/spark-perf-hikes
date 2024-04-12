// Spark: 3.5.1
// Local: --executor-memory 1G --driver-memory 1G --executor-cores 1 --master local[2] --packages io.delta:delta-spark_2.12:3.1.0 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
// Databricks: ...

// COMMAND ----------

/*
This example shows how a merge of a few records onto a large delta table (matching few records) can lead to large write
amplification, and how to improve the situation by using Merge-on-Read strategy with Deletion Vectors.

# Symptom
The volume of data being rewritten in a Delta Table on a MERGE is way above the volume of the records expected to be updated / added.

# Explanation
Let's take first the case without Deletion Vectors. Upon MERGE, the Delta Table will rewrite the files that contain the
records to be updated / added. This is clearly suboptimal when few records are to be updated. For example, in an extreme case, if
there is 1 record to be updated in a file with a million records, the whole file has to be deleted and written in a newer version.
Instead, Delete Vectors can be used (Merge-on-Read strategy). This strategy reuses large files with few records to be deleted, and simply
marks the records to be ignored from the such files, in deletion vector files. New records are written in new small files.
See this https://docs.delta.io/latest/delta-deletion-vectors.html for more information.
*/

// COMMAND ----------

import java.util.UUID
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit}

val spark: SparkSession = SparkSession.active

spark.conf.set("spark.sql.adaptive.enabled", false)
val input = "/tmp/amadeus-spark-lab/datasets/optd_por_public.csv"
val tmpPath = "/tmp/amadeus-spark-lab/sandbox/" + UUID.randomUUID()

spark.sparkContext.setJobDescription("Read CSV")
val rawCsv = spark.read.option("delimiter","^").option("header","true").csv(input)
val projected = rawCsv.select("iata_code", "envelope_id", "name", "latitude", "longitude", "date_from", "date_until", "comment", "country_code", "country_name", "continent_name", "timezone", "wiki_link")
projected.where(col("location_type")==="A" and col("iata_code").isNotNull).createOrReplaceTempView("table")
val airports = spark.sql("SELECT row_number() OVER (PARTITION BY iata_code ORDER BY envelope_id, date_from DESC) as r, * FROM table").where(col("r") === 1).drop("r")

// COMMAND ----------

spark.sparkContext.setJobDescription("Initialize Delta tables")
val deltaWithoutDvDir = tmpPath + "/delta-without-dv"
airports.write.format("delta").save(deltaWithoutDvDir)
spark.sql(s"ALTER TABLE delta.`$deltaWithoutDvDir` SET TBLPROPERTIES ( delta.enableDeletionVectors = false)") // DV disabled
val deltaWithDvDir = tmpPath + "/delta-with-dv"
airports.write.format("delta").save(deltaWithDvDir)
spark.sql(s"ALTER TABLE delta.`$deltaWithDvDir`    SET TBLPROPERTIES ( delta.enableDeletionVectors = true)") // DV enabled

// COMMAND ----------

def buildDataframeToMerge(countryCode: String, newCode: String): DataFrame = {
  val df = airports.where(col("country_code") === countryCode).drop("iata_code").withColumn("iata_code", lit(newCode))
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
    "operationMetrics.numTargetFilesAdded", "operationMetrics.numTargetFilesRemoved",
    "operationMetrics.numTargetBytesAdded", "operationMetrics.numTargetBytesRemoved"
  ).show(false)
}

// COMMAND ----------

spark.sparkContext.setJobDescription("Merge without DV")
mergeOntoDeltaTable(DeltaTable.forPath(deltaWithoutDvDir), buildDataframeToMerge("AR", "newcode1"))

spark.sparkContext.setJobDescription("Merge with DV")
mergeOntoDeltaTable(DeltaTable.forPath(deltaWithDvDir), buildDataframeToMerge("AR", "newcode1"))

//buildDataframeToMerge("AR", "newcode1").createOrReplaceTempView("df2")
//spark.sql(s"MERGE INTO delta.`${deltaWithDvDir}` as src USING df2 ON src.geoname_id = dfw.geoname_id WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")

// COMMAND ----------

spark.sparkContext.setJobDescription("Show stats")
println("WITHOUT DV")
// large write amplification (MERGE deletes all old files and writes new large files)
showDeltaTableHistory(DeltaTable.forPath(deltaWithoutDvDir))
println("WITH DV")
// small write amplification (MERGE keeps old files marking records to be ignored, and writes new small files with new records)
showDeltaTableHistory(DeltaTable.forPath(deltaWithDvDir))


