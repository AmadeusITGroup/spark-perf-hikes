// Spark: 3.5.1
// Local: --driver-memory 1G --master 'local[2]' --packages io.delta:delta-spark_2.12:3.1.0 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
// Databricks: ...

// COMMAND ----------

/*
This example shows how a merge of a few records onto a large delta table (matching few records) can lead to large write
amplification, and how to improve the situation by using Merge-on-Read strategy with Deletion Vectors.

References:
- https://docs.delta.io/3.1.0/delta-deletion-vectors.html
- https://docs.delta.io/3.1.0/delta-utility.html

# Symptom
The volume of data being rewritten in a Delta Table on a MERGE is way above the volume of the records expected to be updated / added.

# Explanation
Let's take first the case without Deletion Vectors. Upon MERGE, the Delta Table will rewrite the files that contain the
records to be updated / added. This is clearly suboptimal when few records are to be updated. For example, in an extreme case, if
there is 1 record to be updated in a large file, the whole file has to be replaced.
Instead, Delete Vectors can be used (Merge-on-Read strategy). This strategy reuses large files with few records to be deleted, and simply
marks the records to be ignored from the such files, in deletion vector files. New records are written in new small files.
*/

// COMMAND ----------

import java.util.UUID
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit}

val spark: SparkSession = SparkSession.active

spark.conf.set("spark.sql.adaptive.enabled", false)
val input = "/tmp/perf-hikes/datasets/optd_por_public_filtered.csv"
val tmpPath = "/tmp/perf-hikes/sandbox/" + UUID.randomUUID()

spark.sparkContext.setJobDescription("Read CSV")
val airports = spark.read.option("delimiter","^").option("header","true").csv(input)

// COMMAND ----------

spark.sparkContext.setJobDescription("Initialize Delta tables")
val deltaWithoutDvDir = tmpPath + "/delta-without-dv"
airports.write.format("delta").save(deltaWithoutDvDir)
spark.sql(s"ALTER TABLE delta.`$deltaWithoutDvDir` SET TBLPROPERTIES ( delta.enableDeletionVectors = false)") // DV disabled
val deltaWithDvDir = tmpPath + "/delta-with-dv"
airports.write.format("delta").save(deltaWithDvDir)
spark.sql(s"ALTER TABLE delta.`$deltaWithDvDir`    SET TBLPROPERTIES ( delta.enableDeletionVectors = true)") // DV enabled

// COMMAND ----------

def buildDataframeToMerge(idPrefix: String, newComment: String): DataFrame = {
  val df = airports.where(col("id") like idPrefix + "%").drop("comment").withColumn("comment", lit(newComment))
  println(s"${df.count()} records will be merged (matching id prefix '$idPrefix'*) with new comment '$newComment'")
  df
}

def mergeOntoDeltaTable(target: DeltaTable, df: DataFrame) = {
  target.as("t")
    .merge(df.as("df"), "df.id == t.id")
    .whenMatched.updateAll.whenNotMatched.insertAll.execute()
}

def showMergeStats(target: DeltaTable): Unit = {
  target.history().select("version", "operation",
    "operationMetrics.numTargetFilesAdded", "operationMetrics.numTargetFilesRemoved",
    "operationMetrics.numTargetBytesAdded", "operationMetrics.numTargetBytesRemoved",
    "operationMetrics.numSourceRows"/* number of rows in the source dataframe */, 
    "operationMetrics.numOutputRows" /* total number of rows written */
    //"operationMetrics.numTargetRowsInserted", // output rows in detail
    //"operationMetrics.numTargetRowsUpdated", 
    //"operationMetrics.numTargetRowsDeleted", 
    //"operationMetrics.numTargetRowsCopied", 
  ).where(col("OPERATION") === "MERGE").show(false)
}

// COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", 3)

// COMMAND ----------

// WITHOUT DV
spark.sparkContext.setJobDescription("MERGE WITHOUT DV")
mergeOntoDeltaTable(DeltaTable.forPath(deltaWithoutDvDir), buildDataframeToMerge("SF", "newcomment"))
// large write amplification (MERGE deletes all old files and writes new large files, as many as shuffle.partitions,
// few source rows, many output rows written)
showMergeStats(DeltaTable.forPath(deltaWithoutDvDir))

// WITH DV
spark.sparkContext.setJobDescription("MERGE WITH DV")
mergeOntoDeltaTable(DeltaTable.forPath(deltaWithDvDir), buildDataframeToMerge("SF", "newcomment"))
// small write amplification (MERGE keeps old files marking records to be ignored, and writes new small file with the new records
// few source rows, and few output rows written)
showMergeStats(DeltaTable.forPath(deltaWithDvDir))

// COMMAND ----------
