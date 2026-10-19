// Databricks notebook source
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
The volume of data being rewritten in a Delta Table on a MERGE is 
way above the volume of the records expected to be updated / added.
Another symptom is an unexpected high network egress cost.

# Explanation
Let's take first the case without Deletion Vectors. Upon MERGE, the Delta Table will 
rewrite the files that contain the records to be updated / added. This is clearly suboptimal when few 
records are to be updated. For example, in an extreme case, if there is 1 record to be updated in a 
large parquet file of the delta table, the whole file has to be rewritten.
Instead, Delete Vectors can be used (Merge-on-Read strategy). This strategy reuses large files by doing a soft-delete
of records no longer valid, and creates new small files with the new records.
The write amplification can be assessed by understanding the ratio between the volume of data that is intended
to be written, versus the volume of data physically written. Both can be read from the `history()` operationMetrics 
of the delta tables, either as data volume (in bytes), number of rows or number of files.
See the method `showOperationMetrics`. Also it is possible to improve observability by exploring delta logs, see
method `getFileStats`.

# What to aim for concretely
In Delta merges, the ideal goal is to physically write only as much data as functionally intended to.
Concretely, the goal is to have operationMetrics in the history() such that the two
- numSourceRows (number of batch rows that are intended to be used for the merge)
- numOuputRows (number of rows physically written in the table on the merge)
should have similar values.
Mind that while this goal generally improves the producer performance, it leaves to the consumer the resolution of
the soft-deleted records with some performance cost. 
*/

// COMMAND ----------

// MAGIC %md
// MAGIC # Set up

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
val airports = spark.read.option("delimiter","^").option("header","true").csv(input).repartition(5)

// COMMAND ----------

spark.sparkContext.setJobDescription("Initialize Delta tables")
// Table without Deletion Vectors (DV)
val deltaWithoutDvDir = tmpPath + "/delta-without-dv"
airports.write.format("delta").save(deltaWithoutDvDir)
spark.sql(s"ALTER TABLE delta.`$deltaWithoutDvDir` SET TBLPROPERTIES ( delta.enableDeletionVectors = false)") // DV disabled
// Table with Deletion Vectors (DV)
val deltaWithDvDir = tmpPath + "/delta-with-dv"
airports.write.format("delta").save(deltaWithDvDir)
spark.sql(s"ALTER TABLE delta.`$deltaWithDvDir`    SET TBLPROPERTIES ( delta.enableDeletionVectors = true)") // DV enabled

// COMMAND ----------

// MAGIC %md
// MAGIC # Utility functions

// COMMAND ----------

def buildDataframeToMerge(idPrefix: String, newComment: String): DataFrame = {
  val df = airports.where(col("id") like idPrefix + "%").drop("comment").withColumn("comment", lit(newComment))
  println(s"${df.count()} records will be merged (matching id prefix '$idPrefix'*) with new comment '$newComment'")
  df
}

def mergeOntoDeltaTable(target: DeltaTable, df: DataFrame) = {
  target.as("t")
    .merge(df.as("df"), "df.id == t.id")
    .whenMatched().updateAll().whenNotMatched().insertAll().execute()
}


// COMMAND ----------

def operationMetrics(target: DeltaTable): DataFrame = {
  target.history().select("version", "operation",
    "operationMetrics.numSourceRows"/* number of rows in the source dataframe, intended to be merged */, 
    "operationMetrics.numOutputRows", /* total number of rows physically written */
    "operationMetrics.numTargetBytesAdded", "operationMetrics.numTargetBytesRemoved",
    "operationMetrics.numTargetFilesAdded", "operationMetrics.numTargetFilesRemoved",
    "operationMetrics", "*"
  ).where(col("OPERATION") === "MERGE")
}


// COMMAND ----------

// uncomment if display is unavailable
//def display(df: DataFrame) = df.show(false)

// COMMAND ----------

def getFileStats(tablePath: String, fromCommit: Long, toCommit: Long): DataFrame = {
  // parquet files added/removed by each commit in [fromCommit, toCommit], with their size
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.types._
  require(fromCommit <= toCommit, s"fromCommit ($fromCommit) must be <= toCommit ($toCommit)")
  // explicit schema: actions absent from a commit resolve to null instead of failing
  val fileAction = new StructType().add("path", StringType).add("size", LongType)
  val schema = new StructType().add("add", fileAction).add("remove", fileAction)
  (fromCommit to toCommit)
    .map { commit =>
      spark.read.schema(schema).json(f"$tablePath/_delta_log/$commit%020d.json")
        .where(col("add").isNotNull || col("remove").isNotNull)
        .select(
          lit(commit).as("commit"),
          when(col("add").isNotNull, "add").otherwise("remove").as("operation"),
          coalesce(col("add.path"), col("remove.path")).as("path"),
          coalesce(col("add.size"), col("remove.size")).as("size"))
    }
    .reduce(_ unionByName _)
    .orderBy(col("commit"), desc("operation"), col("size"))
}

// COMMAND ----------

// MAGIC %md
// MAGIC # Merges

// COMMAND ----------

// Perform MERGES

// WITHOUT DV
spark.sparkContext.setJobDescription("MERGE WITHOUT DV")
mergeOntoDeltaTable(DeltaTable.forPath(deltaWithoutDvDir), buildDataframeToMerge("SF", "newcomment"))


// WITH DV
spark.sparkContext.setJobDescription("MERGE WITH DV")
mergeOntoDeltaTable(DeltaTable.forPath(deltaWithDvDir), buildDataframeToMerge("SF", "newcomment"))

// COMMAND ----------

// MAGIC %md
// MAGIC # Explore metrics

// COMMAND ----------

// MAGIC %md
// MAGIC ## General metrics

// COMMAND ----------

display(DeltaTable.forPath(deltaWithoutDvDir).toDF)

// COMMAND ----------

display(DeltaTable.forPath(deltaWithoutDvDir).history)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Without DV

// COMMAND ----------

// WITHOUT DV
// large write amplification (MERGE deletes all old files and writes new large files, few source rows, many output rows written)
display(operationMetrics(DeltaTable.forPath(deltaWithoutDvDir)))

// COMMAND ----------

display(getFileStats(deltaWithoutDvDir, 0, 2))

// COMMAND ----------

// MAGIC %md
// MAGIC ## With DV

// COMMAND ----------

// WITH DV
// small write amplification (MERGE keeps old files marking records to be ignored, and writes new small file with the new records few source rows, and few output rows written)
display(operationMetrics(DeltaTable.forPath(deltaWithDvDir)))

// COMMAND ----------

display(getFileStats(deltaWithDvDir, 0, 2))
