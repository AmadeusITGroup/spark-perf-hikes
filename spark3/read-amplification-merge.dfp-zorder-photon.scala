// Databricks notebook source
// Spark: 3.5.1
// Local: --driver-memory 1G --master 'local[2]' --packages io.delta:delta-spark_2.12:3.1.0 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
// Databricks: 13.3LTS+photon

// COMMAND ----------

/*
This example shows how to address the read amplification problem in case of merges,
using Dynamic File Pruning (DFP) and Photon, coupled with a data skipping friendly data layout such as z-order.

References:
- https://docs.databricks.com/en/optimizations/dynamic-file-pruning.html
- https://www.databricks.com/blog/2020/04/30/faster-sql-queries-on-delta-lake-with-dynamic-file-pruning.html
- https://docs.databricks.com/en/delta/data-skipping.html#delta-zorder
- https://www.databricks.com/blog/2020/09/29/diving-into-delta-lake-dml-internals-update-delete-merge.html

IMPORTANT: DFP and Photon are only available when running on Databricks.

# Symptom
You are merging a small dataframe into a big delta table and you are reading way more data that is actually needed
to perform the merge.
You know that you could read less data, because you know that only few records in the input dataframe will match
with records in the big table.

# Explanation

The delta table is z-ordered on the merge key, in order to co-locate closer keys in the same set of files.
We assume that we are running on Databricks using Photon.

First scenario: No DFP. We see that the whole delta table is read for the merge, both when the dataframe comes from a delta table,
and when it does not.

Second scenario: DFP. We see that only the delta table files containing the merge keys present in the small dataframe are read
in the first (inner) join corresponding to the merge. This happens only when the input dataframe comes from a delta table. 

In order to make sure that DFP can kick-in:
  - the small dataframe must be broadcastable
  - the small dataframe must come from a delta table
  - spark.databricks.optimizer.deltaTableSizeThreshold should be small enough
  - spark.databricks.optimizer.deltaTableFilesThreshold should be small enough
  - spark.databricks.optimizer.dynamicFilePruning should be true

Important note: if the keys in the input dataframe are such that they hit every files of the table,
even having z-oder and activating DFP, there will still be read amplification.

# What to aim for concretely

In the Spark UI, tab "SQL / DataFrame", for a given merge there will be many SQL queries with the same 
Description. Some of them will have multiple Sub Execution IDs. One of those Sub Executions will contain the 
first of the two merge joins (will be an 'inner' join, which can be seen through the tooltip text of the 
'PhotonBroadcastHashJoin' operator). Such first join should have on 'PhotonScan' operator a metric 
'files read' that should be small compared with 'files pruned'. Also in 'Details' there should be 
a mention of 'dynamicpruning'.

*/

// COMMAND ----------

import java.util.UUID
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.DataFrame
import io.delta.tables.DeltaTable

val input = "/tmp/perf-hikes/datasets/optd_por_public_filtered.csv"
val tmpPath = "/tmp/perf-hikes/sandbox/" + UUID.randomUUID()
val deltaDir = tmpPath + "/input"

val spark: SparkSession = SparkSession.active

// COMMAND ----------

// Keep AQE on, to avoid having to many files after merge
spark.conf.set("spark.sql.adaptive.enabled", true)

// Destination delta table
spark.sparkContext.setJobDescription("Create delta table")
val airports = spark.read.option("delimiter","^").option("header","true").csv(input)
airports.write.mode("overwrite").format("delta").save(deltaDir)

// z-order the delta table on the merge key
spark.sparkContext.setJobDescription("Z-order delta table")
spark.conf.set("spark.databricks.delta.optimize.maxFileSize", 50 * 1024L)
DeltaTable.forPath(deltaDir).optimize().executeZOrderBy("iata_code")

// COMMAND ----------

def getMaxMinStats(tablePath: String, colName: String, commit: Int): DataFrame = {
  // stats on parquet files added to the table
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.types._
  val statsSchema = new StructType()
    .add("numRecords", IntegerType, true)
    .add("minValues", MapType(StringType, StringType), true)
    .add("maxValues", MapType(StringType, StringType), true)
  val df = spark.read.json(s"$tablePath/_delta_log/*${commit}.json")
    .withColumn("commit_json_name", input_file_name())
    .withColumn("add_stats", from_json(col("add.stats"), statsSchema))
    .withColumn(s"add_stats_min_col_${colName}", col(s"add_stats.minValues.$colName"))
    .withColumn(s"add_stats_max_col_${colName}", col(s"add_stats.maxValues.$colName"))
    .withColumn("add_size", col("add.size"))
    .withColumn("add_path", col("add.path"))
    .where(col("add_path").isNotNull)
    .select("add_path", s"add_stats_min_col_${colName}", s"add_stats_max_col_${colName}")
    .orderBy(s"add_stats_min_col_${colName}", "add_path")
  df
}

// Note that only 1 parquet file contains the iata_code 'AAI' (out of many parquet files in the delta table)
spark.sparkContext.setJobDescription(s"Display max/min stats for files present in delta table after z-order")
getMaxMinStats(deltaDir, "iata_code", 1).show(false)

// COMMAND ----------

def buildDataframeToMerge(iataCode: String, newVal: String): DataFrame = {
  val c = airports.where(col("iata_code") === iataCode).drop("comment").withColumn("comment", lit(newVal)).collect()
  val df = spark.createDataFrame(spark.sparkContext.parallelize(c), airports.schema)
  df
}

def buildDeltaTableToMerge(iataCode: String, newVal: String): DataFrame = {
  val df = buildDataframeToMerge(iataCode, newVal)
  val path = s"$tmpPath/batch_${iataCode}_${newVal}_${UUID.randomUUID()}"
  df.write.mode("overwrite").format("delta").save(path)
  DeltaTable.forPath(path).toDF
}

def merge(df: DataFrame): Unit = {
  val keyCols = Seq("iata_code")
  DeltaTable
    .forPath(deltaDir).as("o")
    .merge(df.as("t"), keyCols.map(colName => s"t.$colName == o.$colName").mkString(" and "))
    .whenMatched.updateAll.whenNotMatched.insertAll.execute()
}

// First scenario

spark.conf.set("spark.databricks.optimizer.dynamicFilePruning", "false")

// No DFP (input not from delta)
spark.sparkContext.setJobDescription("Merge 1 - NO DFP - input not delta")
DeltaTable.forPath(deltaDir).detail.selectExpr("numFiles as total_number_of_files_before_merge1").show
merge(buildDataframeToMerge("AAI", "no-dfp-no-delta"))

// No DFP (input from delta)
spark.sparkContext.setJobDescription("Merge 2 - NO DFP - input delta")
DeltaTable.forPath(deltaDir).detail.selectExpr("numFiles as total_number_of_files_before_merge2").show
merge(buildDeltaTableToMerge("AAI", "no-dfp-delta"))

// Second scenario

// Be sure to have the conditions to trigger DFP: DFP enabled, broadcast join, table/files thresholds
spark.conf.set("spark.databricks.optimizer.dynamicFilePruning", "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 1000000000L)
spark.conf.set("spark.databricks.optimizer.deltaTableSizeThreshold", 1)
spark.conf.set("spark.databricks.optimizer.deltaTableFilesThreshold", 1)

// DFP (input not from delta)
spark.sparkContext.setJobDescription("Merge 3 - DFP - input not delta")
DeltaTable.forPath(deltaDir).detail.selectExpr("numFiles as total_number_of_files_before_merge3").show
merge(buildDataframeToMerge("AAI", "dfp-no-delta"))

// DFP (input from delta)
spark.sparkContext.setJobDescription("Merge 4 - DFP - input delta")
DeltaTable.forPath(deltaDir).detail.selectExpr("numFiles as total_number_of_files_before_merge4").show
merge(buildDeltaTableToMerge("AAI", "dfp-delta"))

// For each merge, go to the Databricks Spark UI, look for the SQL query corresponding to the Merge,
// open the sub-queries and analyse those corresponding to the two joins (an inner and a left outer join).
// Consider the 'PhotonScan parquet' node corresponding to the delta table.

// If using a DataFrame NOT COMING from a delta table, DFP will not kick-in.
// In both cases (DFP and NO DFP) the first join reads the whole table, while the second join
// only reads the files containing the merge key, but this is not due to DFP.
// Indeed, in the Details we don't see "dynamic" and in the plan we don't see any node creating a dynamic filter
// as input to the 'PhotonScan' parquet node.

// If using a DataFrame COMING from a delta table, DFP will kick-in for the first join, when enabled.
// Indeed, in the Details we see "dynamicpruning" and in the plan we see the nodes creating a dynamic filter
// as input to the 'PhotonScan' parquet node.

