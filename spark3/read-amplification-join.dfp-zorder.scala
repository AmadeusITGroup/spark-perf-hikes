// Databricks notebook source
// Spark: 3.5.1
// Local: --driver-memory 1G --master 'local[2]' --packages io.delta:delta-spark_2.12:3.1.0 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
// Databricks: 13.3LTS or above

// COMMAND ----------

/*
This example shows how to address the read amplification problem in case of joins, 
using Dynamic File Pruning (DFP), coupled with a data skipping friendly data layout such as z-order.

References:
- https://docs.databricks.com/en/optimizations/dynamic-file-pruning.html
- https://www.databricks.com/blog/2020/04/30/faster-sql-queries-on-delta-lake-with-dynamic-file-pruning.html
- https://docs.databricks.com/en/delta/data-skipping.html#delta-zorder

IMPORTANT: DFP is a Databricks-only feature, not supported in OSS Spark/Delta.
When running locally, all scenarios read every file of the probe table.

# Symptom

You are joining a small table with a big one and you are reading way more data than is actually 
needed to perform the join operation.
You know that you could read less data, because you know that only few records in the big table 
match with the join keys present in the small table.

# Explanation

We are doing a join between the following two tables:
- a "build" side: smaller table
- a "probe" side: bigger table

The probe side is z-ordered on the join key, to make sure that close join keys sit in the same 
files. Note: on a single column, z-order is equivalent to a sort; any layout that clusters the join 
keys (sort, z-order, liquid clustering) works.

In the first scenario, DFP is not activated. We see that the whole probe table is read.

In the second scenario, DFP is activated. We see that, for the probe table, only the files containing 
the join keys present in the build table are read.

In the third scenario, DFP is activated but there is no filter on the build side: DFP does not kick in,
and the whole probe table is read again.

In order to make sure that DFP can kick-in:
  - the build side must be broadcastable: make sure the join is a broadcast join, if needed by raising
    spark.sql.autoBroadcastJoinThreshold (here the build table is tiny, so the default 10MB is enough)
  - there should be a filter on the build side
  - spark.databricks.optimizer.deltaTableSizeThreshold should be small enough: the probe table must be 
    at least this size in bytes (default 10GB), otherwise DFP is skipped because reading the whole table 
    is considered cheaper
  - spark.databricks.optimizer.deltaTableFilesThreshold should be small enough: the probe table must have
    at least this number of files (default 10), otherwise DFP is skipped
  - spark.databricks.optimizer.dynamicFilePruning should be true (default)
Our probe table is a few KB in a handful of files, so we lower both thresholds to 1 to let DFP kick in.

Important note: each join key on the build side can add files to read on the probe side.
If the join keys hit every file of the probe side, even with z-order and DFP, there will still be read 
amplification.

# What to aim for concretely

In the Spark UI, in a join between a large and a small table, in the 'Scan parquet' node of the big one, the 
metric 'number of files read' should be small compared with the 'number of files pruned'. 


*/

// COMMAND ----------

import java.util.UUID
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.DataFrame
import io.delta.tables.DeltaTable

val input = "/tmp/perf-hikes/datasets/optd_por_public_filtered.csv"
val tmpPath = "/tmp/perf-hikes/sandbox/" + UUID.randomUUID()
val probeDir = tmpPath + "/input"
val buildDir = tmpPath + "/employee"

val spark: SparkSession = SparkSession.active
import spark.implicits._

// COMMAND ----------

// Probe side
spark.sparkContext.setJobDescription("Create probe table")
val airports = spark.read.option("delimiter","^").option("header","true").csv(input)
airports.write.mode("overwrite").format("delta").save(probeDir)

// z-order the probe table on the join key
spark.sparkContext.setJobDescription("Z-order probe table")
spark.conf.set("spark.databricks.delta.optimize.maxFileSize", 50 * 1024L)
DeltaTable.forPath(probeDir).optimize().executeZOrderBy("country_code")

// COMMAND ----------

// Note the total number of files after z-ordering (it may vary with the runtime version)
DeltaTable.forPath(probeDir).detail().selectExpr("numFiles as total_number_of_files_after_zorder").show()

// COMMAND ----------

def getMaxMinStats(tablePath: String, colName: String, commit: Int): DataFrame = {
  // min/max stats of the parquet files added to the table by the given commit
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.types._
  val statsSchema = new StructType()
    .add("minValues", MapType(StringType, StringType), true)
    .add("maxValues", MapType(StringType, StringType), true)
  spark.read.json(f"$tablePath/_delta_log/$commit%020d.json")
    .where(col("add.path").isNotNull)
    .withColumn("add_stats", from_json(col("add.stats"), statsSchema))
    .select(
      col("add.path").as("add_path"),
      col(s"add_stats.minValues.$colName").as(s"min_$colName"),
      col(s"add_stats.maxValues.$colName").as(s"max_$colName"))
    .orderBy(s"min_$colName", "add_path")
}

// Note that very few files can contain the country_code 'FR' (min <= 'FR' <= max):
// usually 1, but it can be 2 if 'FR' sits on the border between 2 files.
// These are the files that DFP will read in the second scenario.
spark.sparkContext.setJobDescription("Display max/min stats for files present in delta table after z-order")
val stats = getMaxMinStats(probeDir, "country_code", 1)
stats.show(false)
stats.where(col("min_country_code") <= "FR" && col("max_country_code") >= "FR")
  .selectExpr("count(*) as files_containing_FR").show()

// COMMAND ----------

// Build side
case class Employee(name: String, role: String, residence: String)
val employeesData = Seq(
  Employee("Thierry", "Software Engineer", "FR"),
  Employee("Mohammed", "DevOps", "FR"),
  Employee("Gene", "Intern", "FR"),
  Employee("Mau", "Intern", "FR"),
  // Important note: each join key on the build side can add files to read on the probe side.
  // If the join keys hit every file of the probe side, even with z-order and DFP, there will still be
  // read amplification.
  // Uncomment the following line to see that 1 more file will be read just for one new key (ZA).
  // Employee("Lex", "Intern", "ZA"),
  Employee("Mathieu", "Software Engineer", "FR"),
  Employee("Thomas", "Intern", "FR")
)
spark.sparkContext.setJobDescription("Create build table")
employeesData.toDF.write.mode("overwrite").format("delta").save(buildDir)

// COMMAND ----------

// Join 2 tables (probe and build), adding a filter in the smaller one (the build side of the join)
def joinTables(description: String): Unit = {
  val probe = DeltaTable.forPath(probeDir).toDF
  val build = DeltaTable.forPath(buildDir).toDF
  spark.sparkContext.setJobDescription(s"Join tables: $description")
  probe
  .join(build, build("residence") === probe("country_code"), "inner")
  .where(col("role") === "Intern")
  .count()
}

// First scenario: DFP is not activated
spark.conf.set("spark.databricks.optimizer.dynamicFilePruning", "false")
joinTables("NO DFP")

// Be sure to have the conditions to trigger DFP: DFP enabled, broadcast join, table/files thresholds
spark.conf.set("spark.databricks.optimizer.dynamicFilePruning", "true")
// The build table is tiny, so the join is already a broadcast join with the default
// autoBroadcastJoinThreshold (10MB). In general, check in the plan that the join is a broadcast join,
// and if needed raise the threshold, e.g.:
// spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 100 * 1024 * 1024L)
// Lower the thresholds (defaults: 10GB and 10 files): our probe table is much smaller than that.
spark.conf.set("spark.databricks.optimizer.deltaTableSizeThreshold", 1)
spark.conf.set("spark.databricks.optimizer.deltaTableFilesThreshold", 1)

// Second scenario: DFP activated and preconditions are met
joinTables("DFP")

// Go to the Databricks Spark UI, look for the SQL query corresponding to the joins (i.e. 
// "Join tables: DFP" and "Join tables: NO DFP"), and compare the 'number of files read' 
// and 'number of files pruned' metrics in the big table 'Scan parquet' node of both queries.
// You can also get the total amount of files of a delta table with 'describe detail'.
// NOTE: The sum of 'number of files read' and 'number of files pruned' should give the total 
// amount of files in the table as per its last version.

// COMMAND ----------

// Join 2 tables (probe and build), without adding a filter in the smaller one (the build side of the join)
def joinTablesNoFilter(description: String): Unit = {
  val probe = DeltaTable.forPath(probeDir).toDF
  val build = DeltaTable.forPath(buildDir).toDF
  spark.sparkContext.setJobDescription(s"Join tables: $description")
  probe
  .join(build, build("residence") === probe("country_code"), "inner")
  // No filter here
  .count()
}

// Third scenario: DFP activated but no filter in the build side
joinTablesNoFilter("DFP-no-filter")

// Now do the same checks as above for the join without filter.
// This time DFP does not kick in, so we get the same results as in the first scenario, without DFP.