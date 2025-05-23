// Databricks notebook source
// Spark: 3.5.1
// Local: --driver-memory 1G --master 'local[2]' --packages io.delta:delta-spark_2.12:3.1.0 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
// Databricks: 13.3LTS

// COMMAND ----------

/*
This example shows how to address the read amplification problem in case of joins, 
using Dynamic File Pruning (DFP), coupled with a data skipping friendly data layout such as z-order.

References:
- https://docs.databricks.com/en/optimizations/dynamic-file-pruning.html
- https://www.databricks.com/blog/2020/04/30/faster-sql-queries-on-delta-lake-with-dynamic-file-pruning.html
- https://docs.databricks.com/en/delta/data-skipping.html#delta-zorder

IMPORTANT: DFP is only available when running on Databricks.

# Symptom

You are joining a small table with a big one and you are reading way more data that is actually 
needed to perform the join operation.
You know that you could read less data, because you know that only few records in the big table 
match with the join keys present in the small table.

# Explanation

We are doing a join between the following two tables:
- a "build" side: smaller table
- a "probe" side: bigger table

The probe side is z-ordered on the join key, in order to co-locate closer keys in the same set 
of files.

In the first scenario, DFP is not activated. We see that the whole probe table is read.

In the second scenario, DFP is activated. We see that, for the probe table, only the files containing 
the join keys present in the build table are read.

In order to make sure that DFP can kick-in:
  - the build side must be broadcastable
  - there should be a filter on the build side
  - spark.databricks.optimizer.deltaTableSizeThreshold should be small enough
  - spark.databricks.optimizer.deltaTableFilesThreshold should be small enough
  - spark.databricks.optimizer.dynamicFilePruning should be true

Important note: if the join keys on the build side are such that they hit every files of the probe side,
even having z-oder and activating DFP, there will still be read amplification.

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

// Note the total number of files after z-ordering (8)
DeltaTable.forPath(probeDir).detail.selectExpr("numFiles as total_number_of_files_after_zorder").show

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

// Note that only 1 files out of 8 contains the country_code 'FR'
spark.sparkContext.setJobDescription(s"Display max/min stats for files present in delta table after z-order")
getMaxMinStats(probeDir, "country_code", 1).show(false)

// COMMAND ----------

// Build side
case class Employee(name: String, role: String, residence: String)
val employeesData = Seq(
  Employee("Thierry", "Software Engineer", "FR"),
  Employee("Mohammed", "DevOps", "FR"),
  Employee("Gene", "Intern", "FR"),
  Employee("Mau", "Intern", "FR"),
  // Important note: if the join keys on the build side are such that they hit every files of the probe side,
  // even having z-oder and activating DFP, there will still be read amplification.
  // Uncomment the following line to see that 1 more file will be read just for one new key (ZA).
  // Employee("Lex", "Intern", "ZA"),
  Employee("Mathieu", "Software Engineer", "FR"),
  Employee("Thomas", "Intern", "FR")
)
spark.sparkContext.setJobDescription("Create build table")
employeesData.toDF.write.mode("overwrite").format("delta").save(buildDir)

// COMMAND ----------

// Join a 2 tables (probe and build), adding a filter in the smaller one (the build side of the join)
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
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 1000000000L)
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

// Join a 2 tables (probe and build), without adding a filter in the smaller one (the build side of the join)
def joinTablesNoFilter(description: String): Unit = {
  val probe = DeltaTable.forPath(probeDir).toDF
  val build = DeltaTable.forPath(buildDir).toDF
  spark.sparkContext.setJobDescription(s"Join tables: $description")
  probe
  .join(build, build("residence") === probe("country_code"), "inner")
  // No filter here
  .count()
}

// Second scenario: DFP activated but no filter in the build side
joinTablesNoFilter("DFP-no-filter")

// Now do the same checks as above for the join without filter.
// This time DFP does not kick in, so we get the same results as in the first scenario, without DFP.