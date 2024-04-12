// Spark: 3.5.1
// Local: --executor-memory 1G --driver-memory 1G --executor-cores 1 --master local[2] --packages io.delta:delta-spark_2.12:3.1.0 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
// Databricks: ...

// COMMAND ----------

/*
This example shows how to address the read amplification problem in case of merges,
using Dynamic File Pruning (DFP) and Photon, coupled with a data skipping friendly data layout such as z-order.

References:
- https://docs.databricks.com/en/optimizations/dynamic-file-pruning.html
- https://www.databricks.com/blog/2020/04/30/faster-sql-queries-on-delta-lake-with-dynamic-file-pruning.html
- https://docs.databricks.com/en/delta/data-skipping.html#delta-zorder

IMPORTANT: DFP and Photon are only available when running on Databricks.

# Symptom
You are merging a small dataframe into a delta table and you are reading way more data that is actually needed
to perform the merge.
You know that you could read less data, because you know that only few records in the input dataframe will match
with records in the big table.

# Explanation

The delta table is z-ordered on the merge key, in order to co-locate closer keys in the same set of files.
We assume that we are running on Databricks using Photon.

First scenario: No DFP
Here we see that the whole delta table is read for the merge.

Second scenario: DFP
Here we see that only the delta table files containing the merge keys present in the small dataframe are read.

In order to make sure that DFP can kick-in:
  - the small dataframe must be broadcastable
  - spark.databricks.optimizer.deltaTableSizeThreshold should be small enough
  - spark.databricks.optimizer.deltaTableFilesThreshold should be small enough
  - spark.databricks.optimizer.dynamicFilePruning should be true

Important note: if the keys in the input dataframe are such that they hit every files of the table,
even having z-oder and activating DFP, there will still be read amplification.
...

*/

// COMMAND ----------

import java.util.UUID
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.DataFrame
import io.delta.tables.DeltaTable

val input = "/tmp/amadeus-spark-lab/datasets/optd_por_public_filtered.csv"
val tmpPath = "/tmp/amadeus-spark-lab/sandbox/" + UUID.randomUUID()
val deltaDir = tmpPath + "/input"

val spark: SparkSession = SparkSession.active

// COMMAND ----------

// Destination delta table
spark.sparkContext.setJobDescription("Create delta table")
val airports = spark.read.option("delimiter","^").option("header","true").csv(input)
airports.write.mode("overwrite").format("delta").save(deltaDir)

// z-order the delta table on the merge key
spark.sparkContext.setJobDescription("Z-order delta table")
spark.conf.set("spark.databricks.delta.optimize.maxFileSize", 50 * 1024L)
DeltaTable.forPath(deltaDir).optimize().executeZOrderBy("iata_code")

// COMMAND ----------

// Note the total number of files after z-ordering
DeltaTable.forPath(deltaDir).detail.select("numFiles").show

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

// Note that only 1 files out of 9 contains the iata_code 'AAI'
spark.sparkContext.setJobDescription(s"Display max/min stats for files present in delta table after z-order")
getMaxMinStats(deltaDir, "iata_code", 1).show(false)

// COMMAND ----------

def buildDataframeToMerge(iataCode: String, newVal: String): DataFrame =
  airports.where(col("iata_code") === iataCode).drop("comment").withColumn("comment", lit(newVal))

def merge(df: DataFrame): Unit = {
  val keyCols = Seq("iata_code")
  DeltaTable
    .forPath(deltaDir).as("o")
    .merge(df.as("t"), keyCols.map(colName => s"t.$colName == o.$colName").mkString(" and "))
    .whenMatched.updateAll.whenNotMatched.insertAll.execute()
}

// First scenario: No DFP
spark.conf.set("spark.databricks.optimizer.dynamicFilePruning", "true")
spark.sparkContext.setJobDescription("Merge dataframe - NO DFP")
merge(buildDataframeToMerge("AAI", "no-dfp"))

// Be sure to have the conditions to trigger DFP: DFP enabled, broadcast join, table/files thresholds
spark.conf.set("spark.databricks.optimizer.dynamicFilePruning", "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 1000000000L)
spark.conf.set("spark.databricks.optimizer.deltaTableSizeThreshold", 1)
spark.conf.set("spark.databricks.optimizer.deltaTableFilesThreshold", 1)

// Second scenario: DFP
spark.conf.set("spark.databricks.optimizer.dynamicFilePruning", "true")
spark.sparkContext.setJobDescription("Merge dataframe - DFP")
merge(buildDataframeToMerge("AAI", "dfp"))

// Go to the Databricks Spark UI, look for the SQL query corresponding to the Merge,
// and see the number of files actually read in the 'PhotonScan parquet' node
