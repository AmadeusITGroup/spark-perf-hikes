// Databricks notebook source
// Spark: 3.5.1
// Local: --driver-memory 1G --master 'local[2]' --packages io.delta:delta-spark_2.12:3.3.1 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
// Heads up, locally we cannot reproduce DPP!!!  // Databricks: 13.3LTS+photon

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

// Keep AQE on, to avoid having too many files after merge
spark.conf.set("spark.sql.adaptive.enabled", true)

// Destination delta table
import org.apache.spark.sql.functions.{col, substr}
spark.sparkContext.setJobDescription("Create delta table")
val airports = spark.read.option("delimiter", "^").option("header", "true").csv(input).withColumn("partition", substr(col("iata_code"), lit(1), lit(1)))

// COMMAND ----------

airports.write.partitionBy("partition").mode("overwrite").format("delta").save(deltaDir)

// COMMAND ----------

// Note: we are artificially applying a filter on the input data frame that will be merged.
// Without this, DPP is not triggered
def filterDfBeforeMerge(df: DataFrame, iataCode: String): DataFrame = {
  df.where(col("iata_code") === iataCode)
}

def buildDataframeToMerge(iataCode: String, newVal: String): DataFrame = {
  val c = airports.where(col("iata_code") === iataCode).withColumn("comment", lit(newVal)).collect()
  val df = spark.createDataFrame(spark.sparkContext.parallelize(c), airports.schema)
  filterDfBeforeMerge(df, iataCode)
}

def buildDeltaTableToMerge(iataCode: String, newVal: String): DataFrame = {
  val df = buildDataframeToMerge(iataCode, newVal)
  val path = s"$tmpPath/batch_${iataCode}_${newVal}_${UUID.randomUUID()}"
  df.write.mode("overwrite").format("delta").save(path)
  filterDfBeforeMerge(DeltaTable.forPath(path).toDF, iataCode)
}

def buildDeltaTableToMergeNoFilter(iataCode: String, newVal: String): DataFrame = {
  val df = buildDataframeToMerge(iataCode, newVal)
  val path = s"$tmpPath/batch_${iataCode}_${newVal}_${UUID.randomUUID()}"
  df.write.mode("overwrite").format("delta").save(path)
  DeltaTable.forPath(path).toDF
}

def merge(df: DataFrame): Unit = {
  val keyCols = Seq("iata_code", "partition")
  DeltaTable
    .forPath(deltaDir).as("o")
    .merge(df.as("t"), keyCols.map(colName => s"t.$colName == o.$colName").mkString(" and "))
    .whenMatched.updateAll.whenNotMatched.insertAll.execute()
}

// COMMAND ----------

spark.sparkContext.setJobDescription("Build DFs to merge")
val df1 = buildDataframeToMerge("AAI", "dpp-no-delta1")
val delta1 = buildDeltaTableToMerge("AAI", "dpp-delta1")
val delta2 = buildDeltaTableToMerge("AAI", "dpp-delta2")
val delta3 = buildDeltaTableToMergeNoFilter("AAI", "dpp-delta2")

// COMMAND ----------

// Disable DPP
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "false")

// DPP (input from delta)
spark.sparkContext.setJobDescription("Merge 1 - No DPP - Disabled")
merge(delta1)

// COMMAND ----------

// Be sure to have the conditions to trigger DPP: DPP enabled, broadcast join
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 1000000000L)

// COMMAND ----------

// DPP (input not from delta)
spark.sparkContext.setJobDescription("Merge 2 - No DDP - From DF")
merge(df1)

// COMMAND ----------
// DPP (input from delta)
spark.sparkContext.setJobDescription("Merge 3 - DPP")
merge(delta2)

// COMMAND ----------
// No DPP (missing filter)
spark.sparkContext.setJobDescription("Merge 4 - No DPP - Missing filter")
merge(delta3)
