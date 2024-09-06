// Databricks notebook source
// Spark: 3.5.1
// Local: --driver-memory 1G --master 'local[4]' --packages io.delta:delta-spark_2.12:3.1.0 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
// Databricks: ...

// COMMAND ----------

/*
This example shows ...

# Symptom

# Explanation

# What to aim for concretely
*/

// COMMAND ----------


import java.util.UUID
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, SparkSession}

val spark: SparkSession = SparkSession.active

spark.conf.set("spark.sql.adaptive.enabled", false)
val input = "/tmp/perf-hikes/datasets/optd_por_public_filtered.csv"
val tmpPath = "/tmp/perf-hikes/sandbox/" + UUID.randomUUID()

spark.sparkContext.setJobDescription("Read CSV")
val airports = spark.read.option("delimiter","^").option("header","true").csv(input)

// COMMAND ----------

spark.sparkContext.setJobDescription("Initialize input table")
airports.repartition(1).write.format("parquet").save(tmpPath + "/input") // create a table with a single partition (~500KB)

// COMMAND ----------

// Scenario with 1 partition, default spark.sql.files.maxPartitionBytes
spark.sparkContext.setJobDescription("Read input (as 1 partition)")
val df1 = spark.read.format("parquet").load(tmpPath + "/input")
df1.toJavaRDD.partitions.size // what's the number of partitions?

spark.sparkContext.setJobDescription("Write input (as 1 partition)")
df1.write.format("noop").mode("overwrite").save()

// COMMAND ----------

// Scenario with multiple partitions, tweaked spark.sql.files.maxPartitionBytes
spark.sparkContext.setJobDescription("Read input (as multiple partitions)")
spark.conf.set("spark.sql.files.maxPartitionBytes", 1024*125)
val df2 = spark.read.format("parquet").load(tmpPath + "/input")
df2.toJavaRDD.partitions.size // what's the number of partitions?

spark.sparkContext.setJobDescription("Write input (as multiple partitions)")
df2.write.format("noop").mode("overwrite").save()
// It is possible to see the setting in the Spark UI, Tab 'SQL / DataFrame' then 'SQL / DataFrame Properties'

// In the 'Jobs' tab, the 'save' action for each scenario is represented by 1 job 'Write input...'
// Mind that those jobs are very different:
// - for 'Write input (as 1 partition)' job with default settings: 1 task, longer duration
// - for 'Write input (as multiple partitions)' with tweaked ones: 4 tasks, shorter duration
