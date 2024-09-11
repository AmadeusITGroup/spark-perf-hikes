// Databricks notebook source
// Spark: 3.5.1
// Local: --driver-memory 1G --master 'local[4]'
// Databricks: ...

// COMMAND ----------

/*
This example shows how to identify and solve the problem of low CPU usage when reading a input dataframe that yields less
partitions than the total amount of cores available.

# Symptom

For a given Job, the CPU usage is low for long periods of time (even when scaling up / out the cluster).
In the Spark UI (live), the 'Executors' tab / 'Active Tasks' metrics much lower than the 'Cores' metric for a while.
This happens in the first stage of a job.

# Explanation

The Spark stage related to the input dataset read has not enough tasks to fit available cores, hence preventing the optimal
distribution of the work across the available cores of the cluster.

In this example we take advantage of the splitability of .parquet files, which allows to have multiple partitions for a
single file. The setting being used is 'spark.sql.files.maxPartitionBytes'. Mind that not all data storage formats
are splittable by design. For instance .parquet is splittable, while .csv compressed with GZIP is not. In the case of
consuming a non-splittable format you wll be forced to do a repartition of the input dataset to achieve similar speedups.

# What to aim for concretely

During most of the duration of the job, the amount of active tasks is similar to the total amount of cores.

In Databricks, it is possible to see post-mortem the use of CPU along the duration of the whole application. Go to the
cluster running the application, see 'Spark' / 'Active tasks', the curve should stay steadily as high as the total number
of cores available in the cluster.

*/

// COMMAND ----------


import java.util.UUID
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
df1.toJavaRDD.partitions.size // what's the number of partitions calculated?
df1.withColumn("part_id", spark_partition_id()).groupBy("part_id").count().show() // what's the distribution of records per partition?

spark.sparkContext.setJobDescription("Write input (as 1 partition)")
df1.write.format("noop").mode("overwrite").save()

// COMMAND ----------

// Scenario with multiple partitions, tweaked spark.sql.files.maxPartitionBytes
spark.sparkContext.setJobDescription("Read input (as multiple partitions)")
spark.conf.set("spark.sql.files.maxPartitionBytes", 1024*125)
val df2 = spark.read.format("parquet").load(tmpPath + "/input")
df2.toJavaRDD.partitions.size // what's the number of partitions calculated?
df2.withColumn("part_id", spark_partition_id()).groupBy("part_id").count().show() // what's the distribution of records per partition?

spark.sparkContext.setJobDescription("Write input (as multiple partitions)")
df2.write.format("noop").mode("overwrite").save()
// It is possible to see the setting in the Spark UI, Tab 'SQL / DataFrame' then 'SQL / DataFrame Properties'

// In the 'Jobs' tab, the 'save' action for each scenario is represented by 1 job 'Write input...'
// Mind that those jobs are very different:
// - for 'Write input (as 1 partition)' job with default settings: 1 task, longer duration
// - for 'Write input (as multiple partitions)' with tweaked ones: 4 tasks, shorter duration
