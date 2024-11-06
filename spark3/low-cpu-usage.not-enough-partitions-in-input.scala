// Databricks notebook source
// Spark: 3.5.1
// Local: --driver-memory 1G --master 'local[8]'
// Databricks: cluster with 8 cores

// COMMAND ----------

/*
This example shows how to identify and solve the problem of low CPU usage when reading an input dataframe that yields less
partitions than the total amount of cores available.

# Symptom

For a given Job, the CPU usage is low for long periods of time (even when scaling up / out the cluster).
In the Spark UI (live), the 'Executors' tab / 'Active Tasks' metrics is much lower than the 'Cores' metric for a while.
This happens in the first stage of a given job.

# Explanation

The Spark stage related to the input dataset read has not enough tasks to fit available cores, hence wasting
CPU resources.

Parquet files (and delta) are meant to be splitable. The unit of splitability is a row group. Each row group can be 
assigned to a specific task. The row group size is defined by the setting parquet.block.size when writing the dataset.

A single parquet file with multiple row groups can be processed in parallel right at the first Spark stage, however having multiple 
tasks for a given row group is not possible (repartition is mandatory).

In this example we take advantage of the splitability of .parquet files, which allows to have multiple partitions for a
single file. The setting being used is 'spark.sql.files.maxPartitionBytes'. Mind that not all data storage formats
are splittable by design. For instance .parquet is splittable, while .csv compressed with GZIP is not. In the case of
consuming a non-splittable format you wll be forced to do a repartition of the input dataset to achieve similar speedups.

The amount of row-groups in a given parquet file can be analysed using [parquet-tools](https://pypi.org/project/parquet-tools/), 
option 'inspect'.

References: 
- https://celerdata.com/glossary/parquet-file-format#:~:text=Typical%20row%20group%20sizes%20in,on%20the%20specific%20use%20case.
- https://mageswaran1989.medium.com/a-dive-into-apache-spark-parquet-reader-for-small-file-sizes-fabb9c35f64e

Summary: if you have less tasks (pre-shuffle) than cores, try decreasing spark.sql.files.maxPartitionBytes (hopefully input files
will have multiple row-groups that had been grouped together), if it does not work you are forced to do a .repartition().

# What to aim for concretely

During most of the duration of the job, the amount of active tasks is similar to the total amount of cores.

In Databricks, it is possible to see post-mortem the use of CPU along the duration of the whole application. Go to the
cluster running the application, see 'Spark' / 'Active tasks', the curve should stay steadily as high as the total number
of cores available in the cluster.

*/

// COMMAND ----------

// DBTITLE 1,Setup

import java.util.UUID
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

val spark: SparkSession = SparkSession.active

spark.conf.set("spark.sql.files.maxPartitionBytes", 128*1024*1024) // 128 MB, setting it to have idempotent re-runs on the same cluster

spark.conf.set("spark.sql.adaptive.enabled", false)
val input = "/tmp/perf-hikes/datasets/optd_por_public_filtered.csv"
val tmpPath = "/tmp/perf-hikes/sandbox/" + UUID.randomUUID()

spark.sparkContext.setJobDescription("Read CSV")
val airports = spark.read.option("delimiter","^").option("header","true").csv(input)

def showPartitions(df: DataFrame) = df.withColumn("part_id", spark_partition_id()).groupBy("part_id").count().selectExpr("count(distinct(part_id)) as legit_partitions", "max(count) as max_records", "min(count) as min_records").show()

// COMMAND ----------

spark.sparkContext.setJobDescription("Initialize input table")
spark.conf.set("parquet.block.size", 1024 * 1024) // size of the row-group is 1MB

// single parquet file, roughly 4MB each (we create multiple input datasets to avoid warmup effects)
Range(1,100).map(r => airports.withColumn("idx", lit(r))).reduce(_.union(_)).repartition(1).write.format("parquet").save(tmpPath + "/input1")
Range(1,100).map(r => airports.withColumn("idx", lit(r))).reduce(_.union(_)).repartition(1).write.format("parquet").save(tmpPath + "/input2")
Range(1,100).map(r => airports.withColumn("idx", lit(r))).reduce(_.union(_)).repartition(1).write.format("parquet").save(tmpPath + "/input3")

// COMMAND ----------

val expensiveProcessing = "sha(sha(sha(sha(sha(sha(sha(sha(sha(sha(sha(sha(sha(sha(sha(sha(sha(sha(sha(sha(sha(sha(wiki_link))))))))))))))))))))))"

// COMMAND ----------

// DBTITLE 1,Scenario with few partitions
// Scenario with few partitions
spark.sparkContext.setJobDescription("Read input (few partitions)")
val df1 = spark.read.format("parquet").load(tmpPath + "/input1")
showPartitions(df1) // what's the distribution of records per partition?

spark.sparkContext.setJobDescription("Write input (few partitions)")
df1.selectExpr(expensiveProcessing).write.format("noop").mode("overwrite").save()

// COMMAND ----------

// DBTITLE 1,Scenario with many partitions
// Scenario with many partitions
spark.conf.set("spark.sql.files.maxPartitionBytes", 128*1024)
spark.sparkContext.setJobDescription("Read input (many partitions)")
val df2 = spark.read.format("parquet").load(tmpPath + "/input2")
showPartitions(df2) // what's the distribution of records per partition?

spark.sparkContext.setJobDescription("Write input (many partitions)")
df2.selectExpr(expensiveProcessing).write.format("noop").mode("overwrite").save()
// It is possible to see the setting in the Spark UI, Tab 'SQL / DataFrame' then 'SQL / DataFrame Properties'

// COMMAND ----------

// DBTITLE 1,Scenario with repartition
// Scenario with repartition
spark.sparkContext.setJobDescription("Read input (repartitioned)")
val df3 = spark.read.format("parquet").load(tmpPath + "/input3")
spark.sparkContext.setJobDescription("Write input (repartitioned)")
df3.repartition(8*3).selectExpr(expensiveProcessing).write.format("noop").mode("overwrite").save()

// COMMAND ----------

// In the 'Jobs' tab, the 'save' action for each scenario is represented by 1 job 'Write input...'
// Mind that those jobs are very different:
// - for 'Write input (few partitions)': few tasks, longer duration overall
// - for 'Write input (many partitions)': many tasks, shorter duration overall
// - for 'Write input (repartitioned)': many tasks, shorter duration overall
// WARNING: these observations have been made on a Databricks cluster with 2 workers having 4 cores each. 
// To get similar results in local, try to increase the size of the files (e.g. changing Range(1,100) in cell 4).
