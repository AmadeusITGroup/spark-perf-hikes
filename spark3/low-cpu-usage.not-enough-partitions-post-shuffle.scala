// Databricks notebook source
// Spark: 3.5.1
// Local: --driver-memory 1G --master 'local[4]'
// Databricks: ...

// COMMAND ----------

/*
This example shows how to identify and solve the problem of low CPU usage in a post-shuffle stage made of less
tasks than the total amount of cores available.

# Symptom

For a given Job, the CPU usage is low for long periods of time (even when scaling up / out the cluster).
In the Spark UI (live), the 'Executors' tab / 'Active Tasks' metrics much lower than the 'Cores' metric for a while.
This happens not in the first stage of a job, but in later ones (post-shuffle).

# Explanation

A post-shuffle stage (non-first stage of a job containing join, distinct, group by, ...). is made of tasks.
The amount of tasks of such kind of stages needs to be enough to fit them into available cores. By doing this we optimally
distribute the work across the available cores of the cluster.

References:
- https://spark.apache.org/docs/latest/rdd-programming-guide.html#transformations
- https://spark.apache.org/docs/latest/rdd-programming-guide.html#shuffle-operations

The amount of partitions/tasks present in a post-shuffle stage is driven by the parameter 'spark.sql.shuffle.partitions'.
In Databricks it is possible set the value 'auto' to let Spark figure out the best value automatically.

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
Range(1,100).map(r => airports.withColumn("idx", lit(r))).reduce(_.union(_)).write.format("parquet").save(tmpPath + "/input")

// COMMAND ----------

// Scenario with 1 post-shuffle partition, spark.sql.shuffle.partitions=1
spark.sparkContext.setJobDescription("Read (1 post-shuffle partition)")
spark.conf.set("spark.sql.shuffle.partitions", "1")
val df1 = spark.read.format("parquet").load(tmpPath + "/input")
spark.sparkContext.setJobDescription("Shuffle (1 post-shuffle partition)")
df1.distinct().write.format("noop").mode("overwrite").save()

// COMMAND ----------

// Scenario with multiple post-shuffle partitions, spark.sql.shuffle.partitions=4
spark.sparkContext.setJobDescription("Read (4 post-shuffle partitions)")
spark.conf.set("spark.sql.shuffle.partitions", "4")
val df2 = spark.read.format("parquet").load(tmpPath + "/input")
spark.sparkContext.setJobDescription("Shuffle (4 post-shuffle partitions)")
df2.distinct().write.format("noop").mode("overwrite").save()
// It is possible to see the setting in the Spark UI, Tab 'SQL / DataFrame' then 'SQL / DataFrame Properties'

// In the 'Jobs' tab, the 'save' action for each scenario is represented by 1 job 'Shuffle...'
// Mind that those jobs are very different:
// - for 'Shuffle (1...partitions)' job: 2 stages, the last stage (post-shuffle) has 1 task, longer duration
// - for 'Shuffle (4...partitions)' job: 2 stages, the last stage (post-shuffle) has 4 tasks, shorter duration
