// Databricks notebook source
// Spark: 3.5.1
// Local: --master 'local[8]' --driver-memory 1G --conf spark.driver.bindAddress=127.0.0.1 --conf spark.driver.host=127.0.0.1
// Databricks: cluster with 2 executors, 4 cores each

// COMMAND ----------

/*
This example shows how to improve overall throughput by submitting multiple independent Spark jobs
concurrently from the driver, rather than sequentially.

# Symptom

The cluster has available resources (cores, memory) but the application submits
jobs sequentially, leaving the cluster underutilized. 
In the Spark UI timeline, jobs appear as a sequential chain, and the 'Active Tasks' metric 
stays well below the total core count for extended periods.

# Explanation

By default, Spark actions triggered from the driver run sequentially: the driver blocks until one job
completes before submitting the next. When you have multiple independent workloads (e.g., writing to
different tables, computing independent aggregations), this serialization may waste cluster resources
if Spark stages do not fully utilize available CPU resources (because of skew, lack of parallelism, etc.).

The solution is to submit jobs in parallel. One way is using Scala Futures. Each Future runs on its own thread,
and Spark's scheduler assigns tasks from all active jobs to available executors. 

Key considerations:
- Use `setJobDescription` or `setJobGroup` per thread for readable labels in the Spark UI.
- The degree of driver-side parallelism is controlled by the ExecutionContext thread pool.
- Only parallelize truly independent workloads. Jobs that depend on each other's output must remain
  sequential.

# What to aim for concretely

In the Spark UI timeline, multiple jobs should overlap in time. The 'Active Tasks' metric should stay
close to the total core count throughout the parallel section. Overall wall-clock time should decrease
compared to sequential submission.
*/

// COMMAND ----------

import java.util.UUID
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext
import java.util.concurrent.Executors
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

val spark: SparkSession = SparkSession.active
spark.conf.set("spark.sql.adaptive.enabled", false)

// COMMAND ----------

// Setup: create a moderately expensive workload to simulate real jobs
val input = "/tmp/perf-hikes/datasets/optd_por_public_filtered.csv"
val tmpPath = "/tmp/perf-hikes/sandbox/" + UUID.randomUUID()

spark.sparkContext.setJobDescription("Read CSV input")
val airports = spark.read.option("delimiter", "^").option("header", "true").csv(input)

// Expand the dataset so each job takes a noticeable amount of time.
// Write separate copies so that each job reads a never-read-before dataset, avoiding OS/disk cache effects.
val bigDF = Range(1, 50).map(r => airports.withColumn("idx", lit(r))).reduce(_.union(_))
Range(1, 7).foreach { i =>
  bigDF.write.format("parquet").mode("overwrite").save(tmpPath + s"/input$i")
}

val expensiveExpr = "sha(sha(sha(sha(sha(sha(sha(sha(sha(sha(wiki_link))))))))))"

// COMMAND ----------

// DBTITLE 1,Sequential job submission (baseline)
spark.sparkContext.setJobDescription("Sequential - job 1")
spark.read.parquet(tmpPath + "/input1").selectExpr(expensiveExpr).write.format("noop").mode("overwrite").save()

spark.sparkContext.setJobDescription("Sequential - job 2")
spark.read.parquet(tmpPath + "/input2").selectExpr(expensiveExpr).write.format("noop").mode("overwrite").save()

spark.sparkContext.setJobDescription("Sequential - job 3")
spark.read.parquet(tmpPath + "/input3").selectExpr(expensiveExpr).write.format("noop").mode("overwrite").save()

// COMMAND ----------

// DBTITLE 1,Parallel job submission (improved)
// Use a fixed thread pool to control driver-side concurrency.
// Size it according to how many independent jobs you want in flight.
val threadPool = Executors.newFixedThreadPool(3)
implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(threadPool)

val jobNames = Seq("parallel-job-1", "parallel-job-2", "parallel-job-3")

val futures = jobNames.zipWithIndex.map { case (jobName, idx) =>
  val inputPath = tmpPath + s"/input${idx + 4}" // inputs 4, 5, 6 — never read before
  Future {
    spark.sparkContext.setJobDescription(s"Parallel - $jobName")
    spark.read.parquet(inputPath).selectExpr(expensiveExpr).write.format("noop").mode("overwrite").save()
  }
}

Await.result(Future.sequence(futures), 30.minutes)
threadPool.shutdown()

// COMMAND ----------

// Compare the two approaches in the Spark UI:
// - 'Jobs' tab: sequential jobs appear one after another; parallel jobs overlap in time.
// - 'Stages' tab: parallel submission shows stages from different jobs interleaved.
// - Wall-clock time for the parallel section should be roughly 1/3 of the sequential section
//   (assuming enough cluster resources to run all three concurrently).
//
// Tips:
// - For production use, add error handling per Future (e.g., recover/recoverWith) so that one
//   failing job does not crash the entire batch.
// - Avoid sharing mutable state across Futures. Each job should be self-contained.
