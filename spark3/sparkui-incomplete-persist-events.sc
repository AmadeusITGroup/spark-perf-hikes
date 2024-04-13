// Spark: 3.5.1
// Local: --executor-memory 1G --driver-memory 1G --executor-cores 1 --master local[2] --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=/tmp/spark-events --conf spark.ui.retainedJobs=2 --conf spark.ui.retainedStages=2 --conf spark.ui.retainedTasks=2
// Databricks: ...


// COMMAND ----------

/*
This snippet shows an example of an application hitting the limits of Spark UI retention of jobs (same applies for stages or tasks)
and ways to work it around.

# Symptom
Spark UI showing something like "Completed Jobs (4, only showing 2)" (same for stages and tasks). However you wanted to
measure some metrics for those, like CPU duration, spill, etc.

# Explanation

The Spark UI works in a best effort mode to keep as many jobs, stages and tasks available for analysis as requested per configuration. 
Those thresholds are configured via spark.ui.retained* settings. If the application has more to keep than those, the Spark UI will drop 
the difference. However there are no guarantees that exactly that amount will be retained. There are ways to work around this limitation.

Some solutions: 
- 1. Simply increase the amount of retained jobs, stages and/or tasks (mind that the amount retained is not guaranteed, 
     so there is still a risk you wont be able to see all). Settings are for jobs spark.ui.retainedJobs, 
     for stages spark.ui.retainedStages and for tasks spark.ui.retainedTasks.
- 2. Persist the event logs using spark.eventLog.enabled=true and spark.eventLog.dir=/tmp/spark-events (as an example) and 
     then explore them post-mortem via the history server (go to spark installation directory, and run ./sbin/start-history-server.sh)
- 3. Write your own listeners and dispatch whatever information appropriate to your monitoring system

*/

// COMMAND ----------

import org.apache.spark.sql.SparkSession

val spark: SparkSession = SparkSession.active

// The location of our non-skewed set of transactions
val inputPath = "/tmp/amadeus-spark-lab/datasets/optd_por_public_all.csv"
val df = spark.read.option("delimiter", "^").option("header", "true").csv(inputPath)

Range(0,3) foreach { n => // 3 jobs will be launched, but we will see less in Spark UI because of spark.ui.retainedJobs 
  spark.sparkContext.setJobDescription(s"Write number ${n}")
  df.write.format("noop").mode("overwrite").save()
}
// As per the proposed solutions: 
// 1. Just increase the corresponding settings
// 2. Launch spark history server ./sbin/start-history-server.sh (pointing by default to /tmp/spark-events) and see the persisted history (with ALL jobs regardless of the retention set).
// 3. Explore the output of the listener


