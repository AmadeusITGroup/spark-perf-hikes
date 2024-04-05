// Local: --executor-memory 1G --driver-memory 1G --executor-cores 4
// Databricks: ...

// COMMAND ----------

/*
This example shows how a UDF that relies on a concurrency lock can
slow down the execution of a Spark job. Same is applicable for a Scala closure.

# Symptom
The CPU usage is low even during CPU intensive stages. Also in the Spark UI,
under Executors, the "Thread State" of "Executor task launch workers ..." threads are in state different than "RUNNABLE"
for too many threads.

# Explanation
The UDF `threadLockOperation` uses a concurrency lock on a one-per JVM object to synchronize access to a critical section.
This means that in a Spark stage that computes the values for such UDF, multiple tasks (running on different threads of the JVM)
will be contending for the same lock, and only one will prevail at a time.
As a consequence, the parallelism of the stage will be negatively limited.
*/

// COMMAND ----------
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}
val spark: SparkSession = SparkSession.active
// COMMAND ----------
val threadLockOperation = udf { (s: String) =>
  val token = System.getProperties // just a token to lock on within the same JVM (same worker)
  token.synchronized { s"op($s)" }
}
val input = "/tmp/amadeus-spark-lab/datasets/optd_por_public_all.csv"
val df = spark.read.option("delimiter", "^").option("header", "true").csv(input).cache()
// COMMAND ----------
spark.sparkContext.setJobDescription("Dataframe save with UDF with thread contention")
val iterations = 10000
Range.inclusive(1, iterations).foreach { _ =>
  df.select(threadLockOperation(col("asciiname"))).write.format("noop").mode("overwrite").save()
}
