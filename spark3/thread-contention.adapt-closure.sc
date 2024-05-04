// Spark: 3.5.1
// Local: --master 'local[4]' --driver-memory 1G
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

To solve the issue there are alternatives: 
- use built-in SQL functions whenever possible (all of them are free of thread contention, just watch out for the function 'reflect')
- fix your closure/udf so that it avoids going through the synchronized code section
*/

// COMMAND ----------
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}
val spark: SparkSession = SparkSession.active
// COMMAND ----------
val threadLockOperation = udf { (s: String) =>
  val token = System.getProperties // just a token to lock on within the same JVM (same worker)
  token.synchronized { // comment this line out to remove the thread contention!
    val t = Range.inclusive(1, 5000).sum
    s"op($s($t))"
  }                    // comment this line out to remove the thread contention!
}
val input = "/tmp/perf-hikes/datasets/optd_por_public_filtered.csv"
val df = spark.read.option("delimiter", "^").option("header", "true").csv(input).cache()
// COMMAND ----------
spark.sparkContext.setJobDescription("Dataframe save with UDF with thread contention")
val iterations = 10000
Range.inclusive(1, iterations).foreach { _ =>
  df.select(threadLockOperation(col("name"))).write.format("noop").mode("overwrite").save()
}
