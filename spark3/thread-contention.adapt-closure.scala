// Spark: 3.5.1
// Local: --master 'local[8]' --driver-memory 1G
// Databricks: ...

// COMMAND ----------

/*
This example shows how a UDF that relies on a concurrency lock can greatly
slow down the execution of a Spark job. Same is applicable for a Scala closure.

# Symptom
The CPU usage is low even during CPU intensive stages. Also in the live Spark UI (observed while the job is running),
under "Executors", threads "Executor task launch workers ..." have "Thread State" mostly different than "RUNNABLE", 
except for one that is actually "RUNNABLE" (and doing something useful).

# Explanation
The UDF `threadLockOperation` uses a concurrency lock on a one-per JVM object to synchronize access to a critical section.
This means that in a Spark stage that computes the values for such UDF, multiple tasks (running on different threads of the JVM)
will be contending for the same lock, and only one will prevail at a time.
As a consequence, the parallelism of the stage will be negatively limited.
On the other side, the UDF `noThreadLockOperation` removes the lock.

To solve the issue in your specific situation there are multiple alternatives: 
- use built-in SQL functions whenever possible (all of them are free of thread contention, just watch out for the function 'reflect')
- fix your closure/udf so that it avoids going through the synchronized code section

Watch out, you will have few seconds to see the Spark UI while the contention is taking place.

# What to aim for concretely

All the 'Executor task launch workers' threads in a worker, upon sampling, should be in state RUNNABLE.
If not the case, you can check their locks. Threads should not be blocked because the same lock is shared.
*/

// COMMAND ----------
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}
val spark: SparkSession = SparkSession.active
// COMMAND ----------
val threadLockOperation = udf { (s: String) =>
  val token = System.getProperties // just a token to lock on within the same JVM (same worker)
  token.synchronized { // comment this line out to remove the thread contention!
    Thread.sleep(30)   // simulate operation that takes some time (increase if needed to have more time to see it in the Live Spark UI)
    s"op($s)"
  }                    // comment this line out to remove the thread contention!
}
val noThreadLockOperation = udf { (s: String) =>
  Thread.sleep(30)   // simulate operation that takes some time
  s"op($s)"
}

val input = "/tmp/perf-hikes/datasets/optd_por_public_filtered.csv"
val df = spark.read.option("delimiter", "^").option("header", "true").csv(input).repartition(200)
// COMMAND ----------
// Now take a look at the Spark UI!
spark.sparkContext.setJobDescription("Save with udf WITH thread contention")
df.select(threadLockOperation(col("name"))).write.format("noop").mode("overwrite").save()
// COMMAND ----------
spark.sparkContext.setJobDescription("Save with udf WITHOUT thread contention")
df.select(noThreadLockOperation(col("name"))).write.format("noop").mode("overwrite").save()
