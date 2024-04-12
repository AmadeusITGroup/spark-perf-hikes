// Spark: 3.4.2
// Local: --executor-memory 1G --driver-memory 1G --executor-cores 1
// Databricks: ...

// COMMAND ----------

/*
TODO
*/

// COMMAND ----------

import java.util.UUID
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.DataFrame
import io.delta.tables.DeltaTable

val spark: SparkSession = SparkSession.active

// See https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/TestUtils.scala
// In logs look for: INFO ExternalSorter: Task 1 force spilling in-memory map to disk it will release 232.1 MB memory
class SpillListener extends org.apache.spark.scheduler.SparkListener {
  import org.apache.spark.scheduler.{SparkListenerTaskEnd,SparkListenerStageCompleted}
  import org.apache.spark.executor.TaskMetrics
  import scala.collection.mutable

  private val stageIdToTaskMetrics = new mutable.HashMap[Int, mutable.ArrayBuffer[TaskMetrics]]
  private val spilledStageIds = new mutable.HashSet[Int]

  def numSpilledStages: Int = synchronized { spilledStageIds.size }
  def reset(): Unit = synchronized { spilledStageIds.clear }
  def report(): Unit = synchronized { println(f"Spilled Stages: ${numSpilledStages}%,d") }
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = synchronized {
    stageIdToTaskMetrics.getOrElseUpdate(taskEnd.stageId, new mutable.ArrayBuffer[TaskMetrics]) += taskEnd.taskMetrics
  }

  override def onStageCompleted(stageComplete: SparkListenerStageCompleted): Unit = synchronized {
    val stageId = stageComplete.stageInfo.stageId
    val metrics = stageIdToTaskMetrics.remove(stageId).toSeq.flatten
    val spilled = metrics.map(_.memoryBytesSpilled).sum > 0
    if (spilled) spilledStageIds += stageId
  }
}
val spillListener = new SpillListener()
spark.sparkContext.addSparkListener(spillListener)
spillListener.reset()

// Create a large partition by mismanaging shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", 1)
// Disable AQE to have a single job
spark.conf.set("spark.sql.adaptive.enabled", false)

// The location of our non-skewed set of transactions
val trxPath = "/tmp/amadeus-spark-lab/datasets/optd_por_public.csv"
def df() = spark.read.option("delimiter", "^").option("header", "true").csv(trxPath)
val dfs = (1 to 100).map(_ => df()).reduce(_ union _)

spark.sparkContext.setJobDescription("Wide operation with shuffle.partitions = 1")
// Then you can explore the UI (normally on localhost:4040 and look for spills on the Stages or SQL sections) (Spill fields not shown in case of no spill...)
dfs.orderBy("name")/*Some wide transformation*/.write.format("noop").mode("overwrite").save() // Execute a noop write to test

spillListener.report()
