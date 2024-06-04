// Databricks notebook source
// Spark: 3.5.1
// Local: --driver-memory 1G --master 'local[2]' --conf spark.ui.retainedJobs=2

// COMMAND ----------

/*
This snippet shows an example of an application hitting the limits of Spark UI retention of jobs (same applies for stages or tasks)
and ways to work around it.

# Symptom
Spark UI showing something like "Completed Jobs (4, only showing 2)" (same for stages and tasks). However you wanted to
measure some metrics for those, like CPU duration, spill, etc.

# Explanation

The Spark UI tries to retain as many jobs, stages and tasks available as requested per configuration.
The settings are spark.ui.retained* . If the application has more to keep than those, the Spark UI will drop
the difference. However there are no guarantees that exactly that amount will be retained. There are ways to work
around this limitation.

Some solutions: 
- 1. Simply increase the amount of retained jobs, stages and/or tasks (mind that the amount retained is not guaranteed, 
     so there is still a risk you wont be able to see all). Settings are for jobs spark.ui.retainedJobs, 
     for stages spark.ui.retainedStages and for tasks spark.ui.retainedTasks.
- 2. Persist the event logs using spark.eventLog.enabled=true and spark.eventLog.dir=/tmp/spark-events (as an example) and 
     then explore them post-mortem via the history server (go to spark installation directory, and run ./sbin/start-history-server.sh)
- 3. Write your own listeners and dispatch whatever information appropriate to your monitoring system, example provided.
*/

// COMMAND ----------

import org.apache.spark.sql.SparkSession
import org.apache.spark.scheduler._
import spark.implicits._

val spark: SparkSession = SparkSession.active

case class JobMetrics(jobId: Int, jobDescription: String, jobGroup: String, sqlId: String, inputReadMb: Float, outputWriteMb: Float, shuffleReadMb: Float, shuffleWriteMb: Float, execCpuSecs: Float, spillMb: Float, stages: Int)
class OurListener() extends SparkListener {

  private case class JobRef(description: String, group: String, sqlId: String, stageIds: Seq[Int]) // job info only available in start event
  // keep track of job descriptions and stage metrics until the end of the job
  private val jobIdToName = new java.util.concurrent.ConcurrentHashMap[Int, JobRef]()
  private val stageIdToStageInfo = new java.util.concurrent.ConcurrentHashMap[Int, StageInfo]()

  val jobMetrics = scala.collection.mutable.ListBuffer.empty[JobMetrics] // collected metrics for jobs

  override def onJobStart(job: SparkListenerJobStart): Unit = // keep this job event, as it contains its description and tasks
    jobIdToName.put(job.jobId, jobRefFrom(job.stageInfos, job.properties))

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = { // merge start event with end event and metrics of stages
    val jobDesc = jobIdToName.get(jobEnd.jobId)
    val stagesStats = jobDesc.stageIds.flatMap(s => Option(stageIdToStageInfo.get(s)))
    jobMetrics += (
      JobMetrics(jobId = jobEnd.jobId, jobDescription = jobDesc.description, jobGroup = jobDesc.group, sqlId = jobDesc.sqlId,
        inputReadMb = stagesStats.map(_.taskMetrics.inputMetrics.bytesRead.toFloat / 1024 / 1024).sum,
        outputWriteMb = stagesStats.map(_.taskMetrics.outputMetrics.bytesWritten.toFloat / 1024 / 1024).sum,
        shuffleReadMb = stagesStats.map(_.taskMetrics.shuffleReadMetrics.totalBytesRead.toFloat / 1024 / 1024).sum,
        shuffleWriteMb = stagesStats.map(_.taskMetrics.shuffleWriteMetrics.bytesWritten.toFloat / 1024 / 1024).sum,
        execCpuSecs = stagesStats.map(_.taskMetrics.executorCpuTime.toFloat / 1024 / 1024 / 1024).sum,
        spillMb = stagesStats.map(_.taskMetrics.memoryBytesSpilled.toFloat / 1024 / 1024).sum,
        stages = jobDesc.stageIds.size
      )
    )
    jobIdToName.remove(jobEnd.jobId) // keep maps small
    jobDesc.stageIds.foreach(s => stageIdToStageInfo.remove(s)) // keep maps small
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = // keep stage metrics
    stageIdToStageInfo.put(stageCompleted.stageInfo.stageId, stageCompleted.stageInfo)

  private def jobRefFrom(stageInfos: Seq[StageInfo], properties: java.util.Properties): JobRef = {
    // Property names copied from org.apache.spark.context
    val d = Option(properties.getProperty("spark.job.description")).map(_.replace('\n', ' ')).mkString
    val g = Option(properties.getProperty("spark.jobGroup.id")).map(_.replace('\n', ' ')).mkString
    val i = properties.getProperty("spark.sql.execution.id")
    val s = stageInfos.map(i => i.stageId)
    JobRef(description = d, group = g, sqlId = i, stageIds = s)
  }
}

spark.conf.set("spark.sql.adaptive.enabled", false) // make executions more predicatable

val inputPath = "/tmp/perf-hikes/datasets/optd_por_public_filtered.csv"
val df = spark.read.option("delimiter", "^").option("header", "true").csv(inputPath)
val listener = new OurListener()
spark.sparkContext.addSparkListener(listener)
Range.inclusive(1, 30000, 10000) foreach { n => // 3 jobs will be launched, but we will see less in Spark UI because of spark.ui.retainedJobs
  spark.sparkContext.setJobGroup(groupId = s"Group $n", description = s"Write with repartition ${n}")
  df.repartition(n).write.format("noop").mode("overwrite").save() // more repartition = more overhead = slower job
}

listener.jobMetrics.toDS.orderBy("execCpuSecs").show(false)
// As per the proposed solutions above:
// 1. Just increase the corresponding settings.
// 2. Persist history event logs with spark.eventLog settings (as described above), launch spark history server and
//    see the persisted history (with ALL jobs regardless of the retention set).
// 3. Use something like OurListener and explore its output.
