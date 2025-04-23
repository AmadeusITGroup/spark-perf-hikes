// Databricks notebook source
// MAGIC %md
// MAGIC // Spark: 3.5.0
// MAGIC // Local: --master 'local[2]' --driver-memory 1G
// MAGIC // Databricks: DBR 15.4 LTS. Driver and 2 workers same VM: Standard F4, 8GB , 4 cores
// MAGIC
// MAGIC // COMMAND ----------
// MAGIC /* Compare shuffle metrics and performance of flatMapGroups along with proposed alternatives
// MAGIC
// MAGIC # Symptom
// MAGIC Too much data is being shuffled and potentially spilled. Long execution times
// MAGIC
// MAGIC # Explanation
// MAGIC flatMapGroups is a method of the typed KeyGroupedDataset API in Spark.
// MAGIC The goal of this notebook is too educate and guide users about the risk of massive data shuffled and potentially spilled associated using flatMapGroups in order to use it properly or avoid using it altogether. 
// MAGIC After a brief description and explantion of how it works, a bad usage example will be given, a proper usage example and alternatives will be explored in the dataset, dataframe or rdd api. Latest associated documentation: https://spark.apache.org/docs/3.5.2/api/java/org/apache/spark/sql/KeyValueGroupedDataset.html#flatMapGroups-scala.Function2-org.apache.spark.sql.Encoder- 
// MAGIC
// MAGIC # What to aim for concretely
// MAGIC Run every cell and examine and compare the execution plan, overall performance and  amount of data shuffled and spilled in SparkUI.
// MAGIC Every example is triggered by an action such as show, take or count. We also print the execution plan but its better visualised in Spark UI. For each action you investigate the metrics of the 2 stages in the 2 jobs that are created. 
// MAGIC
// MAGIC // COMMAND ----------
// MAGIC
// MAGIC spark.sparkContext.setJobDescription("JOB_flatMapGroups_analysis")

// COMMAND ----------

// MAGIC %md
// MAGIC This work can be tracked in:
// MAGIC https://jira.amadeus.com/agile/browse/SSCE-132

// COMMAND ----------

// MAGIC %md
// MAGIC flatMapGroups applies the given function to each group of data. For each unique group, the function will be passed the group key and an iterator that contains all of the elements in the group. The function can return an iterator containing elements of an arbitrary type which will be returned as a new Dataset.
// MAGIC For simplicity sake we are going to consider a simple summing function where we some all values of each group. This is a commutative and associative operation.
// MAGIC It'ts intresting to compare how the different implementations of flatMapGroups and the proposed alternatives fare with a normal as well skewed dataset. 
// MAGIC The method below allows to tweak the generated input data. Depending if this is run locally or on a cluster, that values should be set accordingly to test the configuration limits

// COMMAND ----------

//spark.sparkContext.setJobDescription("JOB_flatMapGroups_analysis_data_generation")
import org.apache.spark.sql.{Dataset, SparkSession}
import scala.util.Random

case class Event(userId:String, value:Int)

// COMMAND ----------

// Data generated on driver. Deprecated
// Numbers of users will impact partition size
// 0/9 is the skew ratio => 90% of records under "hot_user"
def generateData(skewed: Boolean, numUsers: Int, numEvents: Int = 1000000 ): Seq[Event] = {
  val users = (1 to numUsers).map(i => s"user_$i")

  val data = if (skewed){
    Seq.fill(numEvents) {
      val userId = if (scala.util.Random.nextFloat() < 0.9) "hot_user" else users(scala.util.Random.nextInt(users.size))
      Event(userId ,scala.util.Random.nextInt(100))
    }
  } else {
    Seq.fill(numEvents) {
      val userId = users(scala.util.Random.nextInt(users.size))
      Event(userId ,scala.util.Random.nextInt(100))
    }
  }

  data
}

// COMMAND ----------

// Data generated on executors

def generateSyntheticData(
  spark: SparkSession,
  numEvents: Long,
  numGroups: Int,
  skewed: Boolean,
  skewRatio: Double = 0.9
): Dataset[Event] = {
  import spark.implicits._

  spark.range(numEvents).map { i =>
    val isSkewed = skewed && Random.nextDouble() < skewRatio
    
    val userId = 
      if (isSkewed) "skewed_user"
      else s"user_${i % numGroups}"

    val value = (i% 100).toInt

    Event(userId,value) 
  }
}

// COMMAND ----------

// MAGIC %md
// MAGIC Partitioning always plays a big role. Usually we want each core to work on 3 to 4 partitions. Her we use a default of 4 per core the corresponds to the factor argument.
// MAGIC Depending wethere you run this notebook locally or on databricks you should set values accordingly.Spark confs "spark.executor.instances
// MAGIC and "spark.executor.cores" or not valide for databricks env.
// MAGIC "spark.databricks.clusterUsageTags.clusterGeneration" is an integer but does not correspond to cores per executor. 
// MAGIC

// COMMAND ----------

def optimalPartitions(factor: Int = 4):Int = {
  val sparkConf = spark.sparkContext.getConf
  //val numExecutors = sparkConf.get("spark.executor.instances").toInt // Only local
  //val coresPerExecutor = sparkConf.get("spark.executor.cores").toInt // Only local
  val numExecutors = sparkConf.get("spark.databricks.clusterUsageTags.clusterWorkers").toInt // clusterTargetWorkers
  //val coresPerExecutor = sparkConf.get("spark.databricks.clusterUsageTags.clusterGeneration").toInt
  val coresPerExecutor = 4
  println(coresPerExecutor)
  val totalCores = numExecutors * coresPerExecutor
  //val totalCores = sc.statusTracker.getExecutorInfos.map(_.numCore).sum
  totalCores * factor
}

val numPartitions = optimalPartitions()

// COMMAND ----------

//import org.apache.spark.sql.Encoder
//import org.apache.spark.sql.Encoders
//implicit val eventEncoder: Encoder[Event] = Encoders.product[Event]

// num partition must be a multiple of cpu cores
//val ds =spark.createDataset(generateData(false, 100)).repartition(100)


// COMMAND ----------

// MAGIC %md
// MAGIC The values choses below make a very skewed dataset with just 1OO groups so we can obtain big partitions.
// MAGIC More events, fewer groups/partitions and more skew will be generally be harder to handle.
// MAGIC AQE will handles the skewed partition that could accentuate certain metrics we are trying to highlight. 
// MAGIC Feel free to tweak any setting here but the explanation will stick to these defaults. 
// MAGIC AQE is enabled by default.

// COMMAND ----------

val ds = generateSyntheticData(
  spark,
  numEvents = 4000000000L,
  numGroups = 100,
  skewed = true,
  skewRatio = 0.95
  )

// Unless adaptive execution is set to false generating skewed data wont create bottleneck/straggler 
//spark.conf.set("spark.sql.adaptive.enabled" ,true)

// COMMAND ----------

ds.repartition(numPartitions)
ds.persist
ds.show

// COMMAND ----------

// MAGIC %md
// MAGIC ### Bad usage of the function. 
// MAGIC It materialises all groups to memory due to .toList
// MAGIC shuffles all data (no combiner)
// MAGIC Causes skew if one key is huge then it also leads to spills
// MAGIC

// COMMAND ----------

spark.sparkContext.setJobDescription("JOB_flatMapGroups_analysis_bad_usage")

import spark.implicits._

val grouped = ds.groupByKey(_.userId)

val result = grouped.flatMapGroups { case(userId, values) =>
  // BAD: values is an Iterator, but this collects everything into memory
  val sum = values.toList.map(_.value).sum
  Iterator((userId, sum))
}

result.explain(true)

// COMMAND ----------

result.show

// COMMAND ----------

// MAGIC %md
// MAGIC Look at job 2 and job 3 latest stages.
// MAGIC Notice  the amount of shuffle time and the read amout, the total as well as the one per taks such as  amount for data spilled to memory and disk and the GC time.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Good Enough use
// MAGIC Suffle is still unavoidable ( we need all items of a group together)
// MAGIC We don't toList or collect -- we stream over Iterator, minimizing memory pressure.
// MAGIC No big spikes, better for skezed groups, no OOM or spill in normal cases.
// MAGIC But still not ideal for large skewed keys: 
// MAGIC If iter is massive (here is some artificial skew might be handy for demonstration purposes), it still hits the same reducer, even if we don't load it all into memory
// MAGIC Could still cause long tails/stragglers

// COMMAND ----------

goodEnoughResult1.show

// COMMAND ----------

// MAGIC %md
// MAGIC Metrics here beyond halved execution time  area almost identical but there is a catch.
// MAGIC This example with way to simple. We are just filtering For consistency and clarity sake we'll also do the sum.

// COMMAND ----------

spark.sparkContext.setJobDescription("JOB_flatMapGroups_analysis_good_enough_usage2")

val goodEnoughResult2  = grouped.flatMapGroups { case (userId, iter) =>
  // stream through iterator and compute sum without materializing the group
  var sum = 0
  while( iter.hasNext) {
    sum += iter.next().value
  }
  Iterator((userId, sum))
}

goodEnoughResult2.explain(true)

// COMMAND ----------

goodEnoughResult2.show

// COMMAND ----------

// MAGIC %md
// MAGIC By looking at same metrics as before we can see that the amount of data shuffled written or read does not differ but that we have a much smaller GC time in the second job which ensures much faster halved execution time than the bad exampole. toList does create a lot of ephimeral objects it then needs to clean up.

// COMMAND ----------

// MAGIC %md
// MAGIC The same example in a purely functional way; 
// MAGIC fold left is lazy in term of iterator consumption
// MAGIC Equivalent perf and sematincs to while loop but cleaner

// COMMAND ----------

spark.sparkContext.setJobDescription("JOB_flatMapGroups_analysis_good_enough_usage2b")
val goodEnoughResult2b  = grouped.flatMapGroups { case (userId, iter) =>
  // stream through iterator and compute sum without materializing the group
  val sum = iter.foldLeft(0)( (acc, event) => acc + event.value)
  Iterator((userId, sum))
}

goodEnoughResult2b.explain(true)

// COMMAND ----------

goodEnoughResult2b.show

// COMMAND ----------

// MAGIC %md
// MAGIC This functional way seems to be slightly faster but probably within margins of error. Same observation to be made between the bad example. Same metrics overall besides a much smaller GC time in second job and halved execution time. 
// MAGIC Could go be more concise with underscore magic.

// COMMAND ----------

spark.sparkContext.setJobDescription("JOB_flatMapGroups_analysis_good_enough_usage2c")
val goodEnoughResult2c  = grouped.flatMapGroups { case (userId, iter) =>
  // stream through iterator and compute sum without materializing the group
  Iterator((userId, iter.foldLeft(0)(_ + _.value)))
}

goodEnoughResult2c.explain(true)

// COMMAND ----------

goodEnoughResult2c.show

// COMMAND ----------

// MAGIC %md
// MAGIC ### Better; Using a Typed Aggregator.
// MAGIC Spark inserts a partial reduce stage (combiner) before shuffle
// MAGIC Memory usage lower
// MAGIC Shuffle volume reduced
// MAGIC Handles skew better

// COMMAND ----------

spark.sparkContext.setJobDescription("JOB_flatMapGroups_analysis_alt_typed_agg")
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.Encoders

// This object defines a strongly-typed custom aggregator using Spark's Dataset API
object SumAgg extends Aggregator[Event, Int, Int] {
  // The zero value is the starting point for aggregation. In this case, the sum starts from 0.
  def zero = 0
  // The reduce step is called for each element in the group. It add the current value to the running total
  def reduce(b: Int, a:Event) = b + a.value
  //The merge step combines two intermediate results (from differents partitions or task).
  def merge(b1: Int, b2: Int) = b1 + b2
  // The finish step returns the final aggregated value after all data has been processed.
  // In our case, the buffer is already the final result, so we return it as is.
  def finish(r: Int) = r
  // Encoder for the intermediate buffer type
  def bufferEncoder = Encoders.scalaInt
  // Encoder for the final output result
  def outputEncoder = Encoders.scalaInt
}

val result2 = ds.groupByKey(_.userId).agg(SumAgg.toColumn)

result2.explain(true)

// COMMAND ----------

result2.show

// COMMAND ----------

// MAGIC %md
// MAGIC Here we see a drastic decrease in amount of data shuffled compared too all previous implementations since a most of tye work is done on the mapper side. No spill occurs.
// MAGIC In the first job we can observe we have a some peak execution memory which was previously 0 that reflects the work done by the combiner (pre shuffle). Since only the result of each partition is shuffled the amout is extremely small. No GC occurs at all on 2nd job but seems to slighlty have increased in the 1job. We have further increased also overall performance even more.

// COMMAND ----------

// MAGIC %md
// MAGIC ### The best: equivalent using DataFrame API
// MAGIC Under the hood, spark is smart enough to combine on map-side( if spark.sql.shuffle.partitions is tuned)
// MAGIC The function you might be intrested could be already part of spark dataframe built in aggregation functions. In that case use it directly

// COMMAND ----------

spark.sparkContext.setJobDescription("JOB_flatMapGroups_analysis_alt_dataframe1")
import spark.implicits._
import org.apache.spark.sql.functions._

val dFEquiv = ds.toDF("userId", "value")
.groupBy(($"userId"))
.agg(sum("value").as("sum"))

dFEquiv.explain(true)

// COMMAND ----------

dFEquiv.show

// COMMAND ----------

// MAGIC %md
// MAGIC The dataframe equivalent has same total shuffle metrics than previously but like the type aggregator most of the work occurs on the mapper side (1 job). Notice we can spot also some peak execution memory. Overall execution is even faster, the fastest.  GC time is further reduced on the mapper side. Here we can witness the whole power of spark dataframe api optimisations under the hood like tungsten.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Equivalent but using Window functions

// COMMAND ----------

spark.sparkContext.setJobDescription("JOB_flatMapGroups_analysis_alt_dataframe_window")
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

//Reuse the Dataset[Event]
val df = ds.toDF()

// Define a partition window per userId
val w = Window.partitionBy("userId")

// Compute sum per userId using window function
val withSum = df.withColumn("sum_value", sum("value").over(w))

withSum.explain(true)

// COMMAND ----------

withSum.show(10)

// COMMAND ----------

// MAGIC %md
// MAGIC Since Window function  adds column to the source dataset, the expected result will be much bigger since the aggregated value is duplicated for each row of every element in each group. This will run infinitely longer and should stopped after entereing the second job. 
// MAGIC The metrics reflect this where we can see much more data shuffled and no execution on the first job. 
// MAGIC This function does not make sense for such a simple sum but might yield more pertinent resutlts if the function needs to operate on tumbling windows or unbounded data. 

// COMMAND ----------

// MAGIC %md
// MAGIC ### Optional RDD Alternative (Combiner-style)
// MAGIC RDD is more verbose, but gives explicit control over aggreagation logic
// MAGIC - Use aggregateByKey when your aggregation logic is associative and you have a neutral starting value (like 0 for sum, 1 for multiplication). Same Zero value for all keys. Result must be same type as input. Ceaner with shared zero value
// MAGIC - Use combine ByKey when you want to initialize the accumulator differently from the input type,or you're doing something more complex like collecting statistics or creating a list per key. Per-key initializer via function. Can change tpe (eg to List). More control, a bit more verbose.

// COMMAND ----------

spark.sparkContext.setJobDescription("JOB_flatMapGroups_analysis_rdd")
// Original Dataset mapped to RDD[(String, Int)]
val pairRdd: RDD[(String, Int)] = ds.rdd.map(event => (event.userId, event.value)) 

// COMMAND ----------

val aggregated = pairRdd.aggregateByKey(0)( //Initial value per key ( starts the sum at 0)
  // mergeValue: how to merge a value into the accumulator (within partition)
  (accumulator: Int, value: Int) => {
    accumulator + value
  },
  // mergeCombinersy: how to merge two accumulators from different partitions
  (acc1: Int, acc2:Int) => {
    acc1 + acc2
  }
)

// View a few results
aggregated.take(10).foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC Here the results are very similar to those by the typed Aggregator and Dataframe api implementation however the layout in spark UI has a bit shifted and compressed :we have only 1 job with 2 stages. Each stage corresponds to the latest stage of each job previously.
// MAGIC The amount of data shuffled has drastically been reduced and most of the work is also data on the mapper/combiner side. GC time and overall performance slighlty reduced compared to typed Aggregator but not as fast as Dataframe. 

// COMMAND ----------

val combined = pairRdd.combineByKey(
  // createCombiner: what to do with the first value for a key
  (value: Int) => {
    // Here we simply use the value itsef to initialize the combiner
    value
  },

  // mergeValue: how to merge a new value into the existing combiner (within a partition)
  (accumulator: Int, value:Int) => {
    accumulator + value
  },

  // mergeCombiner: how to merge two combiners (across partitions)
  (acc1: Int, acc2: Int) => {
    acc1 + acc2
  }
)

// View a few results
combined.take(10).foreach(println)


// COMMAND ----------

// MAGIC %md
// MAGIC Metrics here are almost identical to aggregateByKey.
// MAGIC aggregateByKey is supposed to be slighly more efficient than combineByKey but this is not obvious on this use case/dataset.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Spark Configs to Tweak
// MAGIC Below are some confs to tweak to accentuante the effects of each implementation

// COMMAND ----------

// Controls when Spark uses map-side aggregation
//spark.conf.set("spark.sql.shuffle.partitions" ,"50") // Lower for small test data 

// Control adaptive execution based on runtime stats
//spark.conf.set("spark.sql.adaptive.enabled" ,true)
//spark.conf.set("spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled" , true)
//spark.conf.set("spark.sql.adaptive.skewJoin.enabled", true)

// Avoid merge sort for smakk groups (improves suffle perf)
//spark.conf.set("spark.shuffle.sort.bypassMergeThreshold", "200")

// Spill Tuning 
//spark.conf.set("spark.memory.fraction", "0.6") //  More memory execution