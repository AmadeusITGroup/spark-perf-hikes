// Databricks notebook source
// MAGIC %md
// MAGIC // Spark: 3.5.0
// MAGIC // Local: --master 'local[2]' --driver-memory 1G
// MAGIC // Databricks: DBR 15.4 LTS. Driver and 2 workers same VM: Standard F4, 8GB , 4 cores
// MAGIC
// MAGIC
// MAGIC Compare shuffle metrics and performance of flatMapGroups along with proposed alternatives
// MAGIC
// MAGIC # Symptom
// MAGIC Too much data is being shuffled and potentially spilled. Long execution times potential OOM errors and crashes.
// MAGIC
// MAGIC # Explanation
// MAGIC flatMapGroups is a method of the typed KeyGroupedDataset API in Spark.
// MAGIC The goal of this notebook is too educate and guide users about the risk of massive data shuffled and potentially spilled associated using flatMapGroups in order to use it properly or avoid using it altogether. 
// MAGIC After a brief description and explantion of how it works, a bad usage example will be given, a proper usage example and alternatives will be explored in the dataset or dataframe api. Latest associated documentation: https://spark.apache.org/docs/3.5.2/api/java/org/apache/spark/sql/KeyValueGroupedDataset.html#flatMapGroups-scala.Function2-org.apache.spark.sql.Encoder- 
// MAGIC
// MAGIC # What to aim for concretely
// MAGIC Don't materialize the iterator in flatMapGroups in order to avoid spill and potentially OOM. Use other alternatives like the typed Aggregator in dataset API or dataframe API functions.
// MAGIC Every example is triggered by an action such as show, take or count. We also print the execution plan but its better visualised in Spark UI. For each action you investigate the metrics of the 2 stages in the 2 jobs that are created. 

// COMMAND ----------

// MAGIC %md
// MAGIC flatMapGroups applies the given function to each group of data. For each unique group, the function will be passed the group key and an iterator that contains all of the elements in the group. The function can return an iterator containing elements of an arbitrary type which will be returned as a new Dataset.
// MAGIC For simplicity sake we are going to consider a simple summing function where we some all values of each group. This is a commutative and associative operation.
// MAGIC It'ts intresting to compare how the different implementations of flatMapGroups and the proposed alternatives fare with a normal as well skewed dataset. 
// MAGIC The method below allows to tweak the generated input data. Depending if this is run locally or on a cluster, that values should be set accordingly to test the configuration limits

// COMMAND ----------

import org.apache.spark.sql.{Dataset, SparkSession}
import scala.util.Random

case class Event(userId:String, value:Int)

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
// MAGIC Partitioning always plays a big role. Usually we want each core to work on 3 to 4 partitions. Here we use a default of 4 per core that corresponds to the factor argument.
// MAGIC Depending whether you run this notebook locally or on databricks you should set values accordingly. Spark confs "spark.executor.instances
// MAGIC and "spark.executor.cores" or not valid for databricks env.
// MAGIC "spark.databricks.clusterUsageTags.clusterGeneration" is an integer but does not correspond to cores per executor. 
// MAGIC

// COMMAND ----------

def optimalPartitions(factor: Int = 4):Int = {
  val sparkConf = spark.sparkContext.getConf
  val numExecutors = sparkConf.get("spark.databricks.clusterUsageTags.clusterWorkers").toInt // clusterTargetWorkers
  //val numExecutors = 1 // for local
  val coresPerExecutor = 4 //  2 for local
  val totalCores = numExecutors * coresPerExecutor
  totalCores * factor
}

val numPartitions = optimalPartitions()

// COMMAND ----------

// MAGIC %md
// MAGIC The values chosen below makes a very skewed dataset with just 1OO groups so we can obtain big partitions.
// MAGIC More events, fewer groups/partitions, and more skew will be generally harder to handle.
// MAGIC AQE does alleviate the skewed partition that could accentuate certain metrics we are trying to highlight. 
// MAGIC Depending on the environment ( local or on databricks) differents set of values should be used.
// MAGIC For each environment we propose 2 set of values: 
// MAGIC - first set of values will ensure execution of all cells highlighting the differences in resources utilization and performance of each solution
// MAGIC - second set of values will make the cell of bad implementation fail and let the rest succeed 
// MAGIC
// MAGIC Databricks environment:
// MAGIC - success : generateSyntheticData(spark,numEvents = 4000000000L,numGroups = 100, skewed = true, skewRatio = 0.95 )
// MAGIC - fail: generateSyntheticData(spark,numEvents = 4000000000L,numGroups = 10, skewed = true, skewRatio = 0.95 )
// MAGIC
// MAGIC Local environment:
// MAGIC - success : generateSyntheticData(spark,numEvents = 20000000L,numGroups = 10, skewed = true, skewRatio = 0.35 )
// MAGIC - fail: generateSyntheticData(spark,numEvents = 20000000L,numGroups = 10, skewed = true, skewRatio = 0.45 )
// MAGIC
// MAGIC
// MAGIC The values set by default are meant for databricks
// MAGIC Feel free to tweak any setting here, but the explanation will stick to these defaults. 
// MAGIC AQE is enabled by default. By disabling AQE you will have to lower the dataset/partition size to achieve similar results

// COMMAND ----------

val ds = generateSyntheticData(
  spark,
  numEvents = 4000000000L,
  numGroups = 10,
  skewed = true,
  skewRatio = 0.95
  )

// COMMAND ----------

ds.repartition(numPartitions)
ds.persist
ds.show

// COMMAND ----------

// MAGIC %md
// MAGIC ### Bad usage of the method. 
// MAGIC It materializes all groups to memory due to .toList
// MAGIC shuffles all data (no combiner)
// MAGIC Causes skew if one key is huge then it also leads to spills
// MAGIC

// COMMAND ----------

import spark.implicits._

val grouped = ds.groupByKey(_.userId)

val badUsageResult = grouped.flatMapGroups { case(userId, values) =>
  // BAD: values is an Iterator, but this collects everything into memory
  val sum = values.toList.map(_.value).sum
  Iterator((userId, sum))
}

badUsageResult.explain(true)

// COMMAND ----------

badUsageResult.show

// COMMAND ----------

// MAGIC %md
// MAGIC Look at job 2 and job 3 latest stages.
// MAGIC Notice the amount of shuffle time and the read amout, the total as well as the one per tasks such as  amount for data spilled to memory and disk and the GC time. This cell won’t finish if you work on bigger groups. Will lead to OOM errors

// COMMAND ----------

// MAGIC %md
// MAGIC ### Good enough usage of the method
// MAGIC Suffle is still unavoidable ( we need all items of a group together)
// MAGIC We don't toList or collect -- we stream over Iterator, minimizing memory pressure.
// MAGIC No big spikes, better for big partitions and skewed groups. Even though spill does occur, its not as severe as previously and we don't get OOM errors.
// MAGIC This is still not ideal since a lot of shuffle occurs.

// COMMAND ----------

// MAGIC %md
// MAGIC Metrics here, beyond halved execution time, are almost identical but there is a catch.
// MAGIC For consistency and clarity sake we'll also do the sum.

// COMMAND ----------

val goodEnoughResult  = grouped.flatMapGroups { case (userId, iter) =>
  // stream through iterator and compute sum without materializing the group
  var sum = 0
  while( iter.hasNext) {
    sum += iter.next().value
  }
  Iterator((userId, sum))
}

goodEnoughResult.explain(true)

// COMMAND ----------

goodEnoughResult.show

// COMMAND ----------

// MAGIC %md
// MAGIC By looking at same metrics as before we can see that the amount of data shuffled written or read does not differ but that we have a much smaller GC time in the second job which ensures much faster halved execution time than the bad example. toList does create many ephemeral objects it then needs to clean up.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Better; Using a Typed Aggregator.
// MAGIC This abstract class  [Aggregator[-IN, BUF, OUT]](https://spark.apache.org/docs/3.5.0/api/scala/org/apache/spark/sql/expressions/Aggregator.html) we extend also operates on  [Class KeyValueGroupedDataset<K,V>](https://spark.apache.org/docs/3.5.2/api/java/org/apache/spark/sql/KeyValueGroupedDataset.html#flatMapGroups-scala.Function2-org.apache.spark.sql.Encoder-)
// MAGIC Spark inserts a partial reduce stage (combiner) before shuffle. This will ensure
// MAGIC - Lower Memory usage.
// MAGIC - Shuffle volume reduced.
// MAGIC - Faster execution.

// COMMAND ----------

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.Encoders

// This object defines a strongly typed custom aggregator using Spark's Dataset API
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

val datasetAggregatorResult = ds.groupByKey(_.userId).agg(SumAgg.toColumn)

datasetAggregatorResult.explain(true)

// COMMAND ----------

datasetAggregatorResult.show

// COMMAND ----------

// MAGIC %md
// MAGIC Here we see a drastic decrease in amount of data shuffled compared to all previous implementations since most the work is done pre shuffle (mapper/combiner). No spill occurs.
// MAGIC In the first job we can observe we have a peak execution memory which was previously 0 that reflects the work done pre shuffle (by the combiner). Since only the result of each partition is shuffled, the amount is extremely small. No GC occurs at all on 2nd job but seems to slightly have increased in the 1rst job. We have further improved also overall performance even more.

// COMMAND ----------

// MAGIC %md
// MAGIC ### The best: equivalent using DataFrame API
// MAGIC Under the hood, spark is smart enough to do most of the aggregation via a combiner on map-side (pre shuffle)
// MAGIC The function you might be interested could be already part of spark dataframe built in aggregation functions. In that case use it directly

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
// MAGIC The dataframe equivalent has same total shuffle metrics than previously but like the typed aggregator most of the work occurs on the mapper side (1 job). Notice we can spot also some peak execution memory. Overall execution is even faster, the fastest. GC time is further reduced on the mapper side. Here we can witness the whole power of spark dataframe api optimization under the hood like tungsten.

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

// COMMAND ----------

// MAGIC %md
// MAGIC ## Conclusion
// MAGIC We can clearly see the various gains in performance of each example compared to a bad usage.  Depending on the associated logic, code base and time the various alternatives might not be viable. 
// MAGIC If you can't avoid using flatMapGroups make sure you don't materialize the whole iterator. If you need to stick with dataset api then try to use the typed Aggregator. Finally, when possible, try to use built in functions of the Dataframe api.
