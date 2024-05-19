// Spark: 3.5.1
// Local: --driver-memory 1G --master 'local[2]' --packages io.delta:delta-spark_2.12:3.1.0 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog

// COMMAND ----------

/*

This snippet presents a use case of streaming over a delta table where we do merges that updates 1 record.

We consider two possible streaming queries:
- one with "readChangeFeed"
- one with "ignoreChanges"

We show that, in both cases, it is possible to get several "delta" versions of a given functional record in a given
streaming micro-batch, thus having "functional" duplicates in the micro batch.
Furthermore, if we use 'ignoreChanges' option in the streaming query, we can see multiple time records that have not
changed.

References:
- https://docs.databricks.com/en/structured-streaming/delta-lake.html
- https://docs.databricks.com/en/delta/delta-change-data-feed.html

# Symptom

We are streaming on a delta table that has updates (or merges), and we have functional duplicates in the same micro-batch
(i.e., for a given functional key, we receive records corresponding to different delta versions in the same micro batch),
or we see in multiple streaming batches records that have not changed.

# Explanation

Both streaming queries use Trigger.AvailableNow.

In both streaming queries, the very first run sees only the latest version of the delta table.
The following run, sees all the intermediate versions generated on the delta table between the two streaming runs.

In case of "ignoreChanges" we may also see again records that have not changed, thus increasing the amount of data
that needs to be processed. This happens because because such records are getting copied along in the new file generated
by the merge (when Deletion Vectors are not used).

Due to the presence of duplicates, the processing of such micro batches (e.g. the join of the micro batch with another
table) may present issues, logical or in performance.

A possible solution is to deduplicate the micro batch on the functional key (e.g. picking the latest version) before
doing any further processing.

# What to aim for concretely

When streaming on a delta table that can have updates, if you need to process these updates, prefer "readChangeFeed"
over "ignoreChanges" in your streaming query, to avoid seeing several times records that have not changed, thus
processing less records.
In any case, in the foreachBatch of your streaming query make sure to properly handle potential functional duplicates
(e.g. via deduplication), to avoid possible logical or performance issues (e.g. exploding joins).

*/

// COMMAND ----------

import java.util.UUID
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.Column

val spark: SparkSession = SparkSession.active
import spark.implicits._

val tmpPath = "/tmp/perf-hikes/sandbox/" + UUID.randomUUID()
val delta = tmpPath + "/delta"
val checkpoints = tmpPath + "/checkpoints"

spark.conf.set("spark.sql.adaptive.enabled", "false")

DeltaTable.create(spark).location(delta).
  property("delta.enableChangeDataFeed", "true").
  addColumn("id", "INT").
  addColumn("timestamp", "TIMESTAMP").
  addColumn("value", "INT").
  execute()
// Uncomment the line below to enable Deletion Vectors (DV)
// spark.sql(s"ALTER TABLE delta.`$delta` SET TBLPROPERTIES (delta.enableDeletionVectors = true)") // enable DV

val data = (1 to 100000).map(i => (i, java.sql.Timestamp.valueOf("2024-01-01 00:00:00"), 0))
val df = data.toDF("id", "timestamp", "value")
df.write.format("delta").mode("append").saveAsTable(s"delta.`${delta}`")

def merge(id: Int, value: Int): Unit = {
  val input = Seq((id, java.sql.Timestamp.valueOf(s"2024-01-01 00:00:0$value"), value)).toDF("id", "timestamp", "value")
  DeltaTable.forPath(delta).as("T").merge(input.as("P"), "T.id == P.id").whenMatched.updateAll.whenNotMatched.insertAll.execute()
}

def dedup(batch: DataFrame, dedupKey: Seq[String], discriminatingCols: Seq[Column]): DataFrame = {
  val windowSpec = Window.partitionBy(dedupKey.map(col): _*).orderBy(discriminatingCols: _*)
  batch
    .withColumn("row_number", row_number().over(windowSpec))
    .filter("row_number = 1")
    .drop("row_number")
}

// streaming query
def runStream(option: String): Unit = {
  spark
    .readStream
    .format("delta")
    .option(option, "true")
    .load(delta)
    .writeStream
    .option("checkpointLocation", s"${checkpoints}_${option}")
    .foreachBatch((b: DataFrame, i: Long) => {
      println(s"# Batch: $i. Option: $option")

      // batch received
      println("NO DEDUP, rows to process: " + b.count())
      b.orderBy("id").show(false)

      // batch after deduplication
      val d = dedup(b, Seq("id"), Seq(col("timestamp").desc_nulls_last))
      println("WITH DEDUP, rows to process: " + d.count())
      d.orderBy("id").show(false)
    })
    .trigger(Trigger.AvailableNow).start.awaitTermination()
}

// base table
spark.sql(s"select * from delta.`$delta` order by id").show()

// test id
val testID = 1

// some merges on test id
(1 to 2).foreach(value => merge(testID, value))

runStream("readChangeFeed")
runStream("ignoreChanges")

// more merges on test id
(2 to 8).foreach(value => merge(testID, value))

runStream("readChangeFeed")
runStream("ignoreChanges")

/*
 In the output of the second batch of the streaming queries, note the printed "rows to process" in each case.

 # Batch: 1. Option: readChangeFeed
 NO DEDUP, rows to process: 14
 WITH DEDUP, rows to process: 1

 # Batch: 1. Option: ignoreChanges
 NO DEDUP, rows to process: 1806
 WITH DEDUP, rows to process: 258

 With CDF we systematically have less records to process.

 */
