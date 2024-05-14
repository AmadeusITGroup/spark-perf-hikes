// Spark: 3.5.1
// Local: --driver-memory 1G --master 'local[2]' --packages io.delta:delta-spark_2.12:3.1.0 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog

// COMMAND ----------

/*

This snippet presents a use case of streaming over a delta table where we do merges that updates 1 record.
We show that, when using 'ignoreChanges' option in the streaming query, it is possible to get several "delta" versions 
of a given functional records in a given streaming micro-batch, thus having "functional" duplicates in the micro batch.

Cf. https://docs.databricks.com/en/structured-streaming/delta-lake.html

# Symptom

We are streaming on a delta table that has updates (or merges), and we have functional duplicates in the same micro-batch
(i.e., for a given functional key, we receive records corresponding to different delta versions in the same micro batch).

# Explanation

In this example, the problem observed is that in a given streaming micro-batch we see a given functional key (id)
in several different delta versions (identified by different values and timestamps).

This happens because a given streaming micro batch consumes the results of several merge operations on the table.

The following processing of such micro batch that may follow (e.g. a join of such micro batch with another table) 
may present issues (logical or in performance) due to these functional duplicates. 
A possible solution is to deduplicate the micro batch on the functional key (e.g. picking the latest version) 
before doing any further processing.

Note also that functional keys not receiving updates may still be present in streaming micro-batches, because they are
getting copied along in the new file (when Deletion Vectors are not used).

Note: remove the metastore_db before running the example
rm -rf metastore_db/ && spark-run-local spark3/delta-streaming-dupes.dedup.sc

# What to aim for concretely

When streaming on a delta table that can have updates using the option "ignoreChanges", in the foreachBatch 
of your streaming micro batches make sure to properly handle potential functional duplicates, to avoid
possible logical or performance issues (e.g. exploding joins).

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

spark.conf.set("spark.sql.adaptive.enabled", "false")

DeltaTable.create(spark).location(delta).
  addColumn("id", "INT").
  addColumn("timestamp", "TIMESTAMP").
  addColumn("value", "INT").
  execute()
// Uncomment the line below to enable Deletion Vectors (DV)
// spark.sql(s"ALTER TABLE delta.`$delta` SET TBLPROPERTIES (delta.enableDeletionVectors = true)") // enable DV

val data = (1 to 10000).map(i => (i, java.sql.Timestamp.valueOf("2024-01-01 00:00:00"), 0))
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

// base table
spark.sql(s"select * from delta.`$delta` order by id").show()

// test id
val testID = 1

// 3 merges on test id
(1 to 3).foreach(value => merge(testID, value))

// streaming query
spark
  .readStream
  .format("delta")
  .option("ignoreChanges", "true")
  .load(delta)
  .writeStream
  .foreachBatch((b: DataFrame, i: Long) => {
    println(s"# Batch: $i")
    println("NO DEDUP")
    b.orderBy("id").show(false)
    println("WITH DEDUP")
    dedup(b, Seq("id"), Seq(col("timestamp").desc_nulls_last)).orderBy("id").show(false)
  })
  .trigger(Trigger.ProcessingTime("15 seconds")).start

// more merges on test id
(4 to 8).foreach(value => merge(testID, value))

// Note in the output that
// In the first batch we always see the latest version of the delta table
// In the following batches, we may receive duplicates for the same id, corresponding to different delta versions for that id
