// Spark: 3.5.1
// Local: --executor-memory 1G --driver-memory 1G --executor-cores 1 --master local[2] --packages io.delta:delta-spark_2.12:3.1.0 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
// Databricks: ...

// COMMAND ----------

/*
This example shows how to address the read and write amplification problem in case of merges,
in a scenario where we are doing an SCD-type-2-like history consolidation.

History consolidation logic in a nutshell:
- your table has a key, a version and a flag stating if it is the last version of the key (schema: id, version, is_last)
- when a new version arrives for a key, you want to update the current last version flag to false
  and insert the new version with the flag set to true

If for the various keys the number of versions is high enough (i.e., last versions are a small percentage of the total),
this scenario can be greatly improved by using partitioning on is-last, and merge queries with filter on is-last flag.
This will enable partition pruning during the merge.

IMPORTANT: we are assuming that keys arrive in order. If this is not the case, out of order scenario must be
detected and handled properly (out of scope here).

# Symptom

You implemented the history consolidation logic described above and you noticed that the merge operation is reading
and writing most of the data all the time, even if the input batch contains only a few keys.

# Explanation

The logic described above, does not strictly need to read all the versions of a key but only the last one,
to set the is-last flag to false. Therefore we can partition the table T on the is-last flag, and use this
in our join and merge logic.

First scenario: naive approach, without partitioning, high read and write amplification.
Steps:
- join new keys with the table (T) to get all history for that key
- adjust the is last flags in the different versions
- merge the patched rows (P) into the table

Second scenario: optimized approach, with partitioning, low read and write amplification.
Steps:
- join new keys with the latest partition of the table (T) to get only the old last for each key
- adjust the is last flags in the different versions
- merge the patched rows (P) into the table, looking for matches only in the latest partition
  For each key, the merge will:
  - update the latest partition (dropping a row from it, and appending it to the other partition)
  - insert the new latest version of the key in the latest partition

Merge query:
```sql
merge into T using P
on P.id = T.id and P.version = T.version and T.is_last = true
when matched then update set *
when not matched then insert *
*/

// COMMAND ----------

import java.util.UUID
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, explode}
import org.apache.spark.sql.DataFrame
import io.delta.tables.DeltaTable

val input = "/tmp/amadeus-spark-lab/datasets/optd_por_public_filtered.csv"
val tmpPath = "/tmp/amadeus-spark-lab/sandbox/" + UUID.randomUUID()
val notPartitionedTable = tmpPath + "/notPartitioned"
val partitionedTable = tmpPath + "/partitioned"

val spark: SparkSession = SparkSession.active

// COMMAND ----------

def optimize(deltaDir: String): Unit = {
  spark.sparkContext.setJobDescription("Optimize delta table to have many files")
  spark.conf.set("spark.databricks.delta.optimize.maxFileSize", 50 * 1024L)
  DeltaTable.forPath(deltaDir).optimize().executeZOrderBy("id")
}

// Prepare table content with 10 versions for each key, only version 10 has is_last = true
val airports = spark.read.option("delimiter", "^").option("header", "true").csv(input)
val versioned = airports.withColumn("versions", lit((1 to 10).toArray)).withColumn("version", explode(col("versions"))).drop("versions")
val tableDf = versioned.withColumn("is_last", col("version") === 10)

// Delta table without partitions
spark.sparkContext.setJobDescription("Create delta table without partitions")
tableDf.write.mode("overwrite").format("delta").save(notPartitionedTable)
optimize(notPartitionedTable)

// Delta table partitioned by is-last
spark.sparkContext.setJobDescription("Create delta table with partitions")
tableDf.write.mode("overwrite").format("delta").partitionBy("is_last").save(partitionedTable)
optimize(partitionedTable)

//// COMMAND ----------

// Note the total number of files
DeltaTable.forPath(notPartitionedTable).detail.select("numFiles").show
DeltaTable.forPath(partitionedTable).detail.select("numFiles").show

//// COMMAND ----------

// prepare patch DF for the merge: create version 11 for a given key, set is-last to false in version 10
// id, 10, false
// id, 11, true
def buildDataframeToMerge(id: String): DataFrame = {
  val v10 = tableDf.where(col("id") === id and col("version") === 10)
  val v10updated = v10.withColumn("is_last", lit(false))
  val v11 = v10.withColumn("version", lit(11)).withColumn("is_last", lit(true))
  val df = v10updated union v11
  df
}

// prepare delta table with patch to merge
// id, 10, false
// id, 11, true
def buildDeltaTableToMerge(id: String): DataFrame = {
  val df = buildDataframeToMerge(id)
  val path = s"$tmpPath/batch_${id}_${UUID.randomUUID()}"
  df.write.mode("overwrite").format("delta").save(path)
  DeltaTable.forPath(path).toDF
}

def merge(df: DataFrame, deltaDir: String, condition: String): Unit = {
  DeltaTable
    .forPath(deltaDir).as("T")
    .merge(df.as("P"), condition)
    .whenMatched.updateAll.whenNotMatched.insertAll.execute()
}

def showStats(deltaDir: String): Unit = {
  DeltaTable.forPath(deltaDir).history.where(col("version") === 2).
    select("operationMetrics.numTargetFilesAdded", "operationMetrics.numTargetRowsInserted", "operationMetrics.numTargetRowsUpdated", "operationMetrics.numTargetFilesRemoved").
    show(false)
}

// First scenario: No partitions
spark.sparkContext.setJobDescription("Merge - no partitions")
merge(buildDeltaTableToMerge("AAI"), notPartitionedTable, "T.id == P.id and T.version = P.version")

// Second scenario: Partitions
spark.sparkContext.setJobDescription("Merge - partitions")
merge(buildDeltaTableToMerge("AAI"), partitionedTable, "T.id == P.id and T.version = P.version and T.is_last = true")
showStats(partitionedTable)

// Go to the Databricks Spark UI, look for the SQL query corresponding to the Merge,
// open the sub-queries and analyse those corresponding to the first join.
// In the Scan parquet node for the table T you will see that all the files are read in the first scenario,
// while only the files of the latest partition are read in the second scenario (only 1 partition read).

// Things to observe in the second scenario:
// - only the latest partition is read (in spark UI)
// - in the latest partition one file is deleted and one is appended (the new version)
// - in the historical partition one file is appended (the old version)

// In particular note this in the delta table history:
//"numTargetFilesAdded": "2"
//"numTargetRowsInserted": "1"
//"numTargetRowsUpdated": "1"
//"numTargetFilesRemoved": "1"
