// Spark: 3.5.1
// Local: --driver-memory 1G --master 'local[2]' --packages io.delta:delta-spark_2.12:3.1.0 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
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

The history consolidation logic described above, does not strictly need to read all the versions of a key
but only the last one, to set the is-last flag to false. Therefore we can partition the table T on the is-last flag,
and use this in our merge logic.

Scenario:
- a new version for an existing key arrives (id = AAI)
- we compute a patch DF P where the new version is set to is-last = true, and the old last version is set to is-last = false
- we merge the patch DF into the table T

Naive approach: without partitioning, high read and write amplification.
Steps:
- compute the patch P
- merge the patched rows P into the table T

Optimized approach: with partitioning, low read and write amplification.
Steps:
- compute the patch P
- merge the patched rows P into the table T, looking for matches only in the latest partition
  For each key, the merge will:
  - update the latest partition (dropping a row from it, and appending it to the other partition)
  - insert the new latest version of the key in the latest partition

  Merge query:
  ```sql
  merge into T using P
  on P.id = T.id and P.version = T.version and T.is_last = true
  when matched then update set *
  when not matched then insert *
  ```
*/

// COMMAND ----------

import java.util.UUID
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, explode, desc, row_number}
import org.apache.spark.sql.DataFrame
import io.delta.tables.DeltaTable
import org.apache.spark.sql.expressions.Window

val input = "/tmp/perf-hikes/datasets/optd_por_public_filtered.csv"
val tmpPath = "/tmp/perf-hikes/sandbox/" + UUID.randomUUID()
val notPartitionedTable = tmpPath + "/notPartitioned"
val partitionedTable = tmpPath + "/partitioned"
val numVersions = 100

val spark: SparkSession = SparkSession.active

// COMMAND ----------

def optimize(deltaDir: String): Unit = {
  spark.sparkContext.setJobDescription("Optimize delta table to have many files")
  spark.conf.set("spark.databricks.delta.optimize.maxFileSize", 50 * 1024L)
  DeltaTable.forPath(deltaDir).optimize().executeZOrderBy("id")
}

// Prepare table content with 'numVersions' versions for each key, only the last version has is_last = true
val airports = spark.read.option("delimiter", "^").option("header", "true").csv(input)
val versioned = airports.withColumn("versions", lit((1 to numVersions).toArray)).withColumn("version", explode(col("versions"))).drop("versions")
val tableDf = versioned.withColumn("is_last", col("version") === numVersions)

// Delta table without partitions
spark.sparkContext.setJobDescription("Create delta table without partitions")
tableDf.write.mode("overwrite").format("delta").save(notPartitionedTable)
optimize(notPartitionedTable)

// Delta table partitioned by is-last
spark.sparkContext.setJobDescription("Create delta table with partitions")
tableDf.write.mode("overwrite").format("delta").partitionBy("is_last").save(partitionedTable)
// Uncomment the line below to enable Deletion Vectors (DV)
//spark.sql(s"ALTER TABLE delta.`$partitionedTable` SET TBLPROPERTIES (delta.enableDeletionVectors = true)") // enable DV
optimize(partitionedTable)

//// COMMAND ----------

// Note the total number of files
DeltaTable.forPath(notPartitionedTable).detail.select("numFiles").show
DeltaTable.forPath(partitionedTable).detail.select("numFiles").show

//// COMMAND ----------

def newVersion(id: String, v: Int): DataFrame = {
  tableDf.filter(s"id = '$id' and version = $numVersions").withColumn("version", lit(v)).withColumn("is_last", lit(true))
}

def toDelta(df: DataFrame): DataFrame = {
  val path = s"$tmpPath/patch_${UUID.randomUUID()}"
  df.write.mode("overwrite").format("delta").save(path)
  DeltaTable.forPath(path).toDF
}

// Prepare patch P for the merge.
// Create a new version for a given key, setting is-last = true; set is-last = false in the previous last version.
// id, 101, true
// id, 100, false
def buildDataframeToMergeSCD(id: String, deltaDir: String, condition: String = "1 = 1"): DataFrame = {
  val keyVersions = DeltaTable.forPath(deltaDir).toDF.filter(s"id = '$id'").filter(condition)
  val union = keyVersions union newVersion(id, numVersions + 1)
  val groupByKeyOrderByVersion = Window.partitionBy("id").orderBy(desc("version"))
  val df = union.withColumn("r", row_number().over(groupByKeyOrderByVersion)).withColumn("is_last", col("r") === 1).drop("r")
  toDelta(df)
}

def merge(df: DataFrame, deltaDir: String, condition: String): Unit = {
  DeltaTable.forPath(deltaDir).as("T").merge(df.as("P"), condition).whenMatched.updateAll.whenNotMatched.insertAll.execute()
}

def showLatestStats(deltaDir: String): Unit = {
  val metrics = Seq("numTargetFilesAdded", "numTargetRowsInserted", "numTargetRowsUpdated", "numTargetFilesRemoved", "numTargetDeletionVectorsAdded")
  val cols = metrics.map(m => col(s"operationMetrics.$m"))
  val latestVersion = DeltaTable.forPath(deltaDir).history.selectExpr("max(version)").collect.head.getLong(0)
  DeltaTable.forPath(deltaDir).history.where(col("version") === latestVersion).select(cols: _*).show(false)
}

// First scenario: No partitions
spark.sparkContext.setJobDescription("Build patch DF - no partitions")
val patchNoPart = buildDataframeToMergeSCD("AAI", notPartitionedTable)
spark.sparkContext.setJobDescription("Merge - no partitions")
merge(patchNoPart, notPartitionedTable, "T.id == P.id and T.version = P.version")
showLatestStats(notPartitionedTable)

// Second scenario: Partitions
spark.sparkContext.setJobDescription("Build patch DF - partitions")
val patchPart = buildDataframeToMergeSCD("AAI", partitionedTable, "is_last = true")
spark.sparkContext.setJobDescription("Merge - partitions")
merge(patchPart, partitionedTable, "T.id == P.id and T.version = P.version and T.is_last = true")
showLatestStats(partitionedTable)

/**
Go to the Spark UI, look for the SQL query corresponding to the Merge,
open the sub-queries and analyse those corresponding to the first join (inner join).
In the Scan parquet node for the table T you will see that all the files are read in the first scenario,
while only the files of the latest partition are read in the second scenario (only 1 partition read).

Things to observe in the second scenario:
- only the latest partition is read (in spark UI)
- in the latest partition one file is deleted (old latest version) and one is appended (new latest version)
- in the historical partition one file is appended (the old latest version)

In particular note this in the delta table history:
"numTargetFilesAdded": "2"
"numTargetRowsInserted": "1"
"numTargetRowsUpdated": "1"
"numTargetFilesRemoved": "1"

If DV enabled:
"numTargetDeletionVectorsAdded": "1"
"numTargetFilesRemoved": "0"
*/
