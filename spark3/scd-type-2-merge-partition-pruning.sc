// Spark: 3.5.1
// Local: --executor-memory 1G --driver-memory 1G --executor-cores 1 --master local[2] --packages io.delta:delta-spark_2.12:3.1.0 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
// Databricks: ...

// COMMAND ----------

/*
This example shows how to address the read and write amplification problem in case of merges,
in a scenario where we are doing an SCD-type-2-like history consolidation.

In a nutshell:
- your table has a key, a version and a flag stating if it is the last version of the key
- when a new version arrives for a key, you want to update the last version flag to false
  and insert the new version with the flag set to true

If for the various keys the number of versions is high enough (i.e., last versions are a small percentage of the total),
this scenario can be greatly improved by using partition pruning and merge queries with filter on is last flag.

IMPORTANT: we are assuming that keys arrive in order. If this is not the case, out of order scenario must be
detected and handled properly (out of scope here).

# Symptom
You implemented the logic described above and you noticed that the merge operation is reading and writing most of the
data all the time, even if the input batch contains only a few keys.

# Explanation

The logic described above, does not strictly need to read all the versions of a key but only the last one,
to set the is-last flag to false.

Therefore we can partition the table T on the is-last flag, and use this in our join and merge logic.

Steps (naive approach, without partitioning):
- join new keys with the table to get all history for that key
- adjust the is last flags in the different versions
- merge the patched rows into the table

Steps (optimized approach, with partitioning):
- join new keys with the latest partition of the table to get only the old last for each key
- adjust the is last flags in the different versions
- merge the patched rows into the table, looking for matches only in the latest partition
  For each key, the merge will:
  - update the latest partition (dropping a row from it, and appending it to the other partition)
  - insert the new latest version of the key in the latest partition

Merge query:
```sql
merge into T using N
on N.key = T.key and T.isLast = true
when matched then update set *
when not matched then insert *
*/

// COMMAND ----------


// TODO all this

import java.util.UUID
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.DataFrame
import io.delta.tables.DeltaTable

val input = "/tmp/amadeus-spark-lab/datasets/optd_por_public_filtered.csv"
val tmpPath = "/tmp/amadeus-spark-lab/sandbox/" + UUID.randomUUID()
val deltaDir = tmpPath + "/input"

val spark: SparkSession = SparkSession.active

// COMMAND ----------

// Destination delta table
spark.sparkContext.setJobDescription("Create delta table")
val airports = spark.read.option("delimiter","^").option("header","true").csv(input)
airports.write.mode("overwrite").format("delta").save(deltaDir)

// z-order the delta table on the merge key
spark.sparkContext.setJobDescription("Z-order delta table")
spark.conf.set("spark.databricks.delta.optimize.maxFileSize", 50 * 1024L)
DeltaTable.forPath(deltaDir).optimize().executeZOrderBy("iata_code")

// COMMAND ----------

// Note the total number of files after z-ordering
DeltaTable.forPath(deltaDir).detail.select("numFiles").show

// COMMAND ----------

def getMaxMinStats(tablePath: String, colName: String, commit: Int): DataFrame = {
  // stats on parquet files added to the table
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.types._
  val statsSchema = new StructType()
    .add("numRecords", IntegerType, true)
    .add("minValues", MapType(StringType, StringType), true)
    .add("maxValues", MapType(StringType, StringType), true)
  val df = spark.read.json(s"$tablePath/_delta_log/*${commit}.json")
    .withColumn("commit_json_name", input_file_name())
    .withColumn("add_stats", from_json(col("add.stats"), statsSchema))
    .withColumn(s"add_stats_min_col_${colName}", col(s"add_stats.minValues.$colName"))
    .withColumn(s"add_stats_max_col_${colName}", col(s"add_stats.maxValues.$colName"))
    .withColumn("add_size", col("add.size"))
    .withColumn("add_path", col("add.path"))
    .where(col("add_path").isNotNull)
    .select("add_path", s"add_stats_min_col_${colName}", s"add_stats_max_col_${colName}")
    .orderBy(s"add_stats_min_col_${colName}", "add_path")
  df
}

// Note that only 1 files out of 9 contains the iata_code 'AAI'
spark.sparkContext.setJobDescription(s"Display max/min stats for files present in delta table after z-order")
getMaxMinStats(deltaDir, "iata_code", 1).show(false)

// COMMAND ----------

def buildDataframeToMerge(iataCode: String, newVal: String): DataFrame = {
  val c = airports.where(col("iata_code") === iataCode).drop("comment").withColumn("comment", lit(newVal)).collect()
  val df = spark.createDataFrame(spark.sparkContext.parallelize(c), airports.schema)
  df
}

def buildDeltaTableToMerge(iataCode: String, newVal: String): DataFrame = {
  val df = buildDataframeToMerge(iataCode, newVal)
  val path = s"$tmpPath/batch_${iataCode}_${newVal}_${UUID.randomUUID()}"
  df.write.mode("overwrite").format("delta").save(path)
  DeltaTable.forPath(path).toDF
}

def merge(df: DataFrame): Unit = {
  val keyCols = Seq("iata_code")
  DeltaTable
    .forPath(deltaDir).as("o")
    .merge(df.as("t"), keyCols.map(colName => s"t.$colName == o.$colName").mkString(" and "))
    .whenMatched.updateAll.whenNotMatched.insertAll.execute()
}

// First scenario: No DFP
spark.conf.set("spark.databricks.optimizer.dynamicFilePruning", "false")
spark.sparkContext.setJobDescription("Merge dataframe - NO DFP")
merge(buildDeltaTableToMerge("AAI", "no-dfp"))

// number of files after first merge
DeltaTable.forPath(deltaDir).detail.select("numFiles").show

// Be sure to have the conditions to trigger DFP: DFP enabled, broadcast join, table/files thresholds
spark.conf.set("spark.databricks.optimizer.dynamicFilePruning", "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 1000000000L)
spark.conf.set("spark.databricks.optimizer.deltaTableSizeThreshold", 1)
spark.conf.set("spark.databricks.optimizer.deltaTableFilesThreshold", 1)

// Second scenario: DFP
spark.sparkContext.setJobDescription("Merge dataframe - DFP")
merge(buildDeltaTableToMerge("BWU", "dfp"))

// Go to the Databricks Spark UI, look for the SQL query corresponding to the Merge,
// open the sub-queries and analyse those corresponding to the two joins (an inner and a left outer join).
// Consider the 'PhotonScan parquet' node corresponding to the delta table.

// FIXME: strange behavior, both with Photon and non-Photon cluster.
// In both cases (DFP and NO DFP) the first join reads the whole table, while the second join
// only reads the files containing the merge key.
// In the Details we don't see "dynamic" and in the plan we don't see any dynamic query created.

// TODO: try merge from Delta table to delta table.
