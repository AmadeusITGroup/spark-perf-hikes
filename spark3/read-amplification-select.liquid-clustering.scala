// Databricks notebook source
// Spark: 3.5.1
// Local: --driver-memory 1G --master 'local[2]' --packages io.delta:delta-spark_2.12:3.3.1 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
// Databricks: ...

// COMMAND ----------

/*
This example shows how to address the read amplification problem in case of select queries on a delta table,
using a data skipping friendly data layout obtained enabling liquid clustering on the table.

References:
- https://docs.delta.io/latest/delta-clustering.html

# Symptom
You are running a select query on a delta table and you are reading way more data than actually needed,
since your result-set is really small.

# Explanation
In this example we create a delta table composed by 5 parquet files, and we run a select query, filtering
on the 'country_code' column, before and after having enabled liquid clustering on such column.

Before clustering the table, we observe that each parquet file contains values of the 'country_code'
that span over the whole domain (AE to ZW).
Therefore, the select query needs to read all the table files. This is visible in the 'number of files read' metric,
in the Scan Parquet node of the Spark Plan corresponding to such query (Spark UI, SQL/Dataframe tab).

We then enable liquid clustering (ALTER TABLE ... CLUSTER BY (country_code)) and run OPTIMIZE, which rewrites
the files so that country codes are colocated. The min/max ranges of the files no longer overlap (except at boundaries), and data skipping
lets the select query read only 1 file. The table still has 5 files: the gain comes from the layout, not compaction.
In the query details we observe that a filter push down took place:
PushedFilters: [IsNotNull(country_code), EqualTo(country_code,IT)]

Compared to z-order, which is a one-off rewrite whose columns must be repeated on every OPTIMIZE, liquid clustering
stores the clustering keys in the table metadata: a plain OPTIMIZE keeps the table clustered, and keys can be changed
without rewriting the table. Compared to partitioning, it avoids a rigid directory layout that suffers with too high
or too low cardinality columns. Liquid clustering is not compatible with partitioning nor ZORDER.

# What to aim for concretely
In the execution plan of the query, there should be pushed filters on the clustering column.
These filters should reduce the 'number of files read' metric in the scan parquet node for the query (5 -> 1).

============
TODO CHECK THIS SECTION
============
*/

// COMMAND ----------

// MAGIC %md
// MAGIC # Setup

// COMMAND ----------

import java.util.UUID
import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession

val input = "/tmp/perf-hikes/datasets/optd_por_public_filtered.csv"
val tmpPath = "/tmp/perf-hikes/sandbox/" + UUID.randomUUID()
val inputDirWithoutLc = tmpPath + "/input-without-lc"
val inputDirWithLc = tmpPath + "/input-with-lc"
val spark: SparkSession = SparkSession.active

spark.conf.set("spark.sql.adaptive.enabled", false)

spark.sparkContext.setJobDescription("Read input CSV")
val airports = spark.read.option("delimiter","^").option("header","true").csv(input)

val partitions = 5
spark.sparkContext.setJobDescription("Write delta table 1")
airports.repartition(partitions).write.format("delta").save(inputDirWithoutLc)
spark.sparkContext.setJobDescription("Write delta table 2")
airports.repartition(partitions).write.format("delta").save(inputDirWithLc)

// COMMAND ----------

// Enable liquid clustering on the corresponding table, then cluster the data with OPTIMIZE
spark.sparkContext.setJobDescription("Enable liquid clustering on one table")
spark.sql(s"ALTER TABLE delta.`$inputDirWithLc` CLUSTER BY (iata_code, country_code)")
spark.sparkContext.setJobDescription("OPTIMIZE (liquid clustering)")
spark.conf.set("spark.databricks.delta.optimize.maxFileSize", 100 * 1024L)
spark.sql(s"OPTIMIZE delta.`$inputDirWithLc`")

// COMMAND ----------

def selectQuery(tablePath: String, tag: String): Unit = {
  spark.sparkContext.setJobDescription(s"Select query = $tag")
  val df = DeltaTable.forPath(tablePath).toDF
  df.where(s"country_code = 'IT'").where(s"iata_code like 'F%'").select("country_code", "iata_code").show(false)
}

// COMMAND ----------

// Max/min stats of the parquet files added by each commit in [fromCommit, toCommit], unioned in one dataframe
import org.apache.spark.sql.DataFrame
def showMaxMinStats(tablePath: String, colNames: Seq[String], fromCommit: Int, toCommit: Int): DataFrame = {
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.types._
  require(colNames.nonEmpty, "At least one column name is required")
  val statsSchema = new StructType()
    .add("numRecords", IntegerType, true)
    .add("minValues", MapType(StringType, StringType), true)
    .add("maxValues", MapType(StringType, StringType), true)

  val minCols = colNames.map(c => s"add_stats_min_col_$c")
  val statsCols = colNames.flatMap { c =>
    Seq(
      col("add_stats.minValues").getItem(c).as(s"add_stats_min_col_$c"),
      col("add_stats.maxValues").getItem(c).as(s"add_stats_max_col_$c")
    )
  }

  val perCommit = (fromCommit to toCommit)
    .map(commit => commit -> spark.read.json(f"$tablePath/_delta_log/$commit%020d.json"))
    .collect { case (commit, json) if json.columns.contains("add") => // only commits with added files (e.g. CLUSTER BY may lack)
      json
        .withColumn("add_stats", from_json(col("add.stats"), statsSchema))
        .where(col("add.path").isNotNull)
        .select(
          Seq(
            lit(commit).as("commit"),
            col("add.path").as("add_path")
          ) ++ statsCols ++ Seq(            
            to_json(col("add.tags")).as("add_tags")
          ): _*
        )
    }

  require(perCommit.nonEmpty, s"No files added between commits $fromCommit and $toCommit")
  spark.sparkContext.setJobDescription(s"Display max/min stats for files added to delta table (commits ${fromCommit} to ${toCommit})")
  perCommit.reduce(_ unionByName _).orderBy((Seq("commit") ++ minCols :+ "add_path").map(col): _*)
}

// COMMAND ----------

// MAGIC %md 
// MAGIC ## No liquid clustering

// COMMAND ----------

// show that files have data that is not well organized (ranges max/min values overlap for different files)
display(showMaxMinStats(inputDirWithoutLc, Seq("country_code", "iata_code"), 0, 0))

// COMMAND ----------

selectQuery(inputDirWithoutLc, "NO LIQUID CLUSTERING")

// COMMAND ----------

// MAGIC %md
// MAGIC ## With liquid clustering

// COMMAND ----------

spark.sparkContext.setJobDescription("DESCRIBE HISTORY")
display(spark.sql(s"DESCRIBE HISTORY delta.`$inputDirWithLc`"))

// COMMAND ----------


display(showMaxMinStats(inputDirWithLc, Seq("country_code", "iata_code"), 0, 5))

// COMMAND ----------


selectQuery(inputDirWithLc, "LIQUID CLUSTERING")

// Check the Spark UI to see the amount of files read for the same select query before and after liquid clustering.
