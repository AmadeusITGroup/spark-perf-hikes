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
*/

// COMMAND ----------

import java.util.UUID
import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession

val input = "/tmp/perf-hikes/datasets/optd_por_public_filtered.csv"
val tmpPath = "/tmp/perf-hikes/sandbox/" + UUID.randomUUID()
val inputDir = tmpPath + "/input"

val spark: SparkSession = SparkSession.active

spark.conf.set("spark.sql.adaptive.enabled", false)

spark.sparkContext.setJobDescription("Read input CSV")
val airports = spark.read.option("delimiter","^").option("header","true").csv(input)

spark.sparkContext.setJobDescription("Format input CSV into delta (multiple files)")
airports.repartition(5).write.format("delta").save(inputDir)

// COMMAND ----------

import org.apache.spark.sql.DataFrame

def showMaxMinStats(tablePath: String, colName: String, commit: Int): DataFrame = {
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
  spark.sparkContext.setJobDescription(s"Display max/min stats for files present in delta table (commit ${commit})")
  df
}

def selectQuery(tablePath: String, tag: String): Unit = {
  spark.sparkContext.setJobDescription(s"Select query = $tag")
  val df = DeltaTable.forPath(tablePath).toDF
  val c = df.where(s"country_code = 'IT'").count()
  println(c)
}

// COMMAND ----------

// show that files have data that is not well organized (ranges max/min values overlap for different files)
display(showMaxMinStats(inputDir, "country_code", 0))

// COMMAND ----------

selectQuery(inputDir, "NO LIQUID CLUSTERING")

// COMMAND ----------

// Enable liquid clustering on the existing table, then cluster the data with OPTIMIZE
spark.sparkContext.setJobDescription("Enable liquid clustering")
spark.sql(s"ALTER TABLE delta.`$inputDir` CLUSTER BY (country_code)")
spark.sparkContext.setJobDescription("OPTIMIZE (liquid clustering)")
spark.conf.set("spark.databricks.delta.optimize.maxFileSize", 100 * 1024L)
spark.sql(s"OPTIMIZE delta.`$inputDir`")


// COMMAND ----------

spark.sparkContext.setJobDescription("Describe table")
spark.sql(s"DESCRIBE DETAIL delta.`$inputDir`").select("numFiles", "clusteringColumns").show(false)
spark.sql(s"DESCRIBE HISTORY delta.`$inputDir`").show(false)

// COMMAND ----------

// show that files have data that is well organized now (commit 1 is the CLUSTER BY metadata change, commit 2 is the OPTIMIZE)
display(showMaxMinStats(inputDir, "country_code", 2))

// COMMAND ----------


selectQuery(inputDir, "LIQUID CLUSTERING")

// Check the Spark UI to see the amount of files read for the same select query before and after liquid clustering.
