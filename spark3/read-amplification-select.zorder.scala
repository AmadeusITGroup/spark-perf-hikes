// Databricks notebook source
// Spark: 3.5.1
// Local: --driver-memory 1G --master 'local[2]' --packages io.delta:delta-spark_2.12:3.1.0 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
// Databricks: ...

// COMMAND ----------

/*
This example shows how to address the read amplification problem in case of select queries on a delta table, 
using a data skipping friendly data layout obtained z-ordering the table.

References:
- https://docs.databricks.com/en/delta/data-skipping.html#delta-zorder

# Symptom

You are running a select query on a delta table and you are reading way more data than actually needed,
since your result-set is really small.

# Explanation

In this example we create a delta table composed of 5 parquet files, and we run a select query, filtering 
on the 'country_code' column, before and after having z-ordered the table on such column.

Before z-ordering the table, we observe that each parquet file contains values of the 'country_code' 
that span over the whole domain (AE to ZW).
Therefore, the select query needs to read all the table files. This is visible in the 'number of files read' metric,
in the Scan Parquet node of the Spark Plan corresponding to such query (Spark UI, SQL/Dataframe tab).

After z-ordering the table, we observe that country codes are well sorted and colocated in the parquet files:
each file covers a narrow, non-overlapping range of 'country_code' values.
The select query now only reads 1 file, thanks to Delta data skipping.

The filter 'country_code = IT' is used at different moments:
1. Driver, query planning (Delta data skipping): the filter is checked against the per-file min/max stats stored in
   the delta log (_delta_log), and files whose range cannot match are skipped. This is what reduces the number of
   files read, and it only works if file ranges are narrow, which is what z-order provides.
2. Executors, reading the files (Parquet filter push down): the filter is passed to the Parquet reader, visible as
   PushedFilters: [IsNotNull(country_code), EqualTo(country_code,IT)]
   It skips row groups (using the stats in the Parquet file footers) inside the files selected in step 1.
3. Executors, after the scan: the Filter node re-checks every row, since steps 1 and 2 are best-effort.

Note that PushedFilters are present before z-ordering too: they are not what reduces the number of files read.

# What to aim for concretely

In the scan node of the query (Spark UI, SQL/Dataframe tab), the 'number of files read' metric should be much lower
than the total number of files in the table (on Databricks, see also 'number of files pruned').
For this, the filter must be on a column with stats (by default the first 32 columns, see
delta.dataSkippingNumIndexedCols / delta.dataSkippingStatsColumns), and the files must have narrow ranges on it.

*/

// COMMAND ----------

// DBTITLE 1,Setup
import java.util.UUID
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, SparkSession}

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

// Max/min stats of the parquet files added or removed by each commit in [fromCommit, toCommit], unioned in one dataframe.
// Column 'action' is 'add' or 'remove'. Remove actions often carry no stats (nulls): match them by path with earlier adds.
def showMaxMinStats(tablePath: String, colNames: Seq[String], fromCommit: Int, toCommit: Int): DataFrame = {
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.types._
  require(colNames.nonEmpty, "At least one column name is required")
  val statsSchema = new StructType()
    .add("numRecords", LongType, true)
    .add("minValues", MapType(StringType, StringType), true)
    .add("maxValues", MapType(StringType, StringType), true)
  val minCols = colNames.map(c => s"min_$c")
  val statsCols = colNames.flatMap { c =>
    Seq(
      col("stats.minValues").getItem(c).as(s"min_$c"),
      col("stats.maxValues").getItem(c).as(s"max_$c")
    )
  }
  // One dataframe per (commit, action), only if the commit json has that action (e.g. a pure append has no 'remove')
  def fileActions(json: DataFrame, commit: Int, action: String): Option[DataFrame] =
    if (!json.columns.contains(action)) None
    else {
      val fields = json.schema(action).dataType.asInstanceOf[StructType]
      def has(name: String) = fields.fieldNames.contains(name)
      val hasZCube = has("tags") && fields("tags").dataType.asInstanceOf[StructType].fieldNames.contains("ZCUBE_ID")
      Some(json
        .where(col(s"$action.path").isNotNull)
        .withColumn("stats", from_json(if (has("stats")) col(s"$action.stats") else lit(null).cast(StringType), statsSchema))
        .select(
          Seq(
            lit(commit).as("commit"),
            lit(action).as("action"),
            col(s"$action.path").as("path"),
            (if (hasZCube) col(s"$action.tags.ZCUBE_ID") else lit(null).cast(StringType)).as("zcube_id")
          ) ++ statsCols ++ Seq(
            (if (has("tags")) to_json(col(s"$action.tags")) else lit(null).cast(StringType)).as("tags")
          ): _*
        ))
    }
  spark.sparkContext.setJobDescription(s"Display max/min stats for files added/removed in delta table (commits ${fromCommit} to ${toCommit})")
  val perCommit = (fromCommit to toCommit).flatMap { commit =>
    val json = spark.read.json(f"$tablePath/_delta_log/$commit%020d.json")
    Seq("add", "remove").flatMap(action => fileActions(json, commit, action))
  }
  require(perCommit.nonEmpty, s"No files added or removed between commits $fromCommit and $toCommit")
  perCommit.reduce(_ unionByName _).orderBy((Seq("commit", "action") ++ minCols :+ "path").map(col): _*)
}

def showDf(df: DataFrame): Unit = {
  display(df) // Databricks
  // df.show(100, false) // local
}

def selectQuery(tablePath: String, tag: String): Unit = {
  spark.sparkContext.setJobDescription(s"Select query = $tag")
  val df = DeltaTable.forPath(tablePath).toDF
  val c = df.where(s"country_code = 'IT'").count()
  println(c)
}

// COMMAND ----------

// DBTITLE 1,Before Z-order
// show that files have data that is not well organized (ranges max/min values overlap for different files)
showDf(showMaxMinStats(inputDir, Seq("country_code"), 0, 0))

// COMMAND ----------

// Separate cell: display runs its query when the cell ends, so it would get this cell's job description
selectQuery(inputDir, "NO ZORDER")

// COMMAND ----------

// DBTITLE 1,Optimize with Z-order
// Execute the optimize command
spark.sparkContext.setJobDescription("OPTIMIZE with Z-ORDER")
spark.conf.set("spark.databricks.delta.optimize.maxFileSize", 100 * 1024L)
DeltaTable.forPath(inputDir).optimize().executeZOrderBy("country_code") 

// COMMAND ----------

// DBTITLE 1,After Z-order
// show that files have data that is well organized now
showDf(showMaxMinStats(inputDir, Seq("country_code"), 1, 1))

// COMMAND ----------

selectQuery(inputDir, "ZORDER")

// Check the Spark UI to see the number of files read for the same select query before and after z-order.

// COMMAND ----------

// DBTITLE 1,Appendix: is Z-order incremental?
// Incremental Z-order (Databricks runtime only): files z-ordered together in one OPTIMIZE share a ZCUBE_ID (see zcube_id).
// A later OPTIMIZE ZORDER BY on the same columns:
// - keeps cubes at or above the min cube size threshold untouched, and z-orders only the new files into a new cube
// - merges cubes below the threshold ("partial" cubes) with the new files, and rewrites them all into one new cube
// The threshold conf is internal and not in the Databricks docs. Default ~100GB, so on this small table the commit 1
// cube is partial and gets fully rewritten. We lower it to keep the commit 1 cube.
// The name differs from OSS: OSS has 'optimize.clustering.mergeStrategy.minCubeSize.threshold', used by liquid
// clustering only (OSS ZORDER always rewrites the whole partition); setting that one has no effect on Databricks ZORDER.
// NOTE: spark.conf.set is session-scoped: start a new session to rerun without it.
spark.conf.set("spark.databricks.delta.optimize.zorder.mergeStrategy.minCubeSize.threshold", 10 * 1024L)

// Append new data (commit 2): its country_code values span the whole domain again
spark.sparkContext.setJobDescription("Append new data (not z-ordered)")
airports.sample(0.2, 42).repartition(2).write.format("delta").mode("append").save(inputDir)

// Z-order again (commit 3)
spark.sparkContext.setJobDescription("OPTIMIZE with Z-ORDER (incremental)")
DeltaTable.forPath(inputDir).optimize().executeZOrderBy("country_code")

// COMMAND ----------

// Look at the 'remove' rows of commit 3:
// - incremental (Databricks): only commit 2 files are removed, the commit 1 cube survives, new files get a new zcube_id
// - not incremental (OSS): commit 1 and 2 files are all removed and re-added under one new zcube_id
//   (also what Databricks does without the threshold conf above, since the commit 1 cube is partial)
showDf(showMaxMinStats(inputDir, Seq("country_code"), 1, 3))

// COMMAND ----------

// If incremental, the two cubes overlap on country_code: expect ~1 file read per cube (see "number of files read").
// Each new cube adds overlap: read amplification grows until cubes are merged / rewritten.
selectQuery(inputDir, "INCREMENTAL ZORDER")
