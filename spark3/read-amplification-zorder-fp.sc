// Spark: 3.5.1
// Local: --executor-memory 1G --driver-memory 1G --executor-cores 1 --master 'local[2]' --packages io.delta:delta-spark_2.12:3.1.0 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
// Databricks: ...

/*
This snippet demonstrates the benefits of DeltaTable optimization executeZOrderBy:
it starts by preparing a delta table with 5 partitions based on test dataset  optd_por_public_filtered, then
shows how optimization with ZOrder can serve specific usage.

References:
 - https://docs.delta.io/2.0.0/optimizations-oss.html#z-ordering-multi-dimensional-clustering

# Symptom
If not particulary specified, data are heterogeneously distributed within files in Delta tables.


# Explanation
Data stored in Delta tables can be optimized with usage centric settings.

*/
import java.util.UUID
import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession

val input = "/tmp/amadeus-spark-lab/datasets/optd_por_public_filtered.csv"
val tmpPath = "/tmp/amadeus-spark-lab/sandbox/" + UUID.randomUUID()

//inputDir will store data as DeltaTable.
val inputDir = tmpPath + "/input"

val spark: SparkSession = SparkSession.active

spark.sparkContext.setJobDescription("Read input CSV")
val airports = spark.read.option("delimiter","^").option("header","true").csv(input)

spark.sparkContext.setJobDescription("Format input CSV into delta (multiple files)")
airports.repartition(5).write.format("delta").save(inputDir)

def showMaxMinStats(tablePath: String, colName: String, commit: Int): Unit = {
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
  df.show(false)
}

def selectQuery(tablePath: String, tag: String): Unit = {
  spark.sparkContext.setJobDescription(s"Select query = $tag")
  val df = DeltaTable.forPath(tablePath).toDF
  val c = df.where(s"country_code = 'IT'").count()
  println(c)
}

// show that files have data that is not well organized (ranges max/min values overlap for different files)
showMaxMinStats(inputDir, "country_code", 0)
selectQuery(inputDir, "NO ZORDER")

spark.conf.set("spark.databricks.delta.optimize.maxFileSize", 100 * 1024L)
//Exectute the optimize command
DeltaTable.forPath(inputDir).optimize().executeZOrderBy("country_code") 

// show that files have data that is well organized now
showMaxMinStats(inputDir, "country_code", 1)
selectQuery(inputDir, "ZORDER")

// Check the Databricks Spark UI to see the amount of files read for the same select query before and after z-order.
// TODO: it works also in local, WHY? It's supposed to work only with delta on Databricks (cf. https://docs.databricks.com/en/delta/data-skipping.html)