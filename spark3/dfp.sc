// Local: --executor-memory 1G --driver-memory 1G --executor-cores 1 --master local[2] --packages io.delta:delta-core_2.12:2.4.0,org.apache.spark:spark-avro_2.12:3.3.2 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog

import java.util.UUID
import io.delta.tables.DeltaTable

// COMMAND ----------

val tmpPath = "/tmp/amadeus-spark-lab/sandbox/" + UUID.randomUUID()
val probeDir = tmpPath + "/input"
val buildDir = tmpPath + "/employee"

// COMMAND ----------

sc.setJobDescription("Read input CSV")
val inputCsv = spark.read.option("delimiter","^").option("header","true").csv("/tmp/amadeus-spark-lab/datasets/optd_por_public_all.csv")
sc.setJobDescription("Format input CSV into delta (probe side)")
inputCsv.repartition(4).write.format("delta").save(probeDir)

case class Employee(name: String, role: String, residence: String)
val employeesData = Seq(
  Employee("Thierry", "Software Engineer", "FR"),
  Employee("Mohammed", "DevOps", "FR"),
  Employee("Gene", "Intern", "FR"),
  Employee("Mau", "Intern", "FR"),
  Employee("Mathieu", "Software Engineer", "FR"),
  Employee("Thomas", "Intern", "FR")
)
sc.setJobDescription("Create build table")
employeesData.toDF.write.format("delta").save(buildDir)

val deltaTable = DeltaTable.forPath(probeDir)
val employeeTable = DeltaTable.forPath(buildDir)

def showMaxMinStats(tablePath: String, colName: String, commit: Int): Unit = {
  // stats on parquet files added to the table
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.types._
  import io.delta.tables.DeltaTable
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
  sc.setJobDescription(s"Display max/min stats for files present in delta table (commit ${commit})")
  df.show(false)
}

// Join a 2 tables (probe and build), adding a filter in the smaller one (the build side of the join)
def joinTables(description: String): Unit = {
  val probe = DeltaTable.forPath(probeDir).toDF
  val build = DeltaTable.forPath(buildDir).toDF
  sc.setJobDescription(s"Join tables: $description")
  probe
  .join(build, build("residence") === probe("country_code"), "inner")
  .where(col("role") === "Intern")
  .count()
}

joinTables("before zorder")

// Be sure to trigger DFP: broadcast join + table/files thresholds
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10000000000)
spark.conf.set("spark.databricks.optimizer.deltaTableSizeThreshold", 1)
spark.conf.set("spark.databricks.optimizer.deltaTableFilesThreshold", 1)
spark.conf.set("spark.databricks.optimizer.dynamicFilePruning", true)

// zorder table
spark.conf.set("spark.databricks.delta.optimize.maxFileSize", 1 * 1024 * 1024L)
DeltaTable.forPath(probeDir).optimize().executeZOrderBy("country_code")

joinTables("after zorder")
