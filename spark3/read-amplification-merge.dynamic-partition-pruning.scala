// Databricks notebook source
import java.util.UUID
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.DataFrame
import io.delta.tables.DeltaTable

val input = "/tmp/perf-hikes/datasets/optd_por_public_filtered.csv"
val tmpPath = "/tmp/perf-hikes/sandbox/" + UUID.randomUUID()
val deltaDir = tmpPath + "/input"

val spark: SparkSession = SparkSession.active

// COMMAND ----------

// Keep AQE on, to avoid having too many files after merge
spark.conf.set("spark.sql.adaptive.enabled", true)

// Destination delta table
import org.apache.spark.sql.functions.{col, substr}
spark.sparkContext.setJobDescription("Create delta table")
val airports = spark.read
  .option("delimiter", "^")
  .option("header", "true")
  .csv(input)
  .withColumn("partition", substr(col("iata_code"), lit(1), lit(1)))

// COMMAND ----------

display(airports)

// COMMAND ----------

airports.write.partitionBy("partition").mode("overwrite").format("delta").save(deltaDir)

// COMMAND ----------

// Note: we are artificially applying a filter on the input data frame that will be merged.
// Without this, DPP is not triggered
def filterDfBeforeMerge(df: DataFrame, iataCode: String): DataFrame = {
  df.where(col("iata_code") === iataCode)
}

def buildDataframeToMerge(iataCode: String, newVal: String): DataFrame = {
  val c = airports.where(col("iata_code") === iataCode).withColumn("comment", lit(newVal)).collect()
  val df = spark.createDataFrame(spark.sparkContext.parallelize(c), airports.schema)
  filterDfBeforeMerge(df, iataCode)
}

def buildDeltaTableToMerge(iataCode: String, newVal: String): DataFrame = {
  val df = buildDataframeToMerge(iataCode, newVal)
  val path = s"$tmpPath/batch_${iataCode}_${newVal}_${UUID.randomUUID()}"
  df.write.mode("overwrite").format("delta").save(path)
  filterDfBeforeMerge(DeltaTable.forPath(path).toDF, iataCode)
}

def merge(df: DataFrame): Unit = {
  val keyCols = Seq("iata_code", "partition")
  DeltaTable
    .forPath(deltaDir).as("o")
    .merge(df.as("t"), keyCols.map(colName => s"t.$colName == o.$colName").mkString(" and "))
    .whenMatched.updateAll.whenNotMatched.insertAll.execute()
}

// COMMAND ----------

// Be sure to have the conditions to trigger DPP: DPP enabled, broadcast join
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 1000000000L)

// DPP (input not from delta)
spark.sparkContext.setJobDescription("Merge 1 - DPP - input not delta")
merge(buildDataframeToMerge("AAI", "dpp-no-delta"))

// COMMAND ----------


// DPP (input from delta)
spark.sparkContext.setJobDescription("Merge 2 - DPP - input delta")
merge(buildDeltaTableToMerge("AAI", "dpp-delta"))