// Spark: 3.4.2
// Local: --executor-memory 1G --driver-memory 1G --executor-cores 1 --master local[2] --packages io.delta:delta-core_2.12:2.4.0,org.apache.spark:spark-avro_2.12:3.3.2 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
// Databricks: ...

// COMMAND ----------

/*
This example shows how to address the read amplification problem in case of merges,
using Dynamic File Pruning (DFP), z-order and Photon.

References:
- https://docs.databricks.com/en/optimizations/dynamic-file-pruning.html
- https://www.databricks.com/blog/2020/04/30/faster-sql-queries-on-delta-lake-with-dynamic-file-pruning.html
- https://docs.databricks.com/en/delta/data-skipping.html#delta-zorder

IMPORTANT: DFP and Photon are only available when running on Databricks.

# Symptom
You are merging a small dataframe into a big table and you are reading way more data that is actually needed
to perform the merge.
You know that you could read less data, because you know that only few records in the input dataframe will match
with records in the big table.

# Explanation

In order to limit read amplification, we can:
- z-order the table, in order to colocate closer keys in the same files
- make sure that DFP is activated
  - the build side must be broadcastable
  - spark.databricks.optimizer.deltaTableSizeThreshold should be small enough
  - spark.databricks.optimizer.deltaTableFilesThreshold should be small enough
  - spark.databricks.optimizer.dynamicFilePruning should be true
- use Photon

Important note: if the keys in the input dataframe are such that they hit every files of the table,
even having z-oder and activating DFP, there will still be read amplification.
...

*/

// COMMAND ----------

import java.util.UUID
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.DataFrame
import io.delta.tables.DeltaTable

val input = "/tmp/amadeus-spark-lab/datasets/optd_por_public_all.csv"
val tmpPath = "/tmp/amadeus-spark-lab/sandbox/" + UUID.randomUUID()
val deltaDir = tmpPath + "/input"

val spark: SparkSession = SparkSession.active

// Be sure to have the conditions to trigger DFP: broadcast join + table/files thresholds
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10000000000L)
spark.conf.set("spark.databricks.optimizer.deltaTableSizeThreshold", 1)
spark.conf.set("spark.databricks.optimizer.deltaTableFilesThreshold", 1)
spark.conf.set("spark.databricks.optimizer.dynamicFilePruning", "true")

// COMMAND ----------

// Destination delta table
spark.sparkContext.setJobDescription("Create delta table")
val inputCsv = spark.read.option("delimiter","^").option("header","true").csv(input)
inputCsv.write.mode("overwrite").format("delta").save(deltaDir)

// z-order the delta table on the join key
spark.sparkContext.setJobDescription("Z-order probe table")
spark.conf.set("spark.databricks.delta.optimize.maxFileSize", 1 * 1024 * 1024L)
DeltaTable.forPath(deltaDir).optimize().executeZOrderBy("iata_code")

// COMMAND ----------

// Note the total number of files after z-ordering
DeltaTable.forPath(deltaDir).detail.select("numFiles").show

// COMMAND ----------

def buildDataframeToMerge(countryCode: String, newVal: String): DataFrame =
  inputCsv.where(col("country_code") === countryCode).drop("population").withColumn("population", lit(newVal))

val keyCols = Seq("iata_code", "geoname_id", "envelope_id", "fcode", "location_type")
def merge(df: DataFrame): Unit = {
  DeltaTable
    .forPath(deltaDir).as("o")
    .merge(df.as("t"), keyCols.map(colName => s"t.$colName == o.$colName").mkString(" and "))
    .whenMatched.updateAll.whenNotMatched.insertAll.execute()
}

spark.sparkContext.setJobDescription("Merge IT")
merge(buildDataframeToMerge("IT", "1000"))

// Go to the Databricks Spark UI, look for the SQL query corresponding to the Merge,
// and see the number of files actually read in the 'PhotonScan parquet' node

// WARNING
// Problem with this example.
// In the dataset there are TOO MANY nulls for the iata_code column, which is the z-order column
// (14 out of 17 files have NULL, NULL as min/max values after zorder).
// As we are doing an inner join, records with null keys on either side will be skipped.
// So even without DFP/Photon, there is data skipping, simply because of this.
// Even using a synthetic key that has no null, still there is some skipping
// (5 files pruned out of 18, see ra-merge-dfp-zorder-photon-custom.sc on dbx)