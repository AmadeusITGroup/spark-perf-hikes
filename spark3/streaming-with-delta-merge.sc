// Local: --executor-memory 1G --driver-memory 1G --executor-cores 1 --master local[2] --packages io.delta:delta-core_2.12:2.4.0,org.apache.spark:spark-avro_2.12:3.3.2 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog

import java.util.UUID
import io.delta.tables.DeltaTable

val tmpPath = "/tmp/amadeus-spark-lab/sandbox/" + UUID.randomUUID()
val inputDir = tmpPath + "/input"
val outputDir = tmpPath + "/output"
val checkpointDir = tmpPath + "/checkpoint"

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
//import sys.process._

sc.setJobDescription("Read CSV")
val inputCsv = spark.read.option("delimiter","^").option("header","true").csv(s"/tmp/amadeus-spark-lab/datasets/optd_por_public_all.csv")
sc.setJobDescription("Format input dataset")
inputCsv.repartition(1000).write.format("delta").save(inputDir)

sc.setJobDescription("Initialize output delta table")
spark.read.format("delta").load(inputDir).where(col("country_name") === "None").write.format("delta").save(outputDir) // just create an empty delta table

def processBatch(batch: DataFrame, batchId: Long): Unit = {
  println(s"Batch merge $batchId: tmpPath $tmpPath)")
  sc.setJobDescription(s"Batch nostro: $batchId")
  DeltaTable
    .forPath(outputDir)
    .as("output")
    .merge(
      batch.as("batch"), 
      "batch.iata_code == output.iata_code and batch.geoname_id == output.geoname_id"
    )
    .whenMatched.updateAll
    .whenNotMatched.insertAll
    .execute()
}

sc.setJobDescription(s"Streaming query begins")
spark
  .readStream
  .format("delta")
  .option("maxBytesPerTrigger", "100m")
  .option("ignoreChanges", "true")
  .load(inputDir) 
  .writeStream
  .option("checkpointLocation", checkpointDir)
  .foreachBatch(processBatch _)
  .outputMode(OutputMode.Update)
  .trigger(Trigger.AvailableNow())
  .start()

