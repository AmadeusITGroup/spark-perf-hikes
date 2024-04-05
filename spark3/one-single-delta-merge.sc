// Arguments: --executor-memory 1G --driver-memory 1G --executor-cores 1 --master local[2]

import java.util.UUID
import io.delta.tables.DeltaTable

val inputDir = "datasets/optd_por_public_all.delta.cdf1000"
val tmpPath = "/tmp/tmpPath/" + UUID.randomUUID()
val outputDir = tmpPath + "/output"
val checkpointDir = tmpPath + "/checkpoint"

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import sys.process._

spark.conf.set("spark.sql.shuffle.partitions", "33")
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

s"rm -fr tmpdir" !
s"ln -s ${tmpPath} tmpdir" !

spark.sparkContext.setJobGroup(s"Initialization nostra", "Init nostro")
spark.read.format("delta").load(inputDir).where(col("country_name") === "None").write.format("delta").save(outputDir)

def processBatch(batch: DataFrame, batchId: Long): Unit = {
  println(s"Batch merge $batchId: tmpPath $tmpPath)")
  spark.sparkContext.setJobGroup(s"Batch nostro: $batchId", "Merge nostro")
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

spark.sparkContext.setJobGroup(s"Streaming query nostra", "Streaming query nostra")
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

