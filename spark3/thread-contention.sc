// Arguments: --executor-memory 1G --driver-memory 1G --executor-cores 4

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}

val spark: SparkSession = SparkSession.active
spark.sparkContext.setJobDescription("Step B: shuffle.partitions")

val threadLockOperation = udf { (s: String) =>
  val locker = System.getProperties
  locker.synchronized {
    s"op($s)"
  }
}

val input = s"${System.getenv("SSCE_PATH")}/datasets/optd_por_public_all.csv"
val df = spark.read.option("delimiter", "^").option("header", "true").csv(input).cache()
(1 to 10000).foreach { _ =>
  df.select(threadLockOperation(col("asciiname"))).write.format("noop").mode("overwrite").save()
}
