// Spark: 3.5.1
// Local: --driver-memory 700M --master 'local[8]' --packages io.delta:delta-spark_2.12:3.1.0 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog --conf spark.sql.autoBroadcastJoinThreshold=-1 --conf spark.memory.fraction=0.05
// Databricks: ...

// COMMAND ----------

/*

This example shows how a join with null-safe-equality can lead to an uncontrolled explosion of data and potentially spills on a single task. 

# Symptom
A task seems to be skewed and takes really long time to complete, causes spill, and has little amount of shuffle read and little amount of shuffle write. 

# Explanation

The join takes place with null-safe-equality (null==null gives true, null==X gives false). A couple of columns are null on both tables, and they are therefore joined together. After the shuffle of the join, records with such null columns fall in the same partition, and they get exploded (cartesian product), consuming significant amounts of memory and potentially spilling (in this example we made the spark.memory.fraction very small). Then they can be filtered potentially before doing a shuffle write (if there is a next stage). All this causes a stage that is extremely long because it is putting together in one task the null records, which are exploded causing spill, and then filtered, hence writing little amount of shuffle write.
*/

import java.util.UUID
import org.apache.spark.sql.functions._
import spark.implicits._
val tmpPath = "/tmp/amadeus-spark-lab/sandbox/" + UUID.randomUUID()
spark.conf.set("spark.sql.adaptive.enabled", false)
spark.conf.set("spark.sql.shuffle.partitions", 111)

case class Record(id: String, version: String, other: String)

Range(1,10000).flatMap{v => Seq(
  //Record("a", "1", "x"),
  Record(null, "1", "xasdflkjalsdflkajlsdfasdfj"), // id b
  Record(null, "1", s"xlkjasdfadfhaksldfjklkjasdxf${v}HE"), // id c
)}.toDS.toDF.repartition(10).write.parquet(tmpPath + "/df1")

Range(1,20000).flatMap{v => Seq(
  //Record("a", "1", "x"),
  //Record("b", "1", "x"),
  //Record("c", "1", "x"),
  Record(null, "1", "a;lsjkalsdflkjasdfljasdfx"), // id d
  Record(null, "1", "RExkljaldflkajsldfkjalsdkjflaskdjf"), // id e
  Record(null, "1", "asdflkjasdlf;iuerpqiuwerpiqwerx"), // id f
)}.toDS.toDF.repartition(10).write.parquet(tmpPath + "/df2")


val df1 = spark.read.parquet(tmpPath + "/df1")
val df2 = spark.read.parquet(tmpPath + "/df2")

val df3 = df1.join(df2, df1("id") <=> df2("id") and df1("version") <=> df2("version")).select(df1("id"), df1("version"), concat(df1("other"),df2("other")).as("other"))

val dfToWrite = df3.where(col("other") like "%000HERE%")
// val dfToWrite = spark.createDataFrame(df3.rdd, df3.schema).where(col("other") like "%000HERE%") // break the lineage and see what happens :)
spark.sparkContext.setJobDescription("HERE!!!")
dfToWrite.repartition(2).write.parquet(tmpPath + "/output")

// Really long task, that produces little shuffle write (although hugue explode then filter), and spills because of large amount of memory use
// What's new: when having a filter after a shuffle it is not shown in the execution plan, but still executed, that's why you can have an explosion of data (and even a spill) even with little shuffle write, it seems to be the peak execution memory that absorbs the explosion, we tightened it by decreasing spark.memory.fraction
