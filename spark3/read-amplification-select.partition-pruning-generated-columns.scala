// Databricks notebook source
// Spark: 3.5.1
// Local: --driver-memory 1G --master 'local[2]' --packages io.delta:delta-spark_2.12:3.1.0 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog

// COMMAND ----------

/*
This example shows how a filter based on a column used in a generated expression can lead to 
partition pruning.

References:
- https://docs.databricks.com/en/delta/generated-columns.html 
- https://delta.io/blog/2023-04-12-delta-lake-generated-columns/

# Symptom

Slow queries despite having partitions in place.

# Explanation

In this example we create a delta table with columns:
- 'datetime'
- 'datetime_pp' (same value as 'datetime')
- 'year' (filled in manually via a builtin SQL function, used for partitioning)
- 'year_pp' with same value as 'year' but is a *generated column* as 'year(datetime_pp)' (also used 
   for partitioning) 

If we query the table with a filter on 'datetime', we will not have partition pruning. Why? There is 
no information on the table associating how to infer 'year' value from the 'datetime' predicate value.

However, if we query the table with a filter on 'datetime_pp' we will have partition pruning. Why? The 
'year_pp' is computed as a generated column from a 'datetime_pp' applying 
the SQL builtin function year(). This creates an association between the column 'datetime_pp' and the 
column 'year_pp' that can be used by the optimizer to infer eligible partition values.

Note that only few SQL builtin functions are supported for partition pruning.

# What to aim for concretely

In the execution plan of the query, the 'PartitionFilters' should infer pruning based
on the where predicate values.

*/

// COMMAND ----------

import java.util.UUID
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

val spark: SparkSession = SparkSession.active

spark.conf.set("spark.sql.adaptive.enabled", false)

val tmpPath = "/tmp/perf-hikes/sandbox/" + UUID.randomUUID()

val delta = tmpPath + "/delta"

DeltaTable.create(spark).location(delta).
  addColumn("id", "INT").
  addColumn("datetime", "TIMESTAMP").
  addColumn("year", "INT"). // will be computed from datetime
  addColumn("datetime_pp", "TIMESTAMP").
  addColumn(DeltaTable.columnBuilder("year_pp").dataType("INT").generatedAlwaysAs("year(datetime_pp)").build()). // generated column from datetime_pp
  partitionedBy("year", "year_pp").
  execute()

val df = Seq(
  (0, java.sql.Timestamp.valueOf("2020-01-01 00:00:00")),
  (1, java.sql.Timestamp.valueOf("2021-01-01 00:00:00")),
  (2, java.sql.Timestamp.valueOf("2022-01-01 00:00:00")),
  (3, java.sql.Timestamp.valueOf("2023-01-01 00:00:00"))
).toDF("id", "datetime").
withColumn("datetime_pp", col("datetime")). // datetime_pp == datetime
withColumn("year", year(col("datetime"))) // year computed as SQL builtin function from datetime
df.write.format("delta").mode("append").saveAsTable(s"delta.`${delta}`")

spark.sparkContext.setJobDescription("EXPLORATORY SELECT")
spark.sql(s"SELECT * FROM delta.`${delta}` ORDER BY id").show()

spark.sparkContext.setJobDescription("No partition pruning")
val q1 = spark.sql(s"SELECT * FROM delta.`${delta}` WHERE datetime > '2022-01'")
q1.explain(true) // PartitionFilters: []... no partition pruning
q1.show(false)

spark.sparkContext.setJobDescription("With partition pruning")
val q2 = spark.sql(s"SELECT * FROM delta.`${delta}` WHERE datetime_pp > '2022-01'")
q2.explain(true) // PartitionFilters: [((year_pp#2105 >= year(cast(2022-01-01 00:00:00 as date))) ... some partition pruning
q2.show(false)
