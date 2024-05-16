// Spark: 3.5.1
// Local: --driver-memory 1G --master 'local[2]' --packages io.delta:delta-spark_2.12:3.1.0 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
// Databricks: 13.3LTS+photon

// COMMAND ----------

/*

This example shows how Photon works best when UDFs are replaced by SQL built-in functions.
It also shows how to figure out if Photon is underused in a given section of the execution plan.
Note: this snippet is only meaningful to be executed on Databricks (as Photon is not 
available locally).

# What to aim for
- Photon is fully used: execution plan in SQL Queries is mostly yellow.

# Symptom
You enabled Photon (and agreed to pay the extra cost), but the cost gain is below expectations. 
For instance the overall cost of your Databricks job is higher when Photon enabled.

# Explanation

Chances are Photon is not being fully used. To verify this you can go to the Spark UI, 
tab "SQL / DataFrame", open the two SQL queries named "Select ..." in different tabs to
compare them.

Select 1 (with UDF) presents an execution plan with a WholeStageCodegen that is not supported by 
Photon, hence in blue. It is preceeded by a suboptimal ColumnarToRow operator, because internally 
the UDF is applied to rows (and they are not columnar). Furthermore, going to Details below, you 
should see a message like the following: 

```
== Photon Explanation ==
Photon does not fully support the query because:
	UDF(name#2964) is not supported:
		The expression `scalaudf` is currently unimplemented.
Reference node:
	Project [UDF(name#2964) AS UDF(name)#2989]

which means that photon did not fully support the query.
```

Select 2 (with builtin functions), on the other hand, is fully supported by Photon, hence most 
of its operators in the Execution Plan are yellow (Photon used). If we go to Details below, 
you should see a message like the following confirming: 

```
== Photon Explanation ==
The query is fully supported by Photon.
```

Clearly, the best is to use built-in SQL functions, as Photon can be fully used.
*/

// COMMAND ----------
import java.util.UUID
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
val spark: SparkSession = SparkSession.active
// COMMAND ----------
spark.conf.set("spark.sql.adaptive.enabled", false)
val customUdf = udf { (s: String) => s"prefix-$s" } // custom UDF, Photon cannot run it efficiently
val input = "/tmp/perf-hikes/datasets/optd_por_public_filtered.csv"
val tmpPath = "/tmp/perf-hikes/sandbox/" + UUID.randomUUID()
val inputDeltaPath = tmpPath + "/input"
spark.sparkContext.setJobDescription("Initialization jobs")
val inputDf = spark.read.option("delimiter", "^").option("header", "true").csv(input)
inputDf.write.format("delta").save(inputDeltaPath)
val df = spark.read.format("delta").load(inputDeltaPath) // read from delta table
// COMMAND ----------
// Photon underused
spark.sparkContext.setJobDescription("Select 1: with UDF & save")
df.select(customUdf(col("name"))).write.parquet(tmpPath + "/with-udf")
// COMMAND ----------
// Photon used
spark.sparkContext.setJobDescription("Select 2: with SQL built-in function & save")
df.select(concat(lit("prefix-"), col("name"))).write.parquet(tmpPath + "/with-builtin")
// Follow above instructions to see if Photon is fully used.
