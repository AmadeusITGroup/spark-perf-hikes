// Spark: 3.5.1
// Local: --driver-memory 1G --master 'local[2]' --packages io.delta:delta-spark_2.12:3.1.0 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
// Databricks: ...

// COMMAND ----------

/* Compare performances of multiple .withColumn with one single .withColumns when Spark builds the exection plan.

# Symptom
Unexplain activity on driver side, sometimes very long, while the workers are doing nothing.

# Explanation
Application code uses a lot of .withColumn(colName: String, col: Column): and does not get benefits of improvements driven by .withColumns(colsMap: Map[String, Column]),
leading to a useless workload.

# What to aim for concretely
Long lasting driver time while nothing happens on workers.
*/

// COMMAND ----------

import org.apache.spark.sql.SparkSession
val spark: SparkSession = SparkSession.active

// Parameters used in this test session:

// Number of columns to create using .withColumn and .withColumns
val number_of_columns_to_create = 100
// Number of records to create in our dataframe
val number_of_records_of_df = 10
// Number of times we want to repeat the spark action
val number_of_action_repeat = 10

//In order to compare time spent for each steps of the test,
// timing function prints the elapsed time to execute a given code block.
// we also set the job description for Spark UI
def block_timer[T](label: String, block: => T): T = {
  spark.sparkContext.setJobDescription(label)

  val before = System.nanoTime
  println(s"$label - started: $before")
  val result = block
  val after = System.nanoTime
  println(s"$label - ended:   $after")
  println(s"$label - elapsed: " + (after - before) / 1000000 + "ms")
  result
}

//COMMAND
// for the purpose of our test,
// we use a dataframe of Int from 0 to 100k
// let's generate a DF of numbers
val df = spark.range(0, number_of_records_of_df, 1).toDF

// let's generate a list of column names for this test
val columnNames = {
  1 to number_of_columns_to_create toList
}.map(x => "newColumn_" + x.toString)

//COMMAND
/* Let's build the new Dataframe with many calls to .withColumn (colName) */
var df_many_calls_to_withColum = df
block_timer(s"Build df_many_calls_to_withColum (${columnNames.length} cols)",
  {
    columnNames.foreach(
      colName => {
        df_many_calls_to_withColum = df_many_calls_to_withColum.withColumn(colName, lit(null))
      }
    )
  }
)

/* and le'ts build the new Dataframe with one single call to .withColumn(map_of_columns)
 */
var df_single_call_to_withColumns = df
block_timer(s"Build df_single_call_to_withColumns (${columnNames.length} cols)",
  {
    val allColumns = columnNames.zip(columnNames.map(_ => lit(null))).toMap
    df_single_call_to_withColumns = df.withColumns(allColumns)
  }
)

//COMMAND
/* Now, you will have to check the execution plan, in term of complexity and time used
*/
block_timer(s"Explain df_many_calls_to_withColum (${columnNames.length} cols)",
  {
    df_many_calls_to_withColum.explain("extended")
  }
)
block_timer(s"Explain df_single_call_to_withColumns (${columnNames.length} cols)",
  {
    df_single_call_to_withColumns.explain("extended")
  }
)
/* please compare the respective plans
 == Parsed Logical Plan ==
 == Analyzed Logical Plan ==
 == Optimized Logical Plan ==
 == Physical Plan ==
 and also, compare the amount of time  used to build the full plan in both cases.
 */

//COMMAND
/*
Now we'll check the time to execute
 */
block_timer(s"repeat $number_of_action_repeat times  collect df_many_calls_to_withColum (${columnNames.length} cols)",
  {
    for (i <- 0 to number_of_action_repeat) {
      println("counted rows: "+df_many_calls_to_withColum.count)
    }
  }
)

block_timer(s"repeat $number_of_action_repeat times collect df_single_call_to_withColumns (${columnNames.length} cols)",
  {
    for (i <- 0 to number_of_action_repeat) {
      println("counted rows: "+df_single_call_to_withColumns.count)
    }
  }
)

// Compare the time used to execute the Spark actions.

// Now, re-run the snipped with number_of_records_of_df = 10
// How does the number of records affects to time to execute Spark actions ?

