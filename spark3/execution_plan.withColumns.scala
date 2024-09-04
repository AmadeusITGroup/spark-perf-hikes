// Databricks notebook source
// Spark: 3.5.1
// Local: --master 'local[2]' --driver-memory 1G

// COMMAND ----------
/* Compare performances of multiple .withColumn with one single .withColumns when Spark builds the exection plan.

# Symptom
Unexplained activity on driver side, sometimes very long, while the workers are doing nothing.

# Explanation
Application code uses a lot of .withColumn(colName: String, col: Column): and does not get benefits of improvements driven by .withColumns(colsMap: Map[String, Column]),
leading to a useless workload.

# What to aim for concretely
For any given SQL query, we should have no time slots where workers are idle. This can be observed via CPU usage or via the amount of tasks running at any time.
*/

// COMMAND ----------

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val spark: SparkSession = SparkSession.active

// Parameters used in this test session:

// Number of columns to create using .withColumn and .withColumns
val numberOfColumnsToCreate = 100
// Number of records to create in our dataframe
val numberOfRecordsInDF = 10
// Number of times we want to repeat the spark action
val numberOfActionRepeat = 10

//In order to compare time spent for each steps of the test,
// timing function prints the elapsed time to execute a given code block.
// we also set the job description for Spark UI
def blockTimer[T](label: String, block: => T): T = {
  spark.sparkContext.setJobDescription(label)

  val before = System.nanoTime
  println(s"$label - started: $before")
  val result = block
  val after = System.nanoTime
  println(s"$label - ended:   $after")
  println(s"$label - elapsed: " + (after - before) / 1000000 + "ms")
  result
}

// COMMAND ----------
// for the purpose of our test,
// we use a dataframe of Int from 0 to 100k
// let's generate a DF of numbers
val df = spark.range(0, numberOfRecordsInDF, 1).toDF

// let's generate a list of column names for this test
val columnNames = {
  1 to numberOfColumnsToCreate toList
}.map(x => "newColumn_" + x.toString)

// COMMAND ----------
/* Let's build the new Dataframe with many calls to .withColumn (colName) */
var dfManyCallsToWithColum = df
blockTimer(s"Build dfManyCallsToWithColum (${columnNames.length} cols)",
  {
    columnNames.foreach(
      colName => {
        dfManyCallsToWithColum = dfManyCallsToWithColum.withColumn(colName, lit(null))
      }
    )
  }
)

/* and le'ts build the new Dataframe with one single call to .withColumns(...)
 */
var dfSingleCallToWithColumns = df
blockTimer(s"Build dfSingleCallToWithColumns (${columnNames.length} cols)",
  {
    val allColumns = columnNames.zip(columnNames.map(_ => lit(null))).toMap
    dfSingleCallToWithColumns = df.withColumns(allColumns)
  }
)

// COMMAND ----------
/* Now, you will have to check the execution plan, in term of complexity and time used
*/
blockTimer(s"Explain dfManyCallsToWithColum (${columnNames.length} cols)",
  {
    dfManyCallsToWithColum.explain("extended")
  }
)
blockTimer(s"Explain dfSingleCallToWithColumns (${columnNames.length} cols)",
  {
    dfSingleCallToWithColumns.explain("extended")
  }
)
/* please compare the respective plans
 == Parsed Logical Plan ==
 == Analyzed Logical Plan ==
 == Optimized Logical Plan ==
 == Physical Plan ==
 and also, compare the amount of time  used to build the full plan in both cases.
 */

// COMMAND ----------
/*
Now we'll check the time to execute
 */
blockTimer(s"repeat $numberOfActionRepeat times  collect dfManyCallsToWithColum (${columnNames.length} cols)",
  {
    for (i <- 0 to numberOfActionRepeat) {
      println("counted rows: " + dfManyCallsToWithColum.count)
    }
  }
)

blockTimer(s"repeat $numberOfActionRepeat times collect dfSingleCallToWithColumns (${columnNames.length} cols)",
  {
    for (i <- 0 to numberOfActionRepeat) {
      println("counted rows: " + dfSingleCallToWithColumns.count)
    }
  }
)

// Compare the time used to execute the Spark actions.

// Now, re-run the snipped with numberOfRecordsInDF = 10
// How does the number of records affects to time to execute Spark actions ?

// COMMAND ----------
//Checking further the difference between .withColumn and .withColums: let's checkl effects on join

//Depending on your environment, the multiple calls to .withColum in DF creatio may lead this example code to a java.lang.RuntimeException from Spark's catalyst ...
blockTimer(s"Demonstration of join using two dfManyCallsToWithColum  (${columnNames.length} cols)",
  {
    //Note: here we rename the "id" column to avoid message "Perhaps you need to use aliases."
    val d1ManyCallsToWithColum = dfManyCallsToWithColum.withColumnRenamed("id", "id1")
    val d2ManyCallsToWithColum = dfManyCallsToWithColum.withColumnRenamed("id", "id2")
    val djoinManyCallsToWithColum = d1ManyCallsToWithColum.join(d2ManyCallsToWithColum, d1ManyCallsToWithColum("id1") === d2ManyCallsToWithColum("id2"), "inner")
    djoinManyCallsToWithColum.explain
  })

//... whereas this execution passes.
blockTimer(s"Demonstration of join using two dfSingleCallToWithColumns  (${columnNames.length} cols)", {
  val d1SingleCallToWithColumns = dfSingleCallToWithColumns.withColumnRenamed("id", "id1")
  val d2SingleCallToWithColumns = dfSingleCallToWithColumns.withColumnRenamed("id", "id2")
  val djoinSingleCallToWithColumns = d1SingleCallToWithColumns.join(d2SingleCallToWithColumns, d1SingleCallToWithColumns("id1") === d2SingleCallToWithColumns("id2"), "inner")
  djoinSingleCallToWithColumns.explain
})
