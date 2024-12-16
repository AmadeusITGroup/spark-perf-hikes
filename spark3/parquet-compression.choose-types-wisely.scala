// Databricks notebook source
// Spark: 3.5.1
// Local: --driver-memory 2G --master 'local[4]' --conf spark.ui.retainedJobs=2

// COMMAND ----------

/*
This snippet shows how data type for numerical information and compression can affect Spark.

# Symptom
Storage needs do not match with expectations, for example volume is higher.

# Explanation
There is difference in type when reading the data and type when writing it, causing a loss of compression perfomance.

*/

// COMMAND ----------
import java.util.UUID
// We are going to demonstrate our purpose by converting the same numerical data into different types,
// and write it in parquet using different compression

// Here are the types we want to compare
val allNumericTypes = Seq("byte","short","int","long","double","float", "string","decimal(12,3)","decimal(15,6)", "decimal(9,0)")

// Here are the compressions we want to compare.
// There are more in Spark SQL Parquet Data source documentation: see other in  https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#data-source-option
val allParquetCompressions = Seq("none","gzip","snappy", "lz4", "zstd")

// destination folder for files
val rootFolder ="/tmp/perf-hikes/sandbox/" + UUID.randomUUID()

// our initial data contains one million random numbers.
val rndDF = spark.sparkContext.parallelize(
    List.range (0 ,1000000)
      .map(I=>
        (math.floor(math.random * 100000 ) )
      )).toDF
// Note: as we do not use the slice argument of parallelize and we've lots of row, we may have some warnings
// Stage xxx contains a task of very large size (xxx KiB). The maximum recommended task size is 1000 KiB.
// don't bother about this warning.



// Let's create the test data
println (s"${"-"*20}")

println (s"Write data files to $rootFolder.")

// let's write the data using all expected types and compressions
for (numericTypeName <- allNumericTypes) {
  for (parquetCompressionName <- allParquetCompressions ){
    val fileName=s"$rootFolder/Parquet_${numericTypeName}_${parquetCompressionName}"
    print (".")
    spark.sparkContext.setJobGroup("Write data",s"Write ${numericTypeName} with compression ${parquetCompressionName}")
    rndDF.selectExpr(s"cast(value as $numericTypeName )")
      .write
      .option("compression", parquetCompressionName)
      .format("parquet")
      .mode("overwrite").save(fileName)
  }
}
println ("Done.")


// COMMAND ----------

// Now it's up to you to manually check the amount of storage required for each generated folder ...
// ... or use Spark's Hadoop configuration to fasten that step
println (s"${"-"*20}")
println (s"Here are the size of files (MBytes):")
val hadoopFileSystem = org.apache.hadoop.fs.FileSystem.get( java.net.URI.create(rootFolder) , spark.sparkContext.hadoopConfiguration)

//search for sum of files size in kiloBytes, per parent folder, and show data using a pivot to
// present a tabular view on compression performances
// +--------------+-------------+----+-----+------+- ... -+
// |storage MBytes|content      |gzip|none |snappy|  ...  |
// +--------------+-------------+----+-----+------+- ... -+
// what we want to show here is how crucial it is to choose the compression when you store data.
// In same show, we also demonstrate that type of data has an impact on data size.
hadoopFileSystem.globStatus(new org.apache.hadoop.fs.Path(s"$rootFolder/**/part*"))
  .filter(o=>o.isFile)                                                   // keep only files
  .map(o=> (o.getLen.toFloat/1024/1024, o.getPath.getParent.toString))   // keep only file size and parent folder
  .groupBy(_._2) .mapValues(_.map(_._1).sum).toSeq.sortBy(_._2)          // aggregate by folder
  .toDF("Path", "Size_MBytes")                                           // prepare data for pivot
  .withColumn("storage", split(col("Path"),"_")(0))
  .withColumn("storage",split(col("storage"),"/")(array_size(split(col("storage"),"/"))-1)) // keep only base path
  .withColumn("content", split(col("Path"),"_")(1))
  .withColumn("compression", split(col("Path"),"_")(2))
  .drop("Path")
  .withColumn("Size_MBytes", col("Size_MBytes").cast("decimal(8,3)"))
  .groupBy("storage","content")
  .pivot("compression") .agg(sum("Size_MBytes"))                         // pivot on compression type
  .orderBy("content")
  .withColumnRenamed("storage", "storage MBytes")
  .show(truncate=false )

// Have a break here.
// Our source dataset contains one million numbers.
// Have a look on space used depending on compression used.
// Please note how  flagrant is the impact of compression when we store strings.
// Please note as well how important it is to choose the correct data type and precision.
// For example with Decimal that have fixed precision (the maximum number of digits)
// and scale (the number of digits on right side of dot) : do we really need 6 digits


// COMMAND ----------

// Now let's see how long it takes to read and process such data when using different compression and numerical data formats .
println (s"${"-"*20}")
print (s"Read written data and collect metrics")
import scala.collection.mutable.ArrayBuffer
val readInformations: ArrayBuffer[(String,Long,Double)] = ArrayBuffer.empty
for (numericTypeName <- allNumericTypes) {
  for (parquetCompressionName <- allParquetCompressions ){
    val fileName=s"$rootFolder/Parquet_${numericTypeName}_${parquetCompressionName}"
    print(".")
    spark.sparkContext.setJobGroup("Read data",s"Read ${numericTypeName} with compression ${parquetCompressionName}")
    val t0=System.nanoTime
    val aggValue = spark.read.parquet(fileName)
      .selectExpr (s"cast(avg(value)as Double) as `avg_from_${numericTypeName}_${parquetCompressionName}`")
      .first.getDouble(0)
    val t1=System.nanoTime
    readInformations.append((fileName,((t1-t0)/1000000).toInt, aggValue))
  }}
println
println (s"Read done, metrics collected.")

// COMMAND ----------

// Compare the result of avg() calculation depending on the data type we used to write and the compression used.
// Purpose of this is to ensure you know the impact of your choices.
println (s"${"-"*20}")
println("Does the type of data and/or compression affect the calculation of average ?")
spark.sparkContext.parallelize(readInformations).map(o=>(o._1, o._3))
  .toDF("Path", "avg")
  .withColumn("storage", split(col("Path"),"_")(0))
  .withColumn("storage",split(col("storage"),"/")(array_size(split(col("storage"),"/"))-1)) // keep only base path
  .withColumn("content", split(col("Path"),"_")(1))
  .withColumn("compression", split(col("Path"),"_")(2))
  .drop("Path")
  .groupBy("storage","content")
  .pivot("compression") .agg(sum("avg"))                  // pivot on compression type
  .withColumnRenamed("storage", "result of avg(values)")
  .orderBy("content")
  .show(truncate=false )

// What you should see here (at least) :
// - conversion to  byte and short has affected the initial data
// - conversion to decimal can affect the value
// - compression does not affect the calculation of average


// COMMAND ----------

// Now let's see how long it takes to read and process such data when using different compression and numerical data formats .

println (s"${"-"*20}")
println("Does the type of data and/or compression affect the time to process data  ?")
spark.sparkContext.parallelize(readInformations).map(o=>(o._1, o._2)).toDF("Path", "timeMS")
  .withColumn("storage", split(col("Path"),"_")(0))
  .withColumn("storage",split(col("storage"),"/")(array_size(split(col("storage"),"/"))-1)) // keep only base path
  .withColumn("content", split(col("Path"),"_")(1))
  .withColumn("compression", split(col("Path"),"_")(2))
  .drop("Path")
  .groupBy("storage","content")
  .pivot("compression") .agg(sum("timeMS"))                  // pivot on compression type
  .withColumnRenamed("storage", "Processing time (ms)")
  .orderBy("content")
  .show(truncate=false )


// COMMAND ----------

// Now you should also check how the variety of values affects compression :
// our initial source of data is using Doubles between 0 and 1000000
//      .map(I=>(math.random * 1000000)
//
// maybe could you check what's the best option if you have 1000 distinct values only, examples:
// - only 1000 disctinct numbers but with 6 digits (long string equivalent)
//      .map(I=>(math.floor(math.random * 1000)+999000)
// - only 127 distinct values (short strings equivalents)
//      .map(I=>(math.floor(math.random * scala.Byte.MaxValue))


