// Databricks notebook source
// Spark: 3.5.1
// Local: --driver-memory 1G --master 'local[2]' --conf spark.ui.retainedJobs=2

// COMMAND ----------

/*
This snippet shows how data type for numerical information and compression can affect Spark.

# Symptom
Storage needs do not match with expectations, for example volume is higher.

# Explanation
There is difference in type when reading the data and type when writing it, causing a loss of compression perfomance.

*/

// COMMAND ----------

// We are going to demonstrate our purpose by converting the same numerical data into different types,
// and write it in parquet using different compression


// Here are the types we want to compare
val allNumericTypes = Seq("byte","short","int","long","double","float", "string","decimal(9,2)", "decimal(18,2)")

// Here are the compressions we want to compare.
// There are more in Spark SQL Parquet Data source documentation: see other in  https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#data-source-option
val allParquetCompressions = Seq("none","gzip","snappy")

// our initial data is generated as one million random numbers.
val rndDF = spark.sparkContext
  .parallelize(
    List.range (0 ,1000000)
      .map(I=>(math.random * 1000000)
      ) )
      .toDF


// COMMAND ----------
println (s"${"-"*20}")
println (s"Write data")

// let's write the data using all expected types and compressions
for (numericTypeName <- allNumericTypes) {
  for (parquetCompressionName <- allParquetCompressions ){
    val fileName=s"1M/Parquet_${numericTypeName}_${parquetCompressionName}"
    print (".")
    spark.sparkContext.setJobGroup("Write data",s"Write ${numericTypeName} with compression ${parquetCompressionName}")
    rndDF.selectExpr(s"cast(value as $numericTypeName )")
      .write
      .option("compression", parquetCompressionName)
      .format("parquet")
      .mode("overwrite")
      .save(fileName)

  }
}


// COMMAND ----------

// Now it's up to you to manually check the amount of storage required for each generated folder ...

// ... or to use Spark's Hadoop configuration to do it for you:
println (s"${"-"*20}")
println (s"Check written data size")
val hadoopFileSystem = org.apache.hadoop.fs.FileSystem.get(
  java.net.URI.create("./")
  , spark.sparkContext.hadoopConfiguration)

//search for sum of files size in kiloBytes, per parent folder:
val sizeOnDisk= hadoopFileSystem.globStatus(new org.apache.hadoop.fs.Path("./1M/**/part*"))
  .filter(o=>o.isFile)
  .map(o=> (o.getLen/1024, o.getPath.getParent.toString))
  .groupBy(_._2)
  .mapValues(_.map(_._1).sum).toSeq.sortBy(_._2)

println("part* files sizes (in kB):")
sizeOnDisk.foreach( o=>println ( s"${o._1}\t${o._2}\tkB"))

// COMMAND ----------

// Now let's see how long it takes to read and process such data when using different compression and numerical data formats .
println (s"${"-"*20}")
println (s"Read written data")
import scala.collection.mutable.ArrayBuffer
val readInformations: ArrayBuffer[(String,Long,Any)] = ArrayBuffer.empty
for (numericTypeName <- allNumericTypes) {
  for (parquetCompressionName <- allParquetCompressions ){
    val fileName=s"1M/Parquet_${numericTypeName}_${parquetCompressionName}"
    print(".")
    spark.sparkContext.setJobGroup("Read data",s"Read ${numericTypeName} with compression ${parquetCompressionName}")

    val t0=System.nanoTime
    val aggValue = spark.read.parquet(fileName)
      .selectExpr (s"avg(value) as `avg_from_${numericTypeName}_${parquetCompressionName}`")
      .first
      .get(0)

    val t1=System.nanoTime
    readInformations.append((fileName,(t1-t0)/1000000, aggValue))
  }}


println("Time to read and calculating aggregated value")
readInformations.foreach(readInformation=> println ( s"${readInformation._1}\t${readInformation._2} ms\t(agg value was ${readInformation._3})" ))



// COMMAND ----------

// Now you should also check how the variety of values affects compression :
// our initial source of data is using Doubles between 0 and 1000000
//      .map(I=>(math.random * 1000000)
// maybe could you check what's the best option if you have 1000 distinct values only, example:
//      .map(I=>(math.floor(math.random * 1000)+999000)