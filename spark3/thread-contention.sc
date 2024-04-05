// Arguments: --executor-memory 1G --driver-memory 1G --executor-cores 4
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, udf}

val spark: SparkSession = SparkSession.active
//import java.util.concurrent.locks.ReentrantLock
spark.sparkContext.setJobDescription("Step B: shuffle.partitions")
// Disable AQE to have a single job
spark.conf.set("spark.sql.adaptive.enabled", false)

import spark.implicits._

case class AsciiName(name: String)
//class Locker {}
//val locker = new Locker()


def f(r: AsciiName): AsciiName = {
    val locker = System.getProperties
    //println(locker.hashCode())
    locker.synchronized {
        val k = (1 to 1000).toList.sum
        AsciiName(s"op(${r.name})+${k}")
    }
}
def run(): Unit = {
    val input = s"${System.getenv("SSCE_PATH")}/datasets/optd_por_public_all.csv"
    val df = spark.read.option("delimiter", "^").option("header", "true").csv(input).cache()
    val ds = df.select("asciiname").map(r => AsciiName(r.getAs[String]("asciiname")))
    (1 to 1000).par.foreach { i =>
        ds.map{r => f(r)
        }.write.format("noop").mode("overwrite").save()
    }
}

// Then you can explore the UI
//(1 to 1000).par.foreach { i => 
//  df.select(threadLockOperation(col("asciiname"))).write.format("noop").mode("overwrite").save()
//}

// Then you can explore the UI
run()
