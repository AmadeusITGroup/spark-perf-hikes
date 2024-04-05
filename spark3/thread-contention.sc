// Arguments: --executor-memory 1G --driver-memory 1G --executor-cores 16
import org.apache.spark.sql.functions.{col, udf}
import java.util.concurrent.locks.ReentrantLock
sc.setJobDescription("Step B: shuffle.partitions")
// Disable AQE to have a single job
spark.conf.set("spark.sql.adaptive.enabled", false)


object Locker {
//  final val lock = new ReentrantLock()
}

val input = s"${System.getenv("SSCE_PATH")}/datasets/optd_por_public_all.csv"
val df = spark.read.option("delimiter","^").option("header","true").csv(input).cache()
val threadLockOperation = udf{(s: String) => 
    Locker.synchronized {
    //Locker.lock.lock()
    //println(Locker.lock)
    val k = (1 to 1000).toList.sum
    //Locker.lock.unlock()
    s"op($s)+${k}"
    }
}

// Then you can explore the UI
(1 to 1000).par.foreach { i => 
  df.select(threadLockOperation(col("asciiname"))).write.format("noop").mode("overwrite").save()
}
