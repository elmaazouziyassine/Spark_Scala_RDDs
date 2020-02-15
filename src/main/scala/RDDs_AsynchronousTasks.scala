import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Await
import scala.concurrent.duration.Duration


object RDDs_AsynchronousTasks {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)

    // Create SparkConfig object & SparkContext Object
    val conf = new SparkConf().setAppName("Asynchronous Tasks").setMaster("local[*]")
    val sc = new SparkContext(conf)

    /* Synchronous Mode (or Sequence mode)
    val rdd1 = sc.parallelize(List(1,2,3,4,5,6))
    val slowRDD = rdd1.collect().map(x => println("slowRDD, Element "+x))

    val rdd2 = sc.parallelize(List(10,20,30,40,50,60))
    val fastRDD = rdd2.collect().map(x => println("fastRDD, Element "+x))
    */

    // Asynchronous Mode
    val rdd1 = sc.parallelize(List(1,2,3,4,5,6))
    val slowRDD = rdd1.collectAsync().map{x => x.map{x => println("slowRDD, Element "+x) ; Thread.sleep(3000)}}

    val rdd2 = sc.parallelize(List(10,20,30,40,50,60))
    val fastRDD = rdd2.collectAsync().map{x => x.map{x => println("fastRDD, Element "+x)}}

    Await.result(slowRDD, Duration.Inf) // await for future actions related to slowRDD
  }
}
