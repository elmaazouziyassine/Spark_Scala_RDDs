import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object RDDs_map {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create SparkContext Object
    val conf = new SparkConf().setMaster("local").setAppName("Map Function ")
    val sc = new SparkContext(conf)

    // Create an RDD from a collection
    val data = List(1, 2, 3, 4, 5)
    val dataRDD = sc.parallelize(data)

    // create a multiplyBy2 function
    val multiplyBy2 = (x :Int)  => (2*x)

    // apply multiplyBy2 function on each element of the dataRDD
    val result  = dataRDD.map(multiplyBy2)
    result.collect().foreach(println)

  }

}
