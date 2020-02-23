import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object RDDs_filter {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create SparkContext Object
    val conf = new SparkConf().setMaster("local").setAppName("Filter Function ")
    val sc = new SparkContext(conf)

    // Create an RDD from a collection
    val data = 1 to 100
    val dataRDD = sc.parallelize(data)

    // return/filter only the even values
    val result  = dataRDD.filter( _%2 == 0 )  // .filter(x => x%2==0)
    result.collect().foreach(println)

  }

}
