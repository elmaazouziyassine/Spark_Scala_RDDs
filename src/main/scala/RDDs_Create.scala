import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object RDDs_Create {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)

    // Create SparkConfig object & SparkContext Object
    val conf = new SparkConf().setMaster("local").setAppName("Creating RDDS")
    val sc = new SparkContext(conf)

    // Create an RDD from a collection (list, array, ...)
    val data = List(1, 2, 3, 4, 5)
    val dataRDD = sc.parallelize(data)
    dataRDD.collect().foreach(println)
    println("the sum of elements is " + dataRDD.reduce((a,b) => a+b))   // or use dataRDD.reduce(_+_)

    // Create an RDD from an external dataset
    val fileRDD = sc.textFile("src/datasets/data1")
    fileRDD.collect().foreach(println)

    val lengths = fileRDD.map(x => x.length)
    lengths.collect().foreach(println)
    println(lengths.reduce(_+_))

    //Create an RDD from an existing RDD
    val cubeData = dataRDD.map(x => x*x*x)
    cubeData.collect().foreach(println)

    // Read a directory containing multiple text files
    val myDirectoryRDD = sc.wholeTextFiles("src/datasets")
    myDirectoryRDD.collect().foreach(println)
  }
}
