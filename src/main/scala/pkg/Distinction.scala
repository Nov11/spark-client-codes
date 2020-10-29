package pkg

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Distinction {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(Distinction.toString)
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val data: RDD[String] = sc.textFile("data/textFile")
    data.distinct().foreach(println)
  }
}
