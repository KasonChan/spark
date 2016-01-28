package demo

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.{Failure, Success, Try}

/**
  * Created by kasonchan on 6/22/15.
  */
object Demo {

  def main(args: Array[String]) {
    Try {
      val logFile = "/Users/kasonchan/IdeaProjects/Spark/README.md" // Should be some file on your system
      val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]")
      val sc = new SparkContext(conf)
      val logData = sc.textFile(logFile, 2).cache()
      val numAs = logData.filter(line => line.contains("a")).count()
      val numBs = logData.filter(line => line.contains("b")).count()
      println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
    } match {
      case Success(s) =>
      case Failure(f) => println(f)
    }
  }

}
