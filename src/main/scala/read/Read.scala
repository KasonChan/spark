package read

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by kasonchan on 1/21/16.
  */
object Read {

  def main(args: Array[String]) {
    val listings = "resources/data/listings.txt"
    val products = "resources/data/products.txt"

    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val readListings = sc.textFile(listings, 2).cache()
    val readProducts = sc.textFile(products, 2).cache()

    println("TESTING")
    println("TESTING")
    println("TESTING")
    println("TESTING")

    readListings.foreach(println)
    readProducts.foreach(println)
  }

}
