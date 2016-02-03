package demo

import java.io.{File, PrintWriter}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import play.api.libs.json._

import scala.util.{Failure, Success, Try}

/**
  * Created by kasonchan on 1/28/16.
  */
object Read {

  /**
    * Convert raw JSON string to JSON.
    *
    * @param rawJson raw JSON string
    */
  def toJson(rawJson: String): JsValue = {
    Json.parse(rawJson)
  }

  def main(args: Array[String]) {
    val writer = new PrintWriter(new File("results.txt"))

    Try {
      val productsPath = "resources/data/products.txt"
      val listingsPath = "resources/data/listings.txt"

      val conf = new SparkConf().setAppName("Read").setMaster("local[*]")
      val sc = new SparkContext(conf)

      val productsLines = sc.textFile(productsPath)
      val listingsLines = sc.textFile(listingsPath)

      val productsJson = productsLines.map(l => toJson(l))
      val listingsJson = listingsLines.map(l => toJson(l))

      val listingsJsonCollected = listingsJson.collect()

      val unions: RDD[(JsValue, Array[JsValue])] = productsJson.map { pj =>
        (pj, listingsJsonCollected.filter(l =>
          (// manufacturer, family, model in the title
            ((l \ "title").as[String].toLowerCase contains (pj \ "manufacturer").as[String].toLowerCase) &&
              ((l \ "title").as[String].toLowerCase contains (pj \ "family").asOpt[String].getOrElse("").toLowerCase) &&
              ((l \ "title").as[String].toLowerCase contains (pj \ "model").as[String].toLowerCase) &&
              (pj \ "manufacturer").as[String].toLowerCase == (l \ "manufacturer").as[String].toLowerCase
            ) || (// manufacturer, model in the title
            ((l \ "title").as[String].toLowerCase contains (pj \ "manufacturer").as[String].toLowerCase) &&
              ((l \ "title").as[String].toLowerCase contains (pj \ "model").as[String].toLowerCase) &&
              (pj \ "manufacturer").as[String].toLowerCase == (l \ "manufacturer").as[String].toLowerCase
            )
        ))
      }

      val unionsColleected: Array[(JsValue, Array[JsValue])] = unions.collect()

      val result = unionsColleected.map {
        case (p: JsValue, l: Array[JsValue]) =>
          Json.obj("product_name" -> (p \ "product_name").as[String],
            "listings" -> l)
      }.mkString("\n")

      println(result)
      writer.write(result)
    } match {
      case Success(s) =>
      case Failure(f) => println(f)
    }

    writer.close()

    scala.io.StdIn.readLine() // For spark ui: http://localhost:4040/
  }

}
