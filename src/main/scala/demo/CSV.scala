package demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.{Failure, Success, Try}

/**
  * Created by kasonchan on 1/29/16.
  */
object CSV {

  def main(args: Array[String]) {
    Try {
      val csvPath = "resources/data/oak-water-potentials-simple.csv"

      val conf = new SparkConf().setAppName("CSV").setMaster("local[*]")
      val sc = new SparkContext(conf)

      val csvLines: RDD[String] = sc.textFile(csvPath)

      val csvLinesDropedHead: RDD[String] = csvLines
        .zipWithIndex()
        .filter(_._2 > 0)
        .map { case (line: String, index: Long) =>
          line
        }

      // Q1 How many different species are recorded in these data?
      // 5 species
      val speciesCount: Long = csvLinesDropedHead.map { l =>
        l.split(",")
      }.groupBy(_ (2))
        .distinct()
        .count()
      println(speciesCount)

      // Q2 Mid day water potential should always be at least as negative as pre-dawn
      // water potential. Are there any days and plants for which mid-day water
      // potential is higher than pre-dawn?
      // "08/25/12" "05/22/13" "04/10/11" "05/24/13" "05/23/13" "07/21/11"
      val dates = csvLinesDropedHead.map(l => l.split(","))
        .filter(x => !x(4).contains("NA") && !x(5).contains("NA") && x(5).toDouble > x(4).toDouble)
        .groupBy(y => y(3))
        .map(x => x._1)
      dates.foreach(println)

      // Q3 What is the lowest (most negative) mid-day water potential in this data set?
      // When and for which species was this value recorded?
      // The lowest mid-day water potential in this data set is -6.75, it is
      // recorded on "04/10/11" for "QUGR3 " sp.
      val lowestMdPSI = csvLinesDropedHead.map(l => l.split(","))
        .fold(Array("", "", "", "", "", "0", ""))((m, i) => if (!i(5).contains("NA") && i(5).toDouble < m(5).toDouble) i else m)
      println("The lowest mid-day water potential in this data set is " +
        lowestMdPSI(5) + ", it is recorded on " + lowestMdPSI(3) + " for " +
        lowestMdPSI(2) + " sp.")

      // Q4 For which year was the average mid day water potential lowest (most negative)?
      // 11
      val lowestMdPSIAverageYear = csvLinesDropedHead.map(l => l.split(","))
        .groupBy(_ (6))
        .map(i => (i._1, i._2.map(x => x(5))))
        .map(i => (i._1, i._2.filter(j => !j.contains("NA"))))
        .map(i => (i._1, i._2.map(j => j.toDouble)))
        .map(i => (i._1, i._2.sum / i._2.toList.length))
        .fold("", 0.0)((m, i) => if (i._2 < m._2) i else m)._1

      println(lowestMdPSIAverageYear)

      scala.io.StdIn.readLine() // For spark ui: http://localhost:4040/
    } match {
      case Success(s) =>
      case Failure(f) => println(f)
    }
  }

}
