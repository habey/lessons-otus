import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.json4s._
import org.json4s.jackson.JsonMethods._

object JsonReader extends App {
  case class User (id: Int,
                   country: String,
                   points: Int,
                   title: String,
                   price: Option[Double] = None,
                   variety: String,
                   winery: String)


  implicit val formats = DefaultFormats

  val conf = new SparkConf()
  conf.setMaster("local")
  conf.setAppName("Winemag JSON")
  conf.set("spark.testing.memory", "2147480000")
  val sc = new SparkContext(conf)

  val json_read = sc.textFile("winemag-data-130k-v2.json")
  val result = json_read.map(x => parse(x).extract[User])
  result.take(25).foreach(println)
}
