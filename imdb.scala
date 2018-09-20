
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql
object imdb {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("IMDB_dataframes").master("local").getOrCreate()
    val sc=sparkSession.sparkContext
    val rdd=sc.textFile("F:\\sindhu\\datasets\\imdb\\sample.txt")
     import sparkSession.implicits._
    /*rdd.map(line=>line.split("\t")(4)).foreach(println)*/


    val imdb=rdd.map(_.split("\t")).map(row=>
    try{
      IMDB1(
        row(0).trim.toInt,
        row(1),
        row(2),
        row(3),
        row(4),
        row(5),
        row(6).trim.toInt,
        row(7).trim.toInt,
        row(8).trim.toDouble,
        row(9).trim.toLong,
        row(10).trim.toDouble,
        row(11).toInt

      )}

    catch{
      case exp@(_:ArrayIndexOutOfBoundsException|_:NumberFormatException)=>
        IMDB1(
          -9999,
          " ",
          " ",
          " ",
          " ",
          " ",
          -9999,
          -9999,
          -9999,
          -9999,
          -9999,
          -9999
        )
    }
    ).filter(imdbdata=>imdbdata.rank!= -9999).toDF

    import sql.functions._

    val newDF=imdb
    newDF.createOrReplaceTempView("IMDB_TABLE")
    sparkSession.sql("Select year,rating,title from IMDB_TABLE where votes>50000").groupBy("year").max("rating").show()

  }

}
case class IMDB1(rank:Int,title:String,genre:String,description:String,director:String,actors:String,year:Int,runtime:Double,
                rating:Double,votes:Long,revenue:Double,metascore:Double)