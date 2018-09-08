import java.util.regex.Pattern

import org.apache.spark.sql.SparkSession

object weatherDf {


  def findYear(lines: String): String = {

    val pattern = Pattern.compile("\\d{2}.\\d{2}.(\\d{4}).*")
    val year=lines
    var matching=pattern.matcher(year)
    if(matching.find()){
      return matching.group(1)
    }
    return null
  }

  def findDays(lines:String):String={

    val pattern = Pattern.compile("(\\d{2}).\\d{2}.\\d{4}.*")
    val day=lines
    var matching=pattern.matcher(day)
    if(matching.find()){
      return matching.group(1)
    }
    return null
  }


  def main(args: Array[String]): Unit = {
  val sparkSession=SparkSession.builder.appName("weather_report").master("local").getOrCreate()
  val sc=sparkSession.sparkContext
  val rdd=sc.textFile("F:\\sindhu\\datasets\\w_sample.txt")
    import sparkSession.implicits._


    val weatherdf=rdd.map(_.split("\t")).map(row=>
    try{
      weather_new(
findYear(row(0)).trim.toString,
        row(1).toFloat,
        row(2).toFloat,
        row(3).toFloat,
        row(4).toFloat,
        row(5).toFloat,
        row(6).toFloat,
        row(7).toFloat,
        row(8).toFloat,
        row(9).toFloat,
        row(10).toFloat,
        row(11).toDouble,
        row(12).toFloat,
        row(13).toFloat,
        row(14).toFloat
    )}
catch{
  case ex@(_:ArrayIndexOutOfBoundsException|_:NumberFormatException)=>
  weather_new(
    findYear(" "),
    -9999, -9999, -9999, -9999, -9999, -9999, -9999, -9999, -9999, -9999, -9999, -9999, -9999, -9999)
  }).toDF


    weatherdf.createOrReplaceTempView("WEATHER_REPORT")
    println("Maximum Temperature recorded per year")
     sparkSession.sql("Select DateTime,TdegC from WEATHER_REPORT ").groupBy("DateTime").max("TdegC").show()
    /*sparkSession.sql("select TpotK from WEATHER_REPORT ").groupBy().max().show()*/
    /*sparkSession.sql("select DateTime,rh from WEATHER_REPORT").groupBy().max().show()*/
     /*sparkSession.sql("select DateTime,rh from WEATHER_REPORT where rh > 96.0 ").show()*/
   /*sparkSession.sql("select DateTime ,TpotK,pmbar from WEATHER_REPORT where pmbar> 1.0").groupBy("DateTime").min("TpotK").show()
*/
  }
}

case class weather_new(DateTime:String,	pmbar:Float, TdegC:Float,	TpotK:Float,	TdewdegC:Float,	rh:Float,	VPmaxmbar:Float,	VPactmbar:Float,	VPdefmbar:Float,	sh:Float,	H2OC:Float,	rho:Double, wv:Float,	max_wv:Float,	wddeg:Float
)