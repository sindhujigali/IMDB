package main.java

import org.apache.spark.rdd
import org.apache.spark.sql.SparkSession
import java.util.regex
import java.util.regex.Pattern


object Weather {


  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder.appName("Weather_Status").master("local").getOrCreate()
    val sc = sparkSession.sparkContext
    val rdd = sc.textFile("F:\\sindhu\\datasets\\w_sample.txt")

    val max_temp = rdd.map(line => line.split("\t")).map(temp => (temp(3).toDouble))
     println("Maximum Temperature Recorded " + max_temp.max())

    println("Maximum temperature per year")
    rdd.map(line=>line.split("\t")).map(line=>(findYear(line(0)),line(3))).groupByKey().
    map(line=>(line._1,line._2.toArray.max)).groupByKey().foreach(println)

    val humidity = rdd.map(line => line.split("\t")).map(line => line(5).toFloat)
    println("Highest relative humidity is " + humidity.max())


    rdd.map(line=>line.split("\t")).
      map(line=>(findDays(line(0)),line(5))).
      filter(line=>if(line._2.toString.toFloat>96) true else false).
     foreach(println)

    val Max_Humidity=rdd.map(line=>line.split("\t")).//method 1
      map(line=>(findYear(line(0)),line(5)))
      .map(line=>(maxhumi(line._2),line._1)).max()
    println("Year that has Maximum Humidity"+Max_Humidity)


     println(rdd.map(line=>line.split("\t")).//method 2
     map(line=>(findYear(line(0)),line(5))).
     map(line=>(maxhumi(line._2),line._1)).max())


    rdd.map(line=>line.split("\t")).map(line=>(findDays(line(0)),line(3),line(6))).
    filter(line=>if(line._3>1.toString)true else false).
    map(line=>(line._1,line._2)).groupByKey().map(line=>(line._1,line._2.min)).
    foreach(println)

  }


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
  def maxhumi(data: String): String= {
    var max_humidity = 0d
    var humidity = data.toDouble
    if (max_humidity <= humidity) {
      max_humidity = humidity
      return max_humidity.toString
    }
    return null
  }

}

