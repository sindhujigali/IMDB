import org.apache.spark.sql.SparkSession

object imdb {

  val RANK = 0
  val TITLE = 1
  val GENRE = 2
  val DESCRIPTION = 3
  val DIRECTOR = 4
  val ACTORS = 5
  val YEAR = 6
  val RUNTIME = 7
  val RATING = 8
  val VOTES = 9
  val REVENUE = 10
  val METASCORE = 11


  def main(args: Array[String]): Unit = {


    val sparkSession = SparkSession.builder().appName("IMDB").master("local").getOrCreate()
    val sc = sparkSession.sparkContext

    val rdd = sc.textFile("F:\\sindhu\\datasets\\imdb\\sample.txt")
    rdd.map(line => (line.split("\t")(6), line)).
      groupByKey().
      map(getmovie => (getmovie._1, getMovieName(getmovie._2.toArray))).
      foreach(println)
  }

  def getMovieName(listofMovies: Array[String]): String = {
    var max_rating = 0.0
    var max_votes = 0
    var best_movie=" "

    listofMovies.foreach(line => {
      try {

        val rating = line.split("\t")(8)
        val votes = line.split("\t")(9)
        var movie=line.split("\t")(1)

        if (max_rating < rating.toFloat) {
          max_rating = rating.toFloat
           best_movie=movie
        }
        else if (max_rating == rating.toFloat) {
          if (max_votes < votes.toInt)
            max_votes = votes.toInt
             best_movie=movie

        }
      } catch {
        case e: NumberFormatException =>
          println("...")

      }

    })
 best_movie
  }
}