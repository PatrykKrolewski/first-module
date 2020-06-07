package pl.krolewski.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import java.net.URL
import java.io._
import java.lang.Math._
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.isnan

object DataFramesPosts {
  
  case class Post(id:Int, postTypeId:Int, ownerId:Int, parentId:Int)
  
  def mapper(line:String): Post = { // function maps respective lines of Post.xml file into objects containing desired data
       val postTypeIdPat = "PostTypeId=\"([0-9]+)".r //regex patterns for desired values from Post.xml file
       val idPat = "Id=\"([0-9]+)".r
       val ownerIdPat = "OwnerUserId=\"([0-9]+)".r
       val parentIdPat = "ParentId=\"([0-9]+)".r
       
       if (postTypeIdPat.findAllIn(line).length != 0 & idPat.findAllIn(line).length != 0 & ownerIdPat.findAllIn(line).length != 0){
          val taggedNumberPat = postTypeIdPat.findFirstIn(line).get
          val postTypeId ="([0-9]+)".r.findFirstIn(taggedNumberPat).get.toInt
          
          val taggedIdPat = idPat.findFirstIn(line).get
          val id ="([0-9]+)".r.findFirstIn(taggedIdPat).get.toInt
          
          val taggedOwnerIdPat = ownerIdPat.findFirstIn(line).get
          val ownerId ="([0-9]+)".r.findFirstIn(taggedOwnerIdPat).get.toInt
          
          val parentId = 
            if(parentIdPat.findAllIn(line).length != 0) //questions doesn't have parent Id
              "([0-9]+)".r.findFirstIn(parentIdPat.findFirstIn(line).get).get.toInt
            else 0
          
          val person:Post = Post(id, postTypeId, ownerId, parentId)
        
          return person
       }
    val person:Post = Post(0, 0, 0, 0)   
    return person
  }
  

    
  def main(args: Array[String]) {

    //only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
    
    //This import and methods must be defined in Spark Session
    import spark.implicits._

    // questionAnswer - DataFrame with answer author id column and questions author id column
    // userLocation - DataFrame with user id column and user ISO-2 country column
    // returns DataFrame with ISO-2 country of answer column and ISO-2 country of question column
    def determineLocationsForUserIds(questionAnswer: DataFrame, userLocation: DataFrame) : DataFrame =  {
      questionAnswer.as("qa")
            .join(userLocation.as("answer_location"), $"qa.answer_owner_id" === $"answer_location.id")
            .join(userLocation.as("question_location"), $"qa.question_owner_id" === $"question_location.id")
            .select("answer_location.location", "question_location.location").toDF("answer_location", "question_location")
    }
    
    //answerQuestionLocations - DataFrame with ISO-2 country code of answer column and ISO-2 country code of question column
    //capitalsCoordinates - DataFrame with ISO-2 country code column and columns w with latitude and longitude of given country capital
    //returns DataFrame with global coordinates capital cities of input countries
    def coordinatesOfCapitals( answerQuestionLocations: DataFrame, capitalsCoordinates: DataFrame) : DataFrame = {
      answerQuestionLocations.as("aql")
                      .join(capitalsCoordinates.as("cda"), $"aql.answer_location" === $"cda.iso2", "inner" )
                      .join(capitalsCoordinates.as("cdq"), $"aql.question_location" === $"cdq.iso2" )
                      .sort("aql.answer_location", "aql.question_location")
                      .select("cda.lat", "cdq.lat", "cda.lng", "cdq.lng").toDF("lat1","lat2", "lng1", "lng2")
    }
    
    // geoCoordinates - global coordinates of two points on the globe
    // returns distance between these 2 points
    def orthodromeDiststance(geoCoordinates: DataFrame) : DataFrame= {
      val radius =6371//earth radius in km
      geoCoordinates.select("lat1","lat2", "lng1", "lng2")
                  .withColumn("lt1", $"lat1".cast(DoubleType)* PI/180)// conversion from degrees to radians
                  .withColumn("lt2", $"lat2".cast(DoubleType)* PI/180)
                  .withColumn("ln2-ln1", $"lng2".cast(DoubleType)* PI/180-$"lng1".cast(DoubleType)* PI/180)
                  .withColumn("distance", 
                      org.apache.spark.sql.functions.acos(
                        org.apache.spark.sql.functions.sin("lt1")*org.apache.spark.sql.functions.sin("lt2")
                        +org.apache.spark.sql.functions.cos("lt1")*org.apache.spark.sql.functions.cos("lt2")
                        *org.apache.spark.sql.functions.cos(("ln2-ln1"))
                      )*radius )
    }
    
    
    // Posts data from Posts.xml file are loaded with mapper into DataSet
    val lines = spark.sparkContext.textFile("../Posts.xml")
    val posts = lines.map(mapper)
                      .filter(Post => Post.id != 0) // Lines of the file, which does not represent posts. e.g. xml header
                      .filter(Post => Post.postTypeId ==1 || Post.postTypeId == 2) // Other post types. Not described in https://ia800107.us.archive.org/27/items/stackexchange/readme.txt 
                      .toDS()
                      
    // DataFrame with answers and corresponding questions is obtained below. 
    // Every answer has one question. Question can have any number of answers, even no answer.
    val questionsAnswers = posts.as("answer")
                                    .join(posts.as("question"), $"answer.parentId" === $"question.id", "inner")
                                    .select("answer.ownerId","question.ownerId").toDF("answer_owner_id", "question_owner_id")
    questionsAnswers.show()
    questionsAnswers.cache()
    println(questionsAnswers.count())
    
    
    val questionsCrossAnswers = questionsAnswers.select("answer_owner_id").crossJoin(questionsAnswers.select("question_owner_id"))
     questionsCrossAnswers.show()
        println(questionsCrossAnswers.count())

    
    // File containing map of users'ids and their locations generated with python part of the solution is loaded below
    FileUtils.copyURLToFile(new URL("https://drive.google.com/u/0/uc?id=1-GsCZBPvai-m_QyY_CJovogdnYn0scla&export=download"), new File("../usersLocations.csv"))
    
    val usersLocations = spark.read
       .format("csv")
       .option("header", "true") //first line in file has headers
       .option("mode", "DROPMALFORMED")
       .load("../usersLocations.csv")
       .select("id", "location")

    usersLocations.show()
    
    
    // Below expression determines pairs of answer-question users location.
    // Pair is not taken into account when, at least one location in given pair is not determined.
    val answersQuestionsLocations = determineLocationsForUserIds(questionsAnswers, usersLocations)
    val answersCrossQuestionsLocations = determineLocationsForUserIds(questionsCrossAnswers, usersLocations)

        answersQuestionsLocations.cache()
        answersQuestionsLocations.show()
        
        answersCrossQuestionsLocations.cache()
        answersCrossQuestionsLocations.show()
        
    val capitalsCoordinates = spark.read
       .format("csv")
       .option("header", "true") //first line in file has headers
       .option("mode", "DROPMALFORMED")
       .load("../worldcities.csv")
       .select("iso2", "lat","lng", "capital")
       .as("cc") //without alias it doesn't work
       .filter($"cc.capital"==="primary")
       .dropDuplicates("iso2")
       .select("iso2", "lat","lng")
       //Some countries has more than one capital. "capital = primary" is not sufficient filtering e.q. Netherlands
        

    
    val answersQuestionsCoordinates = coordinatesOfCapitals(answersQuestionsLocations, capitalsCoordinates)
    val answersCrossQuestionsCoordinates = coordinatesOfCapitals(answersCrossQuestionsLocations, capitalsCoordinates)

    val answersQuestionsDistances = orthodromeDiststance(answersQuestionsCoordinates)
                          answersQuestionsDistances.cache()
                          answersQuestionsDistances.show()
                          println(answersQuestionsDistances.count())
                          
                          answersQuestionsDistances.filter(!isnan($"distance")) //There are 2 rows returning NaN distance
                          .select(org.apache.spark.sql.functions
                          .avg($"distance") )
                          .as("average_distance").cache().show()
                          
    val answersCrossQuestionsDistances = orthodromeDiststance(answersCrossQuestionsCoordinates)
                          answersCrossQuestionsDistances.cache()
                          answersCrossQuestionsDistances.show()
                          println(answersCrossQuestionsDistances.count())

    answersCrossQuestionsDistances.filter(!isnan($"distance"))
                          .select(org.apache.spark.sql.functions
                          .avg($"distance") )
                          .as("average_distance").cache().show()

    spark.stop()
  }
}