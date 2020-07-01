import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Assignment {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().
      master("local[2]").
      appName("Assignment").getOrCreate()
    import spark.implicits._

    val sessionDF = spark.read.json("/src/main/resources/assignment")
    //sessionDF.printSchema()  // check the data elements
    sessionDF.select("calc_userid", "eventlaenc", "timestamp").where(
      "calc_userid = 913"
    ).orderBy("timestamp").show(30,false) // sample data

    /*
      Question 1: Find the average session duration - ignore appnameenc = 0

       sessionDF.select("appnameenc").distinct().show() =>  0, 1, 2
       filteredDF.select("appnameenc").distinct().show() // 1,2

      */

    val filteredDF = sessionDF.filter(col("appnameenc") === 1 or col("appnameenc") === 2)
    val avgSessionTime = filteredDF.groupBy("appnameenc").avg("sessionid")
    //avgSessionTime.show(100, false)

    // Question2: Count of "calc_userid" for each "region". ignore "-" and nulls

    val df2 = sessionDF.filter('region =!= '-' and col("region").isNotNull).select("calc_userid", "region")
    val userCountperRegion = df2.groupBy("region").count()
    //userCountperRegion.show(1000)

    // Question 3: Consider "eventlaenc" =126 or 107 as defining actions. Calculate first and second defining action, ordered based on time, for each "calc_userid" and also find the count of those actions.


    //val sessionDF = spark.read.json("/Users/313248/IdeaProjects/spark-test/src/main/resources/assignment")
    val eventDF = sessionDF.filter(col("eventlaenc") === 126 or col("eventlaenc") === 107).select("calc_userid", "eventlaenc", "timestamp")

    val rankSpec = Window.partitionBy("calc_userid", "eventlaenc").orderBy("timestamp")
    val countSpec = Window.partitionBy("calc_userid", "eventlaenc")
    val leadSpec = Window.partitionBy('calc_userid).orderBy("timestamp")
    val rowNumberSpec = row_number() over Window.partitionBy('calc_userid).orderBy("timestamp")


    val actionDF = eventDF.withColumn("rank", rank().over(rankSpec))
      .withColumn("count", count("eventlaenc").over(countSpec))
      .where("rank = 1")
      .withColumn("second_action", lead('eventlaenc, 1) over leadSpec)
      .withColumn("second_action_count", lead('count, 1) over leadSpec)
      .withColumn("row_number", rowNumberSpec)
      .where("row_number = 1"). //.where("row_number = 1 and calc_userid =913"). tested with user = 913
      select(col("calc_userid"),
      col("eventlaenc").as("first_action"),
      col("count").as("first_action_count"),
      col("second_action"),
      col("second_action_count"))

    actionDF.where("calc_userid = 913").show(1000,false)
  }
}
