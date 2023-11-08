package cse512

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App{

  def ST_Contains(queryRectangle:String, pointString:String): Boolean = {
    //check if inputs are empty or not
    if (queryRectangle.length == 0 || pointString.length == 0) {
      return false
    }

    //split queryRectangle string and convert into double
    val rect_array = queryRectangle.split(",")
    //check if it contains 4 coordinates
    if (rect_array.length != 4) {
      return false
    }
    val r_x1 = rect_array(0).trim().toDouble
    val r_y1 = rect_array(1).trim().toDouble
    val r_x2 = rect_array(2).trim().toDouble
    val r_y2 = rect_array(3).trim().toDouble

    //split pointString string and convert into double
    val point_array = pointString.split(",")
    //check if it contains 2 coordinates
    if (point_array.length != 2) {
      return false
    }
    val p_x = point_array(0).trim().toDouble
    val p_y = point_array(1).trim().toDouble

    if (p_x >= r_x1 && p_x <= r_x2 && p_y >= r_y1 && p_y <= r_y2) {
      return true
    }
    else if (p_x >= r_x2 && p_x <= r_x1 && p_y >= r_y2 && p_y <= r_y1) {
      return true
    }
    else {
      return false
    }

    /*//calculate the boundaries of rectangle
    val lower_x = math.min(r_x1, r_x2)
    val higher_x = math.max(r_x1, r_x2)
    val lower_y = math.min(r_y1, r_y2)
    val higher_y = math.max(r_y1, r_y2)

    if (p_x >= lower_x && p_x <= higher_x && p_y >= lower_y && p_y <= higher_y) {
      return true
    }
    else {
      return false
    }*/
  }


  def ST_Within (pointString1:String, pointString2:String, distance:Double): Boolean = {
    //check if inputs are empty or not
    if (pointString1.length == 0 || pointString2.length == 0) {
      return false
    }

    //split 2 pointString strings and convert into double
    val point1_array = pointString1.split(",")
    //check if it contains 2 coordinates
    if (point1_array.length != 2) {
      return false
    }
    val p1_x = point1_array(0).trim().toDouble
    val p1_y = point1_array(1).trim().toDouble

    val point2_array = pointString2.split(",")
    //check if it contains 2 coordinates
    if (point2_array.length != 2) {
      return false
    }
    val p2_x = point2_array(0).trim().toDouble
    val p2_y = point2_array(1).trim().toDouble

    //calculate Euclidean distance using formula: d=sqrt((𝑥2−𝑥1)^2+(𝑦2−𝑦1)^2)
    val d = math.sqrt(math.pow((p2_x - p1_x), 2) + math.pow((p2_y - p1_y), 2))

    if (d <= distance) {
      return true
    }
    else {
      return false
    }
  }


  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((ST_Contains(queryRectangle, pointString))))

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((ST_Contains(queryRectangle, pointString))))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((ST_Within(pointString1, pointString2, distance))))

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((ST_Within(pointString1, pointString2, distance))))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }
}
