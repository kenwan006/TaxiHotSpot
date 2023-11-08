package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{udf, _}
import org.apache.spark.sql.{DataFrame, SparkSession}


object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame = {
    // Load the original data from a data source
    var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter", ";").option("header", "false").load(pointPath);
    pickupInfo.createOrReplaceTempView("nyctaxitrips")
    pickupInfo.show()

    // Assign cell coordinates based on pickup points
    spark.udf.register("CalculateX", (pickupPoint: String) => ((
      HotcellUtils.CalculateCoordinate(pickupPoint, 0)
      )))
    spark.udf.register("CalculateY", (pickupPoint: String) => ((
      HotcellUtils.CalculateCoordinate(pickupPoint, 1)
      )))
    spark.udf.register("CalculateZ", (pickupTime: String) => ((
      HotcellUtils.CalculateCoordinate(pickupTime, 2)
      )))
    pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
    var newCoordinateName = Seq("x", "y", "z")
    pickupInfo = pickupInfo.toDF(newCoordinateName: _*)
    pickupInfo.show()

    // Define the min and max of x, y, z
    val minX = -74.50 / HotcellUtils.coordinateStep
    val maxX = -73.70 / HotcellUtils.coordinateStep
    val minY = 40.50 / HotcellUtils.coordinateStep
    val maxY = 40.90 / HotcellUtils.coordinateStep
    val minZ = 1
    val maxZ = 31
    val numCells = (maxX - minX + 1) * (maxY - minY + 1) * (maxZ - minZ + 1)

    // YOU NEED TO CHANGE THIS PART

    // Create a new dataframe (x, y, z, count). The count is pickup# for each location per day
    pickupInfo = pickupInfo.select("x", "y", "z").where("x >= " + minX + " AND x <= " + maxX + " AND y >= " + minY + " AND y <= " + maxY + " AND z >= " + minZ + " And z <= " + maxZ)
    var pickup_df = pickupInfo.groupBy("x", "y", "z").count().withColumnRenamed("count", "pickups")

    // Calculate average pickups of all cells
    val avg_pickups = pickup_df.select("pickups").agg(sum("pickups")).first().getLong(0).toDouble / numCells.toDouble

    // Calculate standard deviation over all cells pickups
    var pickup_df1 = pickup_df.withColumn("power_pickups", col("pickups") * col("pickups"))
    val stdDev_pickups = scala.math.sqrt((pickup_df1.select("power_pickups").agg(sum("power_pickups")).first().getLong(0).toDouble / numCells) - avg_pickups * avg_pickups)

    // Sum up all neigbors pickups for each cell
    pickup_df.createOrReplaceTempView("cells")

    var sumNeighborsPickups = spark.sql("SELECT c1.x AS x, c1.y AS y, c1.z AS z, sum(c2.pickups) AS neighborsPickups FROM cells AS c1, cells AS c2 WHERE" +
    "(c1.x == c2.x OR c1.x == c2.x - 1 OR c1.x == c2.x + 1) AND (c1.y == c2.y OR c1.y == c2.y - 1 OR c1.y == c2.y + 1) AND (c1.z == c2.z OR c1.z == c2.z - 1 OR c1.z == c2.z + 1) " +
    "GROUP BY c1.z, c1.y, c1.x ")

    // Calculate neighbors number for each cell
    var CalculateNeighbors = udf((minX: Int, minY: Int, minZ: Int, maxX: Int, maxY: Int, maxZ: Int, X: Int, Y: Int, Z: Int) => HotcellUtils.CalculateNeighborsNumber(minX, minY, minZ, maxX, maxY, maxZ, X, Y, Z))
    var sumNeighborsNumber = sumNeighborsPickups.withColumn("neighborsNumber", CalculateNeighbors(lit(minX), lit(minY), lit(minZ), lit(maxX), lit(maxY), lit(maxZ), col("x"), col("y"), col("z")))

    // Calculte G-score for each cell
    var GscoreFunction = udf((x: Int, y: Int, z: Int, neighborsNumber: Int, neighborsPickups: Int, numCells: Int, avg: Double, stdDev: Double) => HotcellUtils.CalculateGscore(x, y, z,neighborsNumber, neighborsPickups, numCells, avg, stdDev))
    var Gscore_df = sumNeighborsNumber.withColumn("gscore", GscoreFunction(col("x"), col("y"), col("z"), col("neighborsNumber"), col("neighborsPickups"), lit(numCells), lit(avg_pickups), lit(stdDev_pickups)))
    pickupInfo = Gscore_df.select("x", "y", "z").orderBy(desc("gscore"), desc("x"), asc("y"), desc("z")).limit(50)
    return pickupInfo // YOU NEED TO CHANGE THIS PART
  }
}
