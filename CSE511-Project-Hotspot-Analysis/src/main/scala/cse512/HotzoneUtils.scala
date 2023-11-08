package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {

    val queryRectangleArray = queryRectangle.split(",").map(_.toDouble);
    val pointStringArray = pointString.split(",").map(_.toDouble);
    val x1 = queryRectangleArray(0);
    val y1 = queryRectangleArray(1);
    val x2 = queryRectangleArray(2);
    val y2 = queryRectangleArray(3);
    val xValues = List(x1,x2);
    val yValues = List(y1,y2);
    val x = pointStringArray(0);
    val y = pointStringArray(1);
    if (xValues.min <= x && x <= xValues.max && yValues.min <= y && y <= yValues.max)
    {
      true
    }
    else
    {
      false
    }
  }



}
