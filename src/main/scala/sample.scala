import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object sample {


  def main(args: Array[String]): Unit = {
       val spark =SparkSession.builder().master("local[2]").appName("test").getOrCreate()

    val sq = Seq((1,"ramesh"),(2,"mehesh"))
    val schema = StructType(Seq(StructField("SI NO",IntegerType, false),
      StructField("Name",StringType, false)))


  }
}
