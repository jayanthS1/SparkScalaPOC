package com.broadridge.spark

//import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object LoadDataFromJSON {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[2]")
      .appName("LoadDatafromJSON use case")
      .getOrCreate()
    //Inside of sparksession, we have sparkSQL

    //DataType Declaration for dataframe
    val schema = StructType(Array(
      StructField("user_id", IntegerType, true),
      StructField("datetime", StringType, true),
      StructField("os", StringType, true),
      StructField("browser", StringType, true),
      StructField("response_time_ms", StringType, true),
      StructField("product", StringType, true),
      StructField("url", StringType, true)))

    val df_with_schema = spark.read.schema(schema).json("file:///C:/Sridhar/workspace/SparkScalaPOC/src/input/json/clickstream_data.json")

    df_with_schema.count

    df_with_schema.printSchema()
    //Schema of the dataframe

    df_with_schema.show(false)
    //it will print 20 rows as default

    df_with_schema.write.mode("overwrite").partitionBy("browser").bucketBy(5, "product").sortBy("user_id").saveAsTable("click_stream_data_in_Parquet")
    //Be default, it will store in the form of partquet format
    //No of folders will be created by browser column which is as partition
    //Inside of the each spark partition folder, will be created number of buckets
    //each bucketed file will be sorted by user_id's

    //df_with_schema.write.mode("overwrite").format("orc").partitionBy("browser").bucketBy(5, "product").sortBy("user_id").saveAsTable("click_stream_data_in_ORC")
    //The data will overwrite in the form of ORC format

    spark.sqlContext.sql("select * from click_stream_data_in_Parquet").show(false)

  }

}