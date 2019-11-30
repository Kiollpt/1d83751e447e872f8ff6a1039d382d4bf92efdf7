package com.harry


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
import org.datasyslab.geosparksql.utils.Adapter


object spark_v3 {

  def main(args: Array[String]) {

    //val ss = SparkSession.builder.appName("Harry").config("spark.master","local[*]").getOrCreate()

    // deploy
    val ss = SparkSession.builder.appName("Harry").getOrCreate()

    val shapefileInputLocation = "s3://testhaha123/taxi_zones"
    val spatialrdd = ShapefileReader.readToGeometryRDD(ss.sparkContext, shapefileInputLocation)

    // Get shape DF
    val spatialDf = Adapter.toDf(spatialrdd, ss)


    // Load taxi data
    val df = ss.read.format("csv")
      .option("header", true).load("s3://testhaha123/yellow_tripdata_2019-0*.csv")


    // JOIN to get location name
    val jointype = "left_outer"
    val expr = df.col("PULocationID") === spatialDf.col("LocationID")
    val df2 = df.join(spatialDf, expr, jointype).withColumnRenamed("zone", "pickup zone")
    val processed_df = df2.select("pickup zone", "tpep_pickup_datetime",
      "tpep_dropoff_datetime"

    )


    //
    val num = processed_df.rdd.getNumPartitions
    //
    println(s"before shuffling, num is $num")
    //
    /* Region with high pick up / drop off */
    val processed_df1 = processed_df.groupBy("pickup zone")
      .agg(count("pickup zone")
        .alias("count"))
      .orderBy(desc("count"))


    processed_df1.show(10)

    val num1 = processed_df1.rdd.getNumPartitions
    println(s"After shuffling, num is $num1")


    /* peak time / off peak time */
    // to date

    //    val processed_df2 = processed_df.withColumn("pickup_hour",hour(col("tpep_pickup_datetime")))
    //      .withColumn("drop_hour",hour(col("tpep_dropoff_datetime")))
    ////
    //    val processed_df3  = processed_df2.groupBy("pickup_hour")
    //      .agg(count("pickup_hour")
    //        .alias("PU_count"))
    ////
    ////
    ////
    //    val peak = processed_df3.select(max("PU_count")).first().get(0)
    //    val off_peak = processed_df3.select(min("PU_count")).first().get(0)
    //
    //
    //    processed_df3.where(col("PU_count")=== peak).show()
    //
    //    processed_df3.where(col("PU_count")=== off_peak).show()

    // Thread.sleep(640000)


  }

}

