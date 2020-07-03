package com.hackerphysics.spark

import org.apache.spark.sql.SaveMode
import org.apache.log4j._
import org.apache.spark.sql.functions.{array, array_union, col, count, explode, lit, typedLit, when}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession, types}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.TABLE
import org.apache.spark
import org.apache.spark.sql.types.{StringType, StructField}

object Bucketing {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Bucketing Test")
      .getOrCreate()

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    //    hiveContext.sql("ALTER TABLE testC PARTITION (idP='1') SET LOCATION '/user/root/A/idp=1/' ")
    spark.sql(
      """
        |CREATE TABLE bucketed_4_10e6
        | (id bigint)
        |  USING PARQUET
        |  CLUSTERED BY (id) INTO 4 BUCKETS
        |  LOCATION 'D:/tmp/2/'
        |""".stripMargin)

    spark.sql(
      """
        |CREATE TABLE bucketed_4_10e4
        |  (id bigint)
        |  USING PARQUET
        |  CLUSTERED BY (id) INTO 4 BUCKETS
        |  LOCATION 'D:/tmp/1/'
        |""".stripMargin)

    //    spark.sql("MSCK REPAIR TABLE bucketed_4_10e4")
    //    spark.sql("MSCK REPAIR TABLE bucketed_4_10e6")

    spark.sql("DESCRIBE EXTENDED bucketed_4_10e4").show(numRows = 21, truncate = false)
    spark.sql("DESCRIBE EXTENDED bucketed_4_10e6").show(numRows = 21, truncate = false)

    val bucketed_4_10e4 = spark.table("bucketed_4_10e4")
    val bucketed_4_10e6 = spark.table("bucketed_4_10e6")

    println("numPartitions of bucketed_4_10e4: ",bucketed_4_10e4.queryExecution.toRdd.getNumPartitions)
    println("numPartitions of bucketed_4_10e6: ",bucketed_4_10e6.queryExecution.toRdd.getNumPartitions)


    // trigger execution of the join query
    //r = a.join(b, a.col("id")<=>b.col("id"))
    //r = a.join(b, "id")

    val  r1 = bucketed_4_10e4.join(bucketed_4_10e6, bucketed_4_10e4.col("id")===bucketed_4_10e6.col("id"))
      .drop( bucketed_4_10e4.col("id"))
    println("numPartitions of r1: ",r1.queryExecution.toRdd.getNumPartitions)
    r1.explain()
    r1.show()

  }

}
