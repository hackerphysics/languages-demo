package com.hackerphysics.spark

package org.bingviz.test
import org.apache.spark.sql.SaveMode
import org.apache
import org.apache.log4j._
import org.apache.spark.sql.functions.{array, array_union, col, count, explode, lit, typedLit, when}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession, types}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark
import org.apache.spark.sql.types.{StringType, StructField}

object BucketTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("BingViz UU Metrics")
      .getOrCreate()

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    // Create bucketed tables
    spark.range(10e4.toLong)
      .coalesce(1)
      .write
      .format("parquet")
      .bucketBy(4, "id")
      .sortBy("id")
      .option("path","D:/tmp/1")
      .mode(SaveMode.Overwrite)
      .saveAsTable("bucketed_4_10e4")

    spark.range(10e5.toLong)
      .coalesce(1)
      .write
      .format("parquet")
      .bucketBy(4, "id")
      .sortBy("id")
      .mode(SaveMode.Overwrite)
      .option("path","D:/tmp/2")
      .saveAsTable("bucketed_4_10e6")

    val df1 = spark.read.parquet("D:/tmp/1")
    val df2 = spark.read.parquet("D:/tmp/2")
    spark.read.

      //    val df1 = spark.table("D:/tmp/1")
      //    val df2 = spark.table("D:/tmp/2")

      println("numPartitions of df1: ",df1.queryExecution.toRdd.getNumPartitions)
    println("numPartitions of df2: ",df2.queryExecution.toRdd.getNumPartitions)
    df1.createOrReplaceTempView("df1")
    df2.createOrReplaceTempView("df2")
    spark.sql("DESCRIBE EXTENDED df1").show(numRows = 21, truncate = false)
    spark.sql("DESCRIBE EXTENDED df2").show(numRows = 21, truncate = false)

    val rr = spark.sql(
      """
    SELECT
    df1.id,
    df2.id AS id2
    FROM df1
    LEFT JOIN df2
    ON
      df1.id == df2.id
    """)
    rr.explain()
    println("numPartitions of rr: ",rr.queryExecution.toRdd.getNumPartitions)

    return

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
    r1.cache()

    // trigger execution of the join query
    val  r11 = bucketed_4_10e4.join(bucketed_4_10e6, "id")
    println("numPartitions of r11: ",r11.queryExecution.toRdd.getNumPartitions)
    r11.explain()
    //    r1.show()

    val r2 = spark.sql(
      """
    SELECT
    bucketed_4_10e4.id,
    bucketed_4_10e6.id AS id2
    FROM bucketed_4_10e4
    LEFT JOIN bucketed_4_10e6
    ON
      bucketed_4_10e4.id == bucketed_4_10e6.id
    """)

    println("numPartitions of r2: ",r2.queryExecution.toRdd.getNumPartitions)
    r2.explain()
    r2.show()
    r2.createOrReplaceTempView("temp")
    spark.sql("DESCRIBE EXTENDED bucketed_4_10e4").show(numRows = 21, truncate = false)
    spark.sql("DESCRIBE EXTENDED bucketed_4_10e4").show(numRows = 21, truncate = false)
    spark.sql("DESCRIBE EXTENDED temp").show(numRows = 21, truncate = false)
    r2.cache()

    val r3 = spark.sql(
      """
    SELECT
    bucketed_4_10e4.*,
    bucketed_4_10e6.*
    FROM bucketed_4_10e4
    LEFT JOIN bucketed_4_10e6
    ON
      bucketed_4_10e4.id <=> bucketed_4_10e6.id
    """)

    println("numPartitions of r3: ",r3.queryExecution.toRdd.getNumPartitions)
    r3.explain()


    //    ANALYZE TABLE students COMPUTE STATISTICS NOSCAN;

    val  r21 = r1.join(r2, "id")
    println("numPartitions of r21: ",r21.queryExecution.toRdd.getNumPartitions)
    r21.explain()

    r1.createOrReplaceTempView("r1")
    r2.createOrReplaceTempView("r2")

    val r22 = spark.sql(
      """
    SELECT
    r1.id,
    r2.id AS id2
    FROM r1
    LEFT JOIN r2
    ON
      r1.id == r2.id
    """)
    r22.explain()

  }

}
