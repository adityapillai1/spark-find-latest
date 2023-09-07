package com.adityaspark4

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory
import java.sql.Timestamp

import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.expressions.Window

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.json4s._
import org.json4s.jackson.JsonMethods._

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import scala.io.Source


object latest {

private val logger = LoggerFactory.getLogger(getClass)
case class Config(cei_code: Long, p_key: String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("latest")
      .getOrCreate()

    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    val inputPath = args(0)
    val outputPath = args(1)
    val configPath = args(2)
    val epoch = (args(3).toLong)/1000

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val inputStream = fs.open(new Path(configPath))
    val jsonString = Source.fromInputStream(inputStream).mkString

    import spark.implicits._
    implicit val formats = DefaultFormats
  

    val timestamp = new Timestamp(epoch)
    println(timestamp)

     // Read the CSV file
     val df = try {
      spark.read.option("header", "true").csv(inputPath)
    } catch {
      case _:Throwable=> logger.warn("Invalid file path"); spark.emptyDataFrame
        // org.apache.spark.sql.AnalysisException => Seq.empty[(String, String, String, String, String)].toDF("cei_code", "d_name", "u_name", "cei_status", "updated_at")
    }

     val filteredDF = df
     .withColumn("epoch_val", lit(epoch))
    .filter($"updated_at" <= to_timestamp(col("epoch_val")))

    filteredDF.show()
    

    val confList = parse(jsonString).extract[List[JObject]].map {
      jsonObject =>

      val ccode = (jsonObject \ "cei_code").extract[Int]
      val pk = (jsonObject \ "p_key").extract[String]

      Config(ccode, pk)

    }

 

    confList.foreach {  config=>

      val p_key = config.p_key
      val cei_code = config.cei_code

      val wSpec = Window.partitionBy(p_key).orderBy($"updated_at".desc)

      val finaldf = filteredDF.where(col("cei_code") === cei_code).withColumn("Rownum", row_number().over(wSpec)) 

      val rowno1DF = finaldf.filter(col("Rownum") === 1).withColumn("updated_at",lit(timestamp)).drop("p_key","Rownum","epoch_val")

 

      rowno1DF.show() 

      rowno1DF.write.option("header","true").partitionBy("updated_at","cei_code").mode("overwrite").csv(outputPath)

     

    }

    spark.stop()
  }

}