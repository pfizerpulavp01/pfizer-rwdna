package com.pfizer.hvad.dl.utils

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.DataFrame
import org.apache.derby.iapi.store.raw.data.DataFactory
import org.apache.spark.sql.DataFrame
import java.io.File
import java.io.FileInputStream
import play.api.libs.json._
import play.api.libs.functional.syntax._
import play.api.libs.iteratee._
import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD



object Utilities {

  val log4j = Logger.getLogger(getClass.getName)
  def getAllcolumnsData(sqlcontext: SQLContext, tablename: String, Query: String): DataFrame = {

    val tabledata = sqlcontext.sql("select * from " + tablename.trim() + " where " + Query.trim())
    tabledata
  }

  def checkHeader(header_check: DataFrame, inputfile: DataFrame, fileid: String) {
    val chck_flg = header_check.select(header_check("check_enable_flag")).first()
    val action = header_check.select(header_check("result_action")).toString()
    if (chck_flg(0).equals("Y")) {
      val header_expected = header_check.select(header_check("check_value")).first()(0).toString()
      println("header expected" + header_expected)
      val header_fromFile = inputfile.first().toString()
      println("header from file" + header_fromFile)
      if (header_expected.trim().equalsIgnoreCase(header_fromFile.trim())) {
        log4j.warn("header check successfull for fileid" + fileid)
      } else {
        if (action == "FAIL") {
          log4j.error("header check failed for fileid" + fileid)
          System.exit(1)
        } else {
          log4j.error("header check force passed for fileid" + fileid)
        }
      }

    } else {
      log4j.warn(" header Check disabled for fileid" + fileid)
    }
  }

  def countCheck(count_check: DataFrame, inputfile: DataFrame, fileid: String) {
    val chck_flg = count_check.select(count_check("check_enable_flag")).first()
    val check_value = count_check.select(count_check("check_value")).toString()
    val action = count_check.select(count_check("result_action")).toString()
    println(chck_flg);
    if (chck_flg(0).equals("Y")) {
      val count = inputfile.count()
      log4j.warn("Record count for fileid" + fileid + " " + count.toString())
      if (count == 0 && check_value.equals("NOT EQUAL TO 0")) {
        if (action == "FAIL") {
          log4j.error("File empty check failed for fileid" + fileid)
          System.exit(1)
        } else {
          log4j.error("File empty check force passed for fileid" + fileid)
        }
      } else {
        log4j.warn("File empty check successfull for fileid" + fileid)
      }

    } else {
      log4j.warn(" File empty Check disabled for fileid" + fileid)
    }
  }
  def getConfigparams(configfileloc: String) = {

    val file=new File(configfileloc)
    val stream = new FileInputStream(file)
    val json = Json.parse(stream)
    val accessKey :String  =(json \ "AWS access params" \ "S3AccessKey").get.toString()
    val accessid :String = (json \ "AWS access params" \ "S3Keyid").get.toString()
    println(accessid + accessKey)
    (accessKey, accessid)
  }
  def readInput(sc :SparkContext,sqlcontext : SQLContext,infile : String,outfile:String,key:String,id:String) : RDD[String]={
    
    val id1:String = id.replaceAll("\"", "")
    val key1:String = key.replaceAll("\"", "")
    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", id1)
    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", key1)
   
    println("id" + id1)
    println("key" +key1)
    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.split.minsize","512000000")
    sc.hadoopConfiguration.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    sqlcontext.setConf("spark.sql.parquet.compression.codec", "snappy")
    log4j.warn(sc.getConf.getAll.mkString("\n"))
    //val repositry_prefix = "s3n://" + id+key
    
    //val input=sc.textFile("s3n://AKIAIEQL7TQCAAQQYGBQ:ibX8N798IV5jg6G+ojbfce2YA18//0t4s9FL5nOf@rwdna-poc/truven_incremental/mdcr*.gz")
    //val input=sc.textFile("s3n://AKIAIRC33AAQXFKA4HIA:DAdkPfnfbKJhPK3KfLs+nCVFRDqGSub6ZZfgaEsz@rwdna-poc/truven-annual/medstat-annual-full.txt")
    //println(repositry_prefix+infile)
    //val input=sc.textFile(repositry_prefix+infile)
    val input=sc.textFile(infile)
    input
   
    
    
    
    
  }
}