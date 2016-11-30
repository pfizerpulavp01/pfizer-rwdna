package com.pfizer.hvad.dl.process
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._
import com.pfizer.hvad.dl.utils.Utilities

object DataIngestion {
  def main(args: Array[String]){
    
    val log4j = Logger.getLogger(getClass.getName)
    if (args.length != 4) {
      log4j.error("Invalid number of Arguments passed")
      log4j.error("Expected Input file path name as Argument(1)")
      log4j.error("Expected logging level as Argument(2)")
      log4j.error("Expected Output file path as Argument(3)")
      log4j.error("Expected config file path")
      System.exit(1)
    }

    val sparkconf = new SparkConf().setAppName("DATAINGEST")
    val sc = new SparkContext(sparkconf)
    sc.setLogLevel(args(1))
    val sqlcontext = new hive.HiveContext(sc)
    import sqlcontext.implicits._
    val infile : String = args(0).trim()
    val outfile : String = args(2).trim()
    val (accesskey : String, accessid :String) =Utilities.getConfigparams(args(3).trim())
    
    val outputfile = Utilities.readInput(sc,sqlcontext,infile,outfile,accesskey,accessid)
    
    outputfile.toDF().write.mode("overwrite").format("parquet").save(outfile)
    //outputfile.saveAsTextFile(outfile)
    
    
  }

}