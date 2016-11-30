package com.pfizer.hvad.dl.process
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._
import com.pfizer.hvad.dl.utils.Utilities

object DqChecks {
  def main(args: Array[String]) {
    val log4j = Logger.getLogger(getClass.getName)
    if (args.length != 3) {
      log4j.error("Invalid number of Arguments passed")
      log4j.error("Expected file name as Argument(1)")
      log4j.error("Expected logging level as Argument(2)")
      log4j.error("Expected fileid as Argument(3)")
      System.exit(1)
    }

    val sparkconf = new SparkConf().setAppName("DQCHCK")
    val sc = new SparkContext(sparkconf)
    sc.setLogLevel(args(1))
    val sqlcontext = new hive.HiveContext(sc)
    import sqlcontext.implicits._
    val Inputfile = sc.textFile(args(0)).toDF()
    log4j.warn("process started")
    sqlcontext.sql("use rwdna_stg_test");
    val table_name = "truven_control_t_prasad_1";
    val Query = "file_id=" + "\"" + args(2).toString().trim() + "\""
    println(Query);
    val control_t_checks = Utilities.getAllcolumnsData(sqlcontext, table_name, Query)
    val header_check = control_t_checks.where(control_t_checks("check_nm") === "HEADER_CHECK")
    val datacnt_check = control_t_checks.where(control_t_checks("check_nm") === "DATACOUNT_CHECK")
    Utilities.checkHeader(header_check, Inputfile, args(2).trim())
    Utilities.countCheck(datacnt_check, Inputfile, args(2).trim())

  }
}