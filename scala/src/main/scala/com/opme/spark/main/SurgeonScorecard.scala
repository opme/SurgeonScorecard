package com.opme.spark.main

import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.apache.spark.sql.SQLContext
import java.util._
import scala.io.Source
import com.typesafe.config._
import com.opme.spark.model.Model
import com.opme.spark.utils.Utils
import com.opme.spark.cohort.Cohort
import com.opme.spark.readmission.Readmission

/** run surgeon scorecard on csv data */
object SurgeonScorecard {
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {

    val config = ConfigFactory.load()
    val env = "scorecard"
    val datadir = config.getString(env + ".data")
    val resultdir = config.getString(env + ".readmission")
    val driver_memory = config.getString(env + ".driver_memory")
    val shuffle_partitions = config.getString(env + ".shuffle_partitions")
	
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    // Create a SparkContext using every core of the local machine, named SurgeonScorecard
    val spark = SparkSession
     .builder()
     .appName("SurgeonScorecard")
     .getOrCreate()
	 
    // read in the data
    var data = Utils.loadRawData(spark, datadir)

	// create a cohort of users and their associated data
    // The cohort and event data is filtered based on properties
    val cohort = new Cohort(spark, data, config)
    println("Number of patients in cohort: ")
	
    // Find readmission events for this cohort for procedures of interest
    var readmit = new Readmission(spark, data, config)

    // write the results to csv files
	for ((key, value) <- readmit.providerProcedureInfoDfs) {
        println("Writing provider data for: " + key)
        val filename = key + ".csv"
        Utils.saveDataframeAsSingleFile(value, resultdir, filename)
    }
	
    // calculate statistics related to codes
    // write a file with counts of all diagnostic icd codes
    readmit.writeCodesAndCount(readmit.diagnostic_codes, resultdir, "procedure_count_all.txt", false)
    // write a file with counts of all diagnostic icd codes where the code is primary
    readmit.writeCodesAndCount(readmit.diagnostic_codes, resultdir, "procedure_count_primary.txt", true)
    // write a file with counts of all readmission icd codes
    readmit.writeCodesAndCount(readmit.readmission_codes, resultdir, "readmission_count_all.txt", false)
    // write a file with counts of all readmission icd codes where the code is primary
    readmit.writeCodesAndCount(readmit.readmission_codes, resultdir, "readmission_count_primary.txt", true)
    // write a file with counts of readmission events by code
    readmit.writeReadmissionCodesAndCount( 
        readmit.readmission_codes, 
        readmit.readmissionDfs, 
        resultdir, 
        "readmission_event_code_count.txt")

  }
}
