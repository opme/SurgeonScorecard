package com.opme.spark.readmission

import util.control.Breaks._
import com.typesafe.config._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import com.opme.spark.utils.Utils

class Readmission(spark: SparkSession, data: collection.mutable.Map[String, DataFrame], config: Config) {
    //
    // Detect readmission given a set of data in OMOP format
    //
    val env = "scorecard"
    val date_input_format = config.getString(env + ".data")
    val readmission_days = config.getString(env + ".readmission")
    val readmission_code_file = config.getString(env + ".readmission")
    val diagnostic_code_file = config.getString(env + ".readmission")
    val comorbidies_code_file = config.getString(env + ".readmission")
    val readmission_codes = getReadmissionCodes(readmission_code_file)
    val diagnostic_codes = getDiagnosticCodes(diagnostic_code_file)
    val comorbitiy_codes = getComorbityCodes(comorbidies_code_file)
    val icd_diagnosis = config.getString(env + ".readmission")
    val icd_readmission = config.getString(env + ".readmission")
    val inpatient_condition_primary_diagnosis = config.getString(env + ".readmission").split(',').toList
    val inpatient_procedure_primary_diagnosis =  config.getString(env + ".readmission").split(',').toList
//(filter_care_sites.split(',')).toList

    // find readmission patients based on criteria in the properties file
    var (readmissionDfs,providerProcedureInfoDfs,deaths) = readmissionPatients(data, config, diagnostic_codes, readmission_codes)

    //
    // read diagnostic codes from a file into a dictionary
    // 
    def getDiagnosticCodes(filename: String) : scala.collection.immutable.Map[String,List[String]] = {
       val codes = Utils.readFileToDict(filename)
       codes
	}

    //
    // read readmission codes from a file into a dictionary
    // 
    def getReadmissionCodes(filename: String) : scala.collection.immutable.Map[String,List[String]] = {
       val codes = Utils.readFileToDict(filename)
       codes
    }
	
    //
    // read readmission codes from a file into a dictionary
    //
    def getComorbityCodes(filename: String) : scala.collection.immutable.Map[String,List[String]] = {
       val codes = Utils.readFileToDict(filename)
       codes
	}   

    //
    // find patients that have been readmitted and return
    // a parallel data structure that contains information
    // on only these individuals
    //
    def readmissionPatients(data: collection.mutable.Map[String, DataFrame], config: Config, diagnostic_codes: scala.collection.immutable.Map[String,List[String]], readmission_codes: scala.collection.immutable.Map[String,List[String]]) = {
        // find readmission patients for each procedure
        var readmissionDfs = collection.mutable.Map[String, DataFrame]()  // dict of dataframe of readmission patients for each procedure
        var providerProcedureInfoDfs = collection.mutable.Map[String, DataFrame]()  // dict of provider event counts for each procedure
        var deaths = collection.mutable.Map[String, DataFrame]()   // dict of patients who died for each procedure
		for ((key, value) <- diagnostic_codes) {
		    breakable {
                // make sure we have readmission codes for this procedure
                if (readmission_codes.contains(key)) {
                    println("No readmission codes for this procedure.  This procedure " + key + " will be skipped. ")
                    break
		        } else {		
                    // find events with codes of interest
                    var condition_occurrence_f = Utils.filterDataframeByCodes(
					    data("condition_occurrence"), 
                        diagnostic_codes(key), 
                        "CONDITION_SOURCE_VALUE").cache() 
                    var procedure_occurrence_f = Utils.filterDataframeByCodes(
					    data("procedure_occurrence"), 
                        diagnostic_codes(key), 
                        "PROCEDURE_SOURCE_VALUE").cache()
                    // only consider inpatient stays where the icd code of interest is an inpatient primary diagnosis
                    condition_occurrence_f = Utils.filterDataframeByCodes(
					    condition_occurrence_f, 
                        inpatient_condition_primary_diagnosis, 
                        "CONDITION_TYPE_CONCEPT_ID").cache()
                    procedure_occurrence_f = Utils.filterDataframeByCodes(
					    procedure_occurrence_f, 
                        inpatient_procedure_primary_diagnosis, 
                        "PROCEDURE_TYPE_CONCEPT_ID").cache()
                    // find readmission events
                    var condition_occurrence_r = Utils.filterDataframeByCodes(
					    data("condition_occurrence"), 
                        readmission_codes(key), 
                        "CONDITION_SOURCE_VALUE").cache()
                    var procedure_occurrence_r = Utils.filterDataframeByCodes(
					    data("procedure_occurrence"), 
                        readmission_codes(key), 
                        "PROCEDURE_SOURCE_VALUE").cache()
                    // only consider readmission events where the icd code of interest is an inpatient primary diagnosis
                    condition_occurrence_r = Utils.filterDataframeByCodes(
					    condition_occurrence_r,
                        inpatient_condition_primary_diagnosis,
                        "CONDITION_TYPE_CONCEPT_ID").cache()
                    procedure_occurrence_r = Utils.filterDataframeByCodes(
					    procedure_occurrence_r,
                        inpatient_procedure_primary_diagnosis,
                        "PROCEDURE_TYPE_CONCEPT_ID").cache()
                    // find users with inpatient stay from the filtered condition_occurrence and procedure_occurrence dataframes
                    val inpatient_co = Utils.findPersonsWithInpatientStay(
					    spark,
					    condition_occurrence_f, 
                        "condition_occurrence", 
                        "VISIT_END_DATE", 
                        true, 
                        date_input_format).cache()
                    val inpatient_po = Utils.findPersonsWithInpatientStay(
					    spark,
					    procedure_occurrence_f, 
                        "procedure_occurrence", 
                        "VISIT_END_DATE", 
                        true, 
                        date_input_format).cache()
                    val inpatient_events = inpatient_co.unionAll(inpatient_po).cache()
                    // find complications.  Only occurs in the condition_occurrence table
                    val complications = Utils.findPersonsWithInpatientStay(
					     spark,
						 condition_occurrence_r, 
                         "condition_occurrence", 
                         "VISIT_START_DATE", 
                         true, 
                         date_input_format).cache()
                    // now find readmissions
                    readmissionDfs(key) = findReadmissionPersons(inpatient_events,
                         complications,
                         readmission_days).cache()
                    deaths(key) = Utils.findDeathAfterEvent(
					     spark, 
						 inpatient_events,
                         readmission_days,
                         date_input_format).cache()


                    // create a dataframe to summarize the provider total procedure count and complication rate
                    val providerEventCount = Utils.countProviderOccurrence(inpatient_events, 
                        spark).withColumnRenamed("COUNT", "PROCEDURE_COUNT").cache()
                    val providerComplicationCount = Utils.countProviderOccurrence(readmissionDfs(key), 
                        spark).withColumnRenamed("COUNT", "READMISSION_COUNT").cache()
                    val providerDeathCount = Utils.countProviderOccurrence(deaths(key),
                        spark).withColumnRenamed("COUNT", "DEATH_COUNT").cache()
                    var providerProcedureInfo = providerEventCount.join(providerComplicationCount, col("providerEventCount.PROVIDER_ID") === col("providerComplicationCount.PROVIDER_ID"), "left")
                    providerProcedureInfo = providerProcedureInfo.na.fill(0)
                    providerProcedureInfo = providerProcedureInfo.join(providerDeathCount, col("providerProcedureInfo.PROVIDER_ID") === col("providerDeathCount.PROVIDER_ID"), "left")
                    providerProcedureInfo = providerProcedureInfo.na.fill(0)
                    providerProcedureInfo = providerProcedureInfo.withColumn("COMPLICATION_COUNT",
                        expr("READMISSION_COUNT + DEATH_COUNT"))
                    providerProcedureInfo = providerProcedureInfo.withColumn("PERCENTAGE", 
                        expr("COMPLICATION_COUNT/PROCEDURE_COUNT"))
                    providerProcedureInfoDfs(key) = providerProcedureInfo
				}
			}
	    }
        (readmissionDfs,providerProcedureInfoDfs,deaths)
    }

    //
    // find persons that have been readmitted to the hospital
    // OMOP tables are global so do not need to be passed to the function
    // dates must be date objects
    //
    def findReadmissionPersons(inpatient_events: DataFrame, complications: DataFrame, days: String) : DataFrame = {
        inpatient_events.registerTempTable("inpatient_events")
        complications.registerTempTable("complications")
        val sqlString = "select distinct inpatient_events.PERSON_ID, inpatient_events.VISIT_END_DATE, inpatient_events.PROVIDER_ID, complications.SOURCE_VALUE from inpatient_events join complications where inpatient_events.PERSON_ID=complications.PERSON_ID and inpatient_events.VISIT_END_DATE < complications.VISIT_START_DATE and complications.VISIT_START_DATE < date_add(inpatient_events.VISIT_END_DATE," + days + ")"
        val df = spark.sql(sqlString)
        df
    }
	
    //
    // find counts of icd codes
    // If primary_only flag is set, only count those icd codes designated as primary inpatient codes
    //
	// removed topandas() need alternative
    def writeCodesAndCount(codes: scala.collection.immutable.Map[String,List[String]], directory: String, filename: String, primary_only: Boolean) = {
        //if not os.path.exists(directory):
        //    os.makedirs(directory)
        var icd_all = spark.emptyDataFrame
		if (primary_only == true) {
            // look only for icd codes that are primary inpatient
            icd_all = Utils.icdGroupingPrimary(data, inpatient_condition_primary_diagnosis, inpatient_procedure_primary_diagnosis)
        } else {
            // look at all icd codes
            icd_all = Utils.icdGrouping(spark)
		}	
        val icd_def = Utils.readFileIcd9("icd/icd9/CMS32_DESC_LONG_DX.txt")  // read icd9 definitions into dict
        /*f = open(os.path.join(directory,filename), "w")
        total_for_all = 0
        for key, value in codes.iteritems():
            f.write("Procedure: " + key + "\n")
            f.write("code, count, description\n")
            total = 0
            for code in value:
                if icd_all[icd_all.SOURCE_VALUE==code].empty:
                    icd_count=0
                else:
                    icd_count = icd_all[icd_all.SOURCE_VALUE==code].COUNT.item()
                total += icd_count
                if code not in icd_def:
                    icd_description = ""
                else:
                    icd_description = icd_def[code]
                outstring = code + "," + str(icd_count) + "," + icd_description + "\n"
                f.write(outstring)
            totalString = "Total Count For This procedure: " + str(total) + "\n\n"
            f.write(totalString)
            total_for_all += total
        totalForAllString = "Total Count For All Procedures: " + str(total_for_all) + "\n"
        f.write(totalForAllString)
        f.close() */
    }
	
    //
    //  For a particular icd code, count the number of occurrences
    //  This is done by summing the count values in condition_occurrence and procedure_occurrence
    //  Tables condition_occurrence and procedure_occurrence are global
    //
    def readmissionGrouping(readmission: DataFrame) : DataFrame = {
        readmission.registerTempTable("readmission")
        val icd_count = spark.sql("select SOURCE_VALUE, count(*) COUNT from readmission group by SOURCE_VALUE")
        icd_count
    }
	
    //
    // find code counts for readmission event
    //
    def writeReadmissionCodesAndCount(codes: scala.collection.immutable.Map[String,List[String]], readmissionDfs: scala.collection.mutable.Map[String,DataFrame], directory: String, filename: String) = {
        //if not os.path.exists(directory):
        //    os.makedirs(directory)
        val icd_def = Utils.readFileIcd9("icd/icd9/CMS32_DESC_LONG_DX.txt")  // read icd9 definitions into dict
        /*f = open(os.path.join(directory,filename), "w")
        total_for_all = 0
        for key, value in codes.iteritems():
            icd_all = readmissionGrouping(sqlContext, readmissionDfs[key]).toPandas()
            f.write("Procedure: " + key + "\n")
            f.write("code, count, description\n")
            total = 0
            for code in value:
                if icd_all[icd_all.SOURCE_VALUE==code].empty:
                    icd_count=0
                else:
                    icd_count = icd_all[icd_all.SOURCE_VALUE==code].COUNT.item()
                total += icd_count
                if code not in icd_def:
                    icd_description = ""
                else:
                    icd_description = icd_def[code]
                outstring = code + "," + str(icd_count) + "," + icd_description + "\n"
                f.write(outstring)
            totalString = "Total Count For This procedure: " + str(total) + "\n\n"
            f.write(totalString)
            total_for_all += total
        totalForAllString = "Total Count For All Procedures: " + str(total_for_all) + "\n"
        f.write(totalForAllString)
        f.close() */
	}	
}
