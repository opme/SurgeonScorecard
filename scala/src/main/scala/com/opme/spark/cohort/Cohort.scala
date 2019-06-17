package com.opme.spark.cohort

import com.typesafe.config._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import com.opme.spark.utils.Utils

//
// define types for omop schema
//
class Cohort(spark: SparkSession, data: collection.mutable.Map[String, DataFrame], config: Config) {

       // set instance variables based on properties
       val env = "scorecard"
       val year_of_birth_min = config.getString(env + ".year_of_birth_min")
       val year_of_birth_max = config.getString(env + ".year_of_birth_max")
       val events_start_date = config.getString(env + ".events_start_date")
       val events_end_date = config.getString(env + ".events_end_date")
       val filter_dead = config.getString(env + ".filter_dead")
       val filter_alive = config.getString(env + ".filter_alive")
       val filter_male = config.getString(env + ".filter_male")
       val filter_female = config.getString(env + ".filter_female")
       val write_csv_output = config.getString(env + ".write_csv_output")
       val csv_output_dir = config.getString(env + ".csv_output_dir")
       val csv_output_codec = config.getString(env + ".csv_output_codec")
       val filter_care_sites = config.getString(env + ".filter_care_sites")
	   val include_care_sites = config.getString(env + ".include_care_sites")
       val inpatient_only = config.getString(env + ".inpatient_only")
       val inpatient_condition_primary_diagnosis = config.getString(env + ".inpatient_condition_primary_diagnosis").split(',').toList
       val inpatient_procedure_primary_diagnosis = config.getString(env + ".inpatient_procedure_primary_diagnosis").split(',').toList
	   
       println("Number of patients in cohort: " + data("person").count())

	   // apply filter functions
       if (year_of_birth_max != null && !year_of_birth_max.isEmpty())
           filterByMaxYearOfBirth(data)
       if (year_of_birth_min != null && !year_of_birth_min.isEmpty())
           filterByMinYearOfBirth(data)
       println("Number of patients in cohort after year of birth: " + data("person").count())
	   
       if (filter_male == "True") 
           filterMale(data)
       if (filter_female == "True")
           filterFemale(data)
       println("Number of patients in cohort after filter male female: " + data("person").count())
	   
       println("Filter by care sites...")
	   if (filter_care_sites != null && !filter_care_sites.isEmpty()) {
	       val filter_care_sites_list = (filter_care_sites.split(',')).toList
           filterCareSites(data, filter_care_sites_list)
	   }	   
       if (include_care_sites != null && !include_care_sites.isEmpty()) {
	       val include_care_sites_list = (include_care_sites.split(',')).toList
           includeCareSites(data, include_care_sites_list)
	   }
	   println("Filter by event date...")
	   if ((events_start_date != null && !events_start_date.isEmpty()) || (events_end_date != null && !events_end_date.isEmpty()))
           filterByEventDate(data, events_start_date, events_end_date)
       // filter by users who have not had an inpatient stay
	   println("Filter by inpatient only...")
       if (inpatient_only == "True")
           filterInpatientOnly(data)

	   // write the filtered data back out to files
       if (write_csv_output == "True")
	        println("Writing cohort as csv files")
            Utils.writeRawData(data,csv_output_codec,csv_output_dir)

       // after filtering data, reset the caches 
       resetDataCache(data)


	// reset data caches
    def resetDataCache(data: collection.mutable.Map[String, DataFrame]) = {
		for ((key,value) <- data) {
            data(key).registerTempTable(key)
            data(key).cache()
		}
	}		
			
    // filter by maximum year of birth
    def filterByMaxYearOfBirth(data: collection.mutable.Map[String, DataFrame]) : collection.mutable.Map[String, DataFrame] = {
		data("person") = data("person").filter(data("person")("YEAR_OF_BIRTH") <=  year_of_birth_max.toInt)
		data
    }
	
    // filter by minimum year of birth
    def filterByMinYearOfBirth(data: collection.mutable.Map[String, DataFrame]) : collection.mutable.Map[String, DataFrame] = {
		data("person") = data("person").filter(data("person")("YEAR_OF_BIRTH") >=  year_of_birth_min.toInt)
		data
	}
	
    //
    // filter by minimum events
    // currently only filters data from condition_occurrence, procedure_occurrence, visit_occurrence, measurement,
    // observation, and device_exposure
	// date can be done by string comparison since it is in YYYYMMDD format
    //
    def filterByEventDate(data: collection.mutable.Map[String, DataFrame], start_date: String, end_date: String) : collection.mutable.Map[String, DataFrame] = {
	     if (start_date != null && !start_date.isEmpty()) {
            data("condition_occurrence") = data("condition_occurrence").filter(data("condition_occurrence")("CONDITION_START_DATE") >= start_date)
            data("procedure_occurrence") = data("procedure_occurrence").filter(data("procedure_occurrence")("PROCEDURE_DATE") >= start_date)
            data("visit_occurrence") = data("visit_occurrence").filter(data("visit_occurrence")("VISIT_START_DATE") >= start_date)
            data("measurement") = data("measurement").filter(data("measurement")("MEASUREMENT_DATE") >= start_date)
            data("observation") = data("observation").filter(data("observation")("OBSERVATION_DATE") >= start_date)
            data("device_exposure") = data("device_exposure").filter(data("device_exposure")("DEVICE_EXPOSURE_START_DATE") >= start_date)
		 }	
	     if (end_date != null && !end_date.isEmpty()) {
            data("condition_occurrence") = data("condition_occurrence").filter(data("condition_occurrence")("CONDITION_END_DATE") >= end_date)
            data("procedure_occurrence") = data("procedure_occurrence").filter(data("procedure_occurrence")("PROCEDURE_DATE") >= end_date)
            data("visit_occurrence") = data("visit_occurrence").filter(data("visit_occurrence")("VISIT_END_DATE") >= end_date)
            data("measurement") = data("measurement").filter(data("measurement")("MEASUREMENT_DATE") >= end_date)
            data("observation") = data("observation").filter(data("observation")("OBSERVATION_DATE") >= end_date)
            data("device_exposure") = data("device_exposure").filter(data("device_exposure")("DEVICE_EXPOSURE_END_DATE") >= end_date)
		 }	
	     data
    }		 
			
    //filter out Male patients
    def filterMale(data: collection.mutable.Map[String, DataFrame]) : collection.mutable.Map[String, DataFrame] = {
		data("person") = data("person").filter(data("person")("GENDER_SOURCE_VALUE") =!= "Male")
		data
    }

    // filter out Female patients
    def filterFemale(data: collection.mutable.Map[String, DataFrame]) : collection.mutable.Map[String, DataFrame] = {
		data("person") = data("person").filter(data("person")("GENDER_SOURCE_VALUE") =!= "Female")
		data
	}

    // filter out dead patients
	// not implemented
    def filterDead(data: collection.mutable.Map[String, DataFrame]) : collection.mutable.Map[String, DataFrame] = {
        data
    }
	
    // filter out alive patients
	// not implemented
    def filterAlive(data: collection.mutable.Map[String, DataFrame]) : collection.mutable.Map[String, DataFrame] = {
        data
    }
    
    // filter out persons who have these primary care sites
    def filterCareSites(data: collection.mutable.Map[String, DataFrame], filter_care_sites_list: List[String]) : collection.mutable.Map[String, DataFrame] = {
        // make sure null is not filtered also when filtering my list by adding an additional check
		data("person") = data("person").filter(not(data("person")("CARE_SITE_ID").isin(filter_care_sites_list:_*)))
		//.bitwiseOR(data("person")("CARE_SITE_ID").isNull)))
		data
    }
	
    // include only persons that have primary care sites
    def includeCareSites(data: collection.mutable.Map[String, DataFrame], include_care_sites_list: List[String]) : collection.mutable.Map[String, DataFrame] = {
	      data("person") = data("person").filter(data("person")("CARE_SITE_ID").isin(include_care_sites_list:_*))
		  data
	}
	
    // filter out users that have not had a visit to the hospital
    // first join person with visit_occurrence then drop the visit_occurrence column and get rid of duplicates
    def filterNoHospitalVisit(data: collection.mutable.Map[String, DataFrame]) : collection.mutable.Map[String, DataFrame] = {
        var df = spark.sql("select person.*, visit_occurrence.VISIT_OCCURRENCE_ID from person inner join visit_occurrence on person.PERSON_ID=visit_occurrence.PERSON_ID")
        df = df.drop("VISIT_OCCURRENCE_ID")
        df = df.dropDuplicates("PERSON_ID")
        data("person") = df
        data		
	}	

    // filter out users that have not had an inpatient stay at the hospital
    // first join person with visit_occurrence then drop the visit_occurrence column and get rid of duplicates
    def filterInpatientOnly(data: collection.mutable.Map[String, DataFrame]) : collection.mutable.Map[String, DataFrame] = {
        // get all patients that have an inpatient condition_occurrence
        val icd_co_temp = Utils.filterDataframeByCodes(data("condition_occurrence"),
            inpatient_condition_primary_diagnosis,
           "CONDITION_TYPE_CONCEPT_ID")
        icd_co_temp.registerTempTable("condition_occurrence_primary")
        val dfc = spark.sql("select person.*, condition_occurrence_primary.CONDITION_TYPE_CONCEPT_ID from person inner join condition_occurrence_primary on person.PERSON_ID=condition_occurrence_primary.PERSON_ID")
        dfc.drop("CONDITION_TYPE_CONCEPT_ID")
        // get all patients that have an inpatient procedure_occurrence
        val icd_po_temp = Utils.filterDataframeByCodes(data("procedure_occurrence"),
            inpatient_procedure_primary_diagnosis,
           "PROCEDURE_TYPE_CONCEPT_ID")
        icd_po_temp.registerTempTable("procedure_occurrence_primary")
        val dfp = spark.sql("select person.*, procedure_occurrence_primary.PROCEDURE_TYPE_CONCEPT_ID from person inner join procedure_occurrence_primary on person.PERSON_ID=procedure_occurrence_primary.PERSON_ID")
        dfp.drop("PROCEDURE_TYPE_CONCEPT_ID")
        // join the two patient dataframes
        var df = dfc.unionAll(dfp)
		df.dropDuplicates("PERSON_ID")
        data("person") = df
		data
	}
}