package com.opme.spark.utils

import scala.io.Source
import org.apache.spark.sql._ 
import java.io._
import com.opme.spark.model.Model
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.col


object Utils {


    // 
    // load a CSV file into a table
    // The name of the table is the same name as the CVS file without extension
    // Compressed files are supported
    //
    def loadCsv(spark: SparkSession, path: String, schema: StructType): DataFrame = {
	    val df = spark.read
            .format("csv")
            .option("header", "true") //reading the headers
            .option("mode", "DROPMALFORMED")
		    .schema(schema)
            .load(path)
        df
    }

    private val pattern = "(\\w+)(\\.csv.gz)?$".r.unanchored
    def inferTableNameFromPath(path: String) = path match {
       case pattern(filename, extension) => filename
       case _ => path
    }

    // 
    // Write a dataframe to csv.  This is writing the file in pieces on different executors
    //
    def saveDataframeAsFile(df: DataFrame, codec: String, filename: String) {
       df
	      .write.mode(SaveMode.Overwrite)
	      .option("header", "true")
		  .option("mapreduce.fileoutputcommitter.marksuccessfuljobs","false")
		  .option("compression",codec)
		  .csv(filename)
    }

    //
    // Write a dataframe to a single csv file.
    // compression codecs include gzip, bzip2, lz4, snappy and deflate
    // potential to go out of memory depending on data size and memory available in master
    //
    def saveDataframeAsSingleFile(df: DataFrame, codec: String, filename: String) {
       df
          .repartition(1)
          .write.mode(SaveMode.Overwrite)
		  .option("header", "true")
		  .option("mapreduce.fileoutputcommitter.marksuccessfuljobs","false")
		  .option("compression",codec)
		  .csv(filename)
    }
  
    //
    // get list of files in a directory
    //
    def getListOfFiles(dir: String):List[File] = {
       val d = new File(dir)
       if (d.exists && d.isDirectory) {
           d.listFiles.filter(_.isFile).toList
       } else {
           List[File]()
       }
    }
  
    //
    // load data from csv files inside a directory and register as a table
    // reference with a dictionary
    // Handle compression file formats
    // supports only compressed csv files
    // 
    def loadRawData(spark: SparkSession, directory: String) : collection.mutable.Map[String, DataFrame] = {
        var data = collection.mutable.Map[String, DataFrame]()
	    val model = new Model()
		val files = getListOfFiles(directory)
		files.foreach(file => {
		    val path = directory + "/" + file.getName()
			var tablename = inferTableNameFromPath(file.getName())
			if (model.model_schema.contains(tablename)) {
			   println(inferTableNameFromPath(file.getName()))
		       val df = loadCsv(spark, path, model.model_schema(tablename))
			   data += (tablename -> df)
			}
            else {
               println("No model exists for: " + file.getName() + ". This data file will be skipped.")	
            }			   
		})
        data		
    }		

    //
    // write data to csv files
    //
    def writeRawData(data: collection.mutable.Map[String, DataFrame], codec: String, directory: String) = {
		val codec = "gzip"
        for ((key, value) <- data) {
            saveDataframeAsSingleFile(value, codec, directory + "/" + key)
		}
    }
	
    //
    // read in the cms icd9 description file CMS32_DESC_LONG_DX.txt into a dictionary
    //
    def readFileIcd9(filename: String) : scala.collection.immutable.Map[String,String] = {
		val file = Source.fromFile(filename).getLines.filter(f => !f.trim.isEmpty)
        val icd9 = file.map(m2 => (m2.split(" ")(0), m2.split(" ")(1))).toMap
        icd9
	}

	//
    // read a file and convert name=values pairs to a dictionary.  The values is a list
	// expects a file with name=value where value is a set of values seperated by commas
    //
    def readFileToDict(filename: String) : scala.collection.immutable.Map[String,List[String]] = {
       println("converting " + filename + " to map")
       val stream: InputStream = getClass.getResourceAsStream(filename)
	   val lines: Iterator[String] = scala.io.Source.fromInputStream( stream ).getLines
       val pairs = 
       for {
         line <- lines
	     val substrings = line.split("=").map(_.trim)
	     name = substrings(0)
	     value = substrings(1).split(",").map(_.trim).toList
       } yield (name -> value)
       pairs.toMap
    }
	
    //
    //  count the distinct condition_occurrence CONDITION_TYPE_CONCEPT_ID
    //
	// maybe need to remove the CONDITION_TYPE_CONCEPT_ID before count.  Is that needed?
    def conditionTypeConceptCount(spark: SparkSession) : DataFrame = {
        val condition_concept_count = spark.sql("select CONDITION_TYPE_CONCEPT_ID, count(*) COUNT from condition_occurrence group by CONDITION_TYPE_CONCEPT_ID")
        condition_concept_count
	}

    //
    //  count the distinct procedure_occurrence PROCEDURE_TYPE_CONCEPT_ID
	//
    def procedureTypeConceptCount(spark: SparkSession) : DataFrame = {
        val procedure_concept_count = spark.sql("select PROCEDURE_TYPE_CONCEPT_ID, count(*) COUNT from procedure_occurrence group by PROCEDURE_TYPE_CONCEPT_ID")
        procedure_concept_count
	}	

    //
    //  For a particular icd code, count the number of occurrences
    //  This is done by summing the count values in condition_occurrence and procedure_occurrence
    //  Tables condition_occurrence and procedure_occurrence are global
    //
    def icdGrouping(spark: SparkSession) : DataFrame = { 
        val icd_co = spark.sql("select CONDITION_SOURCE_VALUE SOURCE_VALUE_CO, count(*) COUNT_CO from condition_occurrence group by CONDITION_SOURCE_VALUE")
        val icd_po = spark.sql("select PROCEDURE_SOURCE_VALUE SOURCE_VALUE_PO, count(*) COUNT_PO from procedure_occurrence group by PROCEDURE_SOURCE_VALUE")
        var icd_all = icd_co.join(icd_po,col("SOURCE_VALUE_CO") === col("SOURCE_VALUE_PO"), "outer").na.fill(0)
        //icd_all = icd_all.withColumn("COUNT", icd_all.COUNT_CO + icd_all.COUNT_PO)
        icd_all
    }
	
    
	//
    //  For a particular icd code, count the number of principal admission diagnosis codes for patients 
    //  undergoing each of the procedures.icd
    //
    def icdGroupingPrimary(data: collection.mutable.Map[String, DataFrame], conditionCodes: List[String], procedureCodes: List[String]) : DataFrame = {
        val icd_co = Utils.filterDataframeByCodes(data("condition_occurrence"),
                conditionCodes,
                "CONDITION_TYPE_CONCEPT_ID")
        val icd_po = Utils.filterDataframeByCodes(data("procedure_occurrence"),
                procedureCodes,
                "PROCEDURE_TYPE_CONCEPT_ID")
        var icd_all = icd_co.join(icd_po,col("icd_co.SOURCE_VALUE") === col("icd_po.SOURCE_VALUE"), "outer").na.fill(0)
        //icd_all = icd_all.withColumn('COUNT', icd_all.COUNT_CO + icd_all.COUNT_PO)
        icd_all 
    }

    //    
    // filter a dataframe by checking a column for a list of codes
    //
    def filterDataframeByCodes(df: DataFrame, codes: List[String], column: String) : DataFrame = {
        val tempdf = df.where(df(column).isin(codes:_*))
        tempdf
    }
	
    //   
    // find persons that have had inpatient stay with a condition_occurrence or procedure_occurrence
    // convert-dates - convert date string columns to date objects
    // OMOP tables are global so do not need to be passed to the function
    //
    def findPersonsWithInpatientStay(spark: SparkSession, df: DataFrame, table: String, date: String, convert_dates: Boolean, date_format: String) : DataFrame = {
        df.registerTempTable("event_occurrence")
        val event = table.stripSuffix("_occurrence").trim
		var start_date : String = ""
		var source_value : String = ""
        if (event == "PROCEDURE") {
            start_date = "PROCEDURE_DATE"
            source_value = "PROCEDURE_SOURCE_VALUE"
		}
        else {
            start_date = "CONDITION_START_DATE"
            source_value = "CONDITION_SOURCE_VALUE"
		}
        val sqlString = "select distinct event_occurrence.PERSON_ID, visit_occurrence." + date + ", visit_occurrence.PROVIDER_ID, event_occurrence." + source_value + " SOURCE_VALUE from visit_occurrence join event_occurrence where event_occurrence.PERSON_ID=visit_occurrence.PERSON_ID and event_occurrence." + start_date + " >= visit_occurrence.VISIT_START_DATE and event_occurrence." + start_date + " <= visit_occurrence.VISIT_END_DATE"
        val tempdf = spark.sql(sqlString)
        //if (convert_dates != null && !convert_dates.isEmpty())
            //date_conversion =  udf (lambda x: datetime.strptime(x, date_format), DateType())
            //val tempdf = df.withColumn(date, date_conversion(col(date)))
		//}
        tempdf
    }
	
    //
    // find persons that have had died after an inpatient event
    // convert-dates - convert date string columns to date objects
    // OMOP tables are global so do not need to be passed to the function
    //
    def findDeathAfterEvent(spark: SparkSession, inpatient_events: DataFrame, days: String, date_format: String) : DataFrame = {
        inpatient_events.registerTempTable("inpatient_events")
        val sqlString = "select * from death"
        val death_date_converted = spark.sql(sqlString)
        //val date_conversion =  udf (lambda x: datetime.strptime(x, date_format), DateType())
		//date_conversion = date_format
        //val death_date_converted = death_date_converted.withColumn('DEATH_DATE', date_conversion(col('DEATH_DATE')))
        death_date_converted.registerTempTable("death_date_converted")
        val sqlString2 = "select distinct inpatient_events.PERSON_ID, inpatient_events.VISIT_END_DATE, inpatient_events.PROVIDER_ID from inpatient_events join death_date_converted where"

        //		inpatient_events.PERSON_ID=death_date_converted.PERSON_ID and death_date_converted.DEATH_DATE"
		//< date_add(inpatient_events.VISIT_END_DATE," + days + ")"
        val df = spark.sql(sqlString2)
        // code death as 0
        //val tempdf = df.withColumn("SOURCE_VALUE", lit(0))
        df
    }
	
    //
    // count the number of occurrences for a provider
    //
    def countProviderOccurrence(eventDf: DataFrame, spark: SparkSession) : DataFrame = {
        eventDf.registerTempTable("provider_events")
        val provider_event_counts = spark.sql("select provider_events.PROVIDER_ID, count(*) count from provider_events group by PROVIDER_ID order by count desc")
        provider_event_counts
	}
}
