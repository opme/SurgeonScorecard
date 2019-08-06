import os
import sys
from datetime import datetime
import configparser
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import col,udf,unix_timestamp,lit
from pyspark.sql.types import *
import Model

class Utils:
    def __init__(self, sqlContext):
        self.sqlContext = sqlContext 

    #
    # load a CSV file into a table
    # The name of the table is the same name as the CVS file without extension
    # Compressed files are supported
    #
    def loadCsv(self, sqlContext, file, schema):
        df = self.sqlContext.read.load(file,
                          format='com.databricks.spark.csv',
                          header='true',
                          mode="DROPMALFORMED",
                          schema=schema)
        return df

    # 
    # Write a dataframe to a csv file
    #
    def saveDataframeAsFile(self, df, codec, file):
        df.write.format('com.databricks.spark.csv').options(header="true", codec=codec).save(file)

    #
    # Write a dataframe to a single csv file.  First convert it to pandas dataframe.
    #
    def saveDataframeAsSingleFile(self, df, directory, filename):
        if not os.path.exists(directory):
            os.makedirs(directory)
        dfp = df.toPandas()
        dfp.to_csv(os.path.join(directory,filename), header=True, index=False)

    #
    # generate a table name from the file path
    #
    def getTableNameFromPath(self, file):
        filename_no_ext, file_extension = os.path.splitext(file)
        return os.path.splitext("path_to_file")[0]


    #
    # string the end of string text based on suffix
    #
    def strip_end(self, text, suffix):
        if not text.endswith(suffix):
            return text
        return text[:len(text)-len(suffix)]

    #
    # load data from csv files inside a directory and register as a table
    # reference with a dictionary
    # Handle compression file formats
    #
    def loadRawData(self, sqlContext, directory):
        data = {}
        model = Model.Model()
        for filename in os.listdir(directory):
            key=None
            if filename.lower().endswith('.csv'): key=self.strip_end(filename, ".csv") # no compression
            if filename.lower().endswith('.csv.gz'): key=self.strip_end(filename, ".csv.gz") # gzip
            if filename.lower().endswith('.csv.zip'): key=self.strip_end(filename, ".csv.zip") # zip
            if filename.lower().endswith('.csv.bzip2'): key=self.strip_end(filename, ".csv.bzip2") # bzip2
            if filename.lower().endswith('.csv.lz4'): key=self.strip_end(filename, ".csv.lz4") # lz4
            if filename.lower().endswith('.csv.snappy'): key=self.strip_end(filename, ".csv.snappy") # snappy
            if key != None:
                model_check = model.model_schema.get(key)
                if model_check:
                    data[key] = self.loadCsv(sqlContext, os.path.join(directory,filename),model.model_schema[key])
                    data[key].registerTempTable(key)  
                else:
                    print("No model exists for: " + key + ".  This data file will be skipped.")
        return data

    #
    # write data to csv files
    #
    def writeRawData(self, data, codec, directory):
        if not os.path.exists(directory):
            os.makedirs(directory)
        for key, value in data.items():
            self.saveDataframeAsFile(data[key], codec, os.path.join(directory,key))

    #
    # read in the cms icd9 description file CMS32_DESC_LONG_DX.txt into a dictionary
    #
    def readFileIcd9(self, file):
        icd9 = {}
        for line in open(file, 'r'):
            a, sep, b = line.partition(' ')
            b = b.rstrip()
            icd9[a] = b
        return icd9

    #
    #  count the distinct condition_occurrence CONDITION_TYPE_CONCEPT_ID
    #
    def conditionTypeConceptCount(self, sqlContext):
        condition_concept_count = sqlContext.sql("select CONDITION_TYPE_CONCEPT_ID, count(*) COUNT from condition_occurrence group by CONDITION_TYPE_CONCEPT_ID")
        return condition_concept_count

    #
    #  count the distinct procedure_occurrence PROCEDURE_TYPE_CONCEPT_ID
    #
    def procedureTypeConceptCount(self, sqlContext):
        procedure_concept_count = sqlContext.sql("select PROCEDURE_TYPE_CONCEPT_ID, count(*) COUNT from procedure_occurrence group by PROCEDURE_TYPE_CONCEPT_ID")
        return procedure_concept_count

    #
    #  For a particular icd code, count the number of occurrences
    #  This is done by summing the count values in condition_occurrence and procedure_occurrence
    #  Tables condition_occurrence and procedure_occurrence are global
    #
    def icdGrouping(self, sqlContext):
        icd_co = sqlContext.sql("select CONDITION_SOURCE_VALUE SOURCE_VALUE, count(*) COUNT_CO from condition_occurrence group by CONDITION_SOURCE_VALUE")
        icd_po = sqlContext.sql("select PROCEDURE_SOURCE_VALUE SOURCE_VALUE, count(*) COUNT_PO from procedure_occurrence group by PROCEDURE_SOURCE_VALUE")
        icd_all = icd_co.join(icd_po,'SOURCE_VALUE', how='outer').fillna(0)
        icd_all = icd_all.withColumn('COUNT', icd_all.COUNT_CO + icd_all.COUNT_PO)
        return icd_all

    #
    #  For a particular icd code, count the number of principal admission diagnosis codes for patients 
    #  undergoing each of the procedures.
    #
    def icdGroupingPrimary(self, sqlContext, data, conditionCodes, procedureCodes):
        icd_co = self.filterDataframeByCodes(data['condition_occurrence'],
                conditionCodes,
                'CONDITION_TYPE_CONCEPT_ID')
        icd_po = self.utils.filterDataframeByCodes(data['procedure_occurrence'],
                procedureCodes,
                'PROCEDURE_TYPE_CONCEPT_ID')
		icd_co.createOrReplaceTempView(icd_co)
		icd_po.createOrReplaceTempView(icd_po)
		icd_co = sqlContext.sql("select CONDITION_SOURCE_VALUE SOURCE_VALUE, count(*) COUNT_CO from icd_co group by CONDITION_SOURCE_VALUE")
		icd_po = sqlContext.sql("select PROCEDURE_SOURCE_VALUE SOURCE_VALUE, count(*) COUNT_PO from icd_po group by PROCEDURE_SOURCE_VALUE")
        icd_all = icd_co.join(icd_po,'SOURCE_VALUE', how='outer').fillna(0)
        icd_all = icd_all.withColumn('COUNT', icd_all.COUNT_CO + icd_all.COUNT_PO)
		
        #icd_co = sqlContext.sql("select CONDITION_SOURCE_VALUE SOURCE_VALUE, count(*) COUNT_CO 
        #                            from condition_occurrence where CONDITION_TYPE_CONCEPT_ID='38000199' group by CONDITION_SOURCE_VALUE")
        #icd_po = sqlContext.sql("select PROCEDURE_SOURCE_VALUE SOURCE_VALUE, count(*) COUNT_PO 
        #                            from procedure_occurrence where PROCEDURE_TYPE_CONCEPT_ID='38000250' group by PROCEDURE_SOURCE_VALUE")
        return icd_all


    #
    # filter a dataframe by checking a column for a list of codes
    #
    def filterDataframeByCodes(self, df, codes, column):
        df = df.where(df[column].isin(codes))
        return df

    #
    # find persons that have had inpatient stay with a condition_occurrence or procedure_occurrence
    # convert-dates - convert date string columns to date objects
    # OMOP tables are global so do not need to be passed to the function
    #
    def findPersonsWithInpatientStay(self, df, table, date, convert_dates, date_format):
        df.registerTempTable('event_occurrence')
        event = self.strip_end(table, "_occurrence").upper()
        if event == "PROCEDURE":
            start_date = "PROCEDURE_DATE"
            source_value = "PROCEDURE_SOURCE_VALUE"
        else:
            start_date = "CONDITION_START_DATE"
            source_value = "CONDITION_SOURCE_VALUE"
        sqlString = "select distinct event_occurrence.PERSON_ID, visit_occurrence." + date + ", visit_occurrence.PROVIDER_ID, event_occurrence." + source_value+ " SOURCE_VALUE from visit_occurrence join event_occurrence where event_occurrence.PERSON_ID=visit_occurrence.PERSON_ID and event_occurrence." + start_date + " >= visit_occurrence.VISIT_START_DATE and event_occurrence." + start_date + " <= visit_occurrence.VISIT_END_DATE"
        df = self.sqlContext.sql(sqlString)
        if convert_dates:
            date_conversion =  udf (lambda x: datetime.strptime(x, date_format), DateType())
            df = df.withColumn(date, date_conversion(col(date)))
        return df

    #
    # find persons that have had died after an inpatient event
    # convert-dates - convert date string columns to date objects
    # OMOP tables are global so do not need to be passed to the function
    #
    def findDeathAfterEvent(self,inpatient_events, days, date_format):
        inpatient_events.registerTempTable('inpatient_events')
        sqlString = "select * from death"
        death_date_converted = self.sqlContext.sql(sqlString)
        date_conversion =  udf (lambda x: datetime.strptime(x, date_format), DateType())
        death_date_converted = death_date_converted.withColumn('DEATH_DATE', date_conversion(col('DEATH_DATE')))
        death_date_converted.registerTempTable('death_date_converted')
        sqlString = "select distinct inpatient_events.PERSON_ID, inpatient_events.VISIT_END_DATE, inpatient_events.PROVIDER_ID from inpatient_events join death_date_converted where inpatient_events.PERSON_ID=death_date_converted.PERSON_ID and death_date_converted.DEATH_DATE < date_add(inpatient_events.VISIT_END_DATE," + days + ")"
        df = self.sqlContext.sql(sqlString)
        # code death as 0
        df = df.withColumn('SOURCE_VALUE', lit(0))
        return df

    #
    # count the number of occurrences for a provider
    #
    def countProviderOccurrence(self, eventDf, sqlContext):
        eventDf.registerTempTable('provider_events')
        provider_event_counts = sqlContext.sql("select provider_events.PROVIDER_ID, count(*) count from provider_events group by PROVIDER_ID order by count desc") 
        return provider_event_counts
