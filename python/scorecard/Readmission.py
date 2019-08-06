import os
import configparser
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
import Utils

class Readmission:
    #
    # Detect readmission given a set of data in OMOP format
    #
    def __init__(self, data, config, sc, sqlContext):
       self.sc = sc
       self.data = data
       self.sqlContext = sqlContext
       self.utils = Utils.Utils(sqlContext)
       self.env = config.get('branch','env')
       self.date_input_format = config.get(self.env+'.data','date_input_format')
       self.readmission_days = config.get(self.env+'.readmission','readmission_days')
       self.readmission_code_file = config.get(self.env+'.readmission','readmission_code_file')
       self.diagnostic_code_file = config.get(self.env+'.readmission','diagnostic_code_file')
       self.comorbidies_code_file = config.get(self.env+'.readmission','comorbidies_code_file')
       self.readmission_codes = self.getReadmissionCodes(self.readmission_code_file)
       self.diagnostic_codes = self.getDiagnosticCodes(self.diagnostic_code_file)
       self.comorbitiy_codes = self.getComorbityCodes(self.comorbidies_code_file)
       self.icd_diagnosis = config.get(self.env+'.readmission','icd_diagnosis')
       self.icd_readmission = config.get(self.env+'.readmission','icd_readmission')
       self.inpatient_condition_primary_diagnosis = config.get(self.env+'.readmission','inpatient_condition_primary_diagnosis').split(",")
       self.inpatient_procedure_primary_diagnosis =  config.get(self.env+'.readmission','inpatient_procedure_primary_diagnosis').split(",")


       # find readmission patients based on criteria in the properties file
       self.readmissionDfs,self.providerProcedureInfoDfs,self.deaths = self.readmissionPatients(data, config, self.diagnostic_codes, self.readmission_codes)

    #
    # read diagnostic codes from a file into a dictionary
    # 
    def getDiagnosticCodes(self, filename):
       codes = self.readFileToDict(filename)
       return codes

    #
    # read readmission codes from a file into a dictionary
    # 
    def getReadmissionCodes(self, filename):
       codes = self.readFileToDict(filename)
       return codes

    #
    # read readmission codes from a file into a dictionary
    #
    def getComorbityCodes(self, filename):
       codes = self.readFileToDict(filename)
       return codes

    #
    # read a file and convert name=values pairs to a dictionary
    #
    def readFileToDict(self, filename):
       props = {}
       with open(filename, 'r') as f:
           for line in f:
               line = line.rstrip() #removes trailing whitespace and '\n' chars
               if "=" not in line: continue #skips blanks and comments w/o =
               if line.startswith("#"): continue #skips comments which contain =
               k, v = line.split("=", 1)
               k = k.strip()
               v = v.strip().split(",")
               props[k] = v
       return props

    #
    # find patients that have been readmitted and return
    # a parallel data structure that contains information
    # on only these individuals
    #
    def readmissionPatients(self, data, config, diagnostic_codes, readmission_codes):
        # find readmission patients for each procedure
        readmissionDfs = {}  # dict of dataframe of readmission patients for each procedure
        providerProcedureInfoDfs = {}  # dict of provider event counts for each procedure
        deaths = {} # dict of patients who died for each procedure
        for key, value in diagnostic_codes.items():
            # make sure we have readmission codes for this procedure
            if key not in readmission_codes:
                print("No readmission codes for this procedure.  This procedure " + key + " will be skipped. ")
                continue
            # find events with codes of interest
            condition_occurrence_f = self.utils.filterDataframeByCodes(data['condition_occurrence'], 
                diagnostic_codes[key], 
                'CONDITION_SOURCE_VALUE').cache() 
            procedure_occurrence_f = self.utils.filterDataframeByCodes(data['procedure_occurrence'], 
                diagnostic_codes[key], 
                'PROCEDURE_SOURCE_VALUE').cache()
            # only consider inpatient stays where the icd code of interest is an inpatient primary diagnosis
            condition_occurrence_f = self.utils.filterDataframeByCodes(condition_occurrence_f, 
                self.inpatient_condition_primary_diagnosis, 
                'CONDITION_TYPE_CONCEPT_ID').cache()
            procedure_occurrence_f = self.utils.filterDataframeByCodes(procedure_occurrence_f, 
                self.inpatient_procedure_primary_diagnosis, 
                'PROCEDURE_TYPE_CONCEPT_ID').cache()
            # find readmission events
            condition_occurrence_r = self.utils.filterDataframeByCodes(data['condition_occurrence'], 
                readmission_codes[key], 
                'CONDITION_SOURCE_VALUE').cache()
            procedure_occurrence_r = self.utils.filterDataframeByCodes(data['procedure_occurrence'], 
                readmission_codes[key], 
                'PROCEDURE_SOURCE_VALUE').cache()
            # only consider readmission events where the icd code of interest is an inpatient primary diagnosis
            condition_occurrence_r = self.utils.filterDataframeByCodes(condition_occurrence_r,
                self.inpatient_condition_primary_diagnosis,
                'CONDITION_TYPE_CONCEPT_ID').cache()
            procedure_occurrence_r = self.utils.filterDataframeByCodes(procedure_occurrence_r,
                self.inpatient_procedure_primary_diagnosis,
                'PROCEDURE_TYPE_CONCEPT_ID').cache()
            # find users with inpatient stay from the filtered condition_occurrence and procedure_occurrence dataframes
            inpatient_co = self.utils.findPersonsWithInpatientStay(condition_occurrence_f, 
                'condition_occurrence', 
                'VISIT_END_DATE', 
                True, 
                self.date_input_format).cache()
            inpatient_po = self.utils.findPersonsWithInpatientStay(procedure_occurrence_f, 
                'procedure_occurrence', 
                'VISIT_END_DATE', 
                True, 
                self.date_input_format).cache()
            inpatient_events = inpatient_co.unionAll(inpatient_po).cache()
            # find complications.  Only occurs in the condition_occurrence table
            complications = self.utils.findPersonsWithInpatientStay(condition_occurrence_r, 
                'condition_occurrence', 
                'VISIT_START_DATE', 
                True, 
                self.date_input_format).cache()
            # now find readmissions
            readmissionDfs[key] = self.findReadmissionPersons(inpatient_events,
                complications,
                self.readmission_days).cache()
            deaths[key] = self.utils.findDeathAfterEvent(inpatient_events,
                self.readmission_days,
                self.date_input_format).cache()

            # create a dataframe to summarize the provider total procedure count and complication rate
            providerEventCount = self.utils.countProviderOccurrence(inpatient_events, 
                self.sqlContext).withColumnRenamed("COUNT", "PROCEDURE_COUNT").cache()
            providerComplicationCount = self.utils.countProviderOccurrence(readmissionDfs[key], 
                self.sqlContext).withColumnRenamed("COUNT", "READMISSION_COUNT").cache()
            providerDeathCount = self.utils.countProviderOccurrence(deaths[key],
                self.sqlContext).withColumnRenamed("COUNT", "DEATH_COUNT").cache()
            providerProcedureInfo = providerEventCount.join(providerComplicationCount, 'PROVIDER_ID', how='left')
            providerProcedureInfo = providerProcedureInfo.fillna(0)
            providerProcedureInfo = providerProcedureInfo.join(providerDeathCount, 'PROVIDER_ID', how='left')
            providerProcedureInfo = providerProcedureInfo.fillna(0)
            providerProcedureInfo = providerProcedureInfo.withColumn('COMPLICATION_COUNT',
                providerProcedureInfo.READMISSION_COUNT + providerProcedureInfo.DEATH_COUNT)
            providerProcedureInfo = providerProcedureInfo.withColumn('PERCENTAGE', 
                providerProcedureInfo.COMPLICATION_COUNT/providerProcedureInfo.PROCEDURE_COUNT)
            providerProcedureInfoDfs[key] = providerProcedureInfo
        return readmissionDfs,providerProcedureInfoDfs,deaths


    #
    # find persons that have been readmitted to the hospital
    # OMOP tables are global so do not need to be passed to the function
    # dates must be date objects
    #
    def findReadmissionPersons(self, inpatient_events, complications, days):
        inpatient_events.registerTempTable('inpatient_events')
        complications.registerTempTable('complications')
        sqlString = "select distinct inpatient_events.PERSON_ID, inpatient_events.VISIT_END_DATE, inpatient_events.PROVIDER_ID, complications.SOURCE_VALUE from inpatient_events join complications where inpatient_events.PERSON_ID=complications.PERSON_ID and inpatient_events.VISIT_END_DATE < complications.VISIT_START_DATE and complications.VISIT_START_DATE < date_add(inpatient_events.VISIT_END_DATE," + days + ")"
        df =  self.sqlContext.sql(sqlString)
        return df

    #
    #  For a particular icd code, count the number of occurrences
    #  This is done by summing the count values in condition_occurrence and procedure_occurrence
    #  Tables condition_occurrence and procedure_occurrence are global
    #
    def icdGrouping(self, sqlContext):
        icd_co = sqlContext.sql("select CONDITION_SOURCE_VALUE SOURCE_VALUE, count(*) COUNT_CO \
                                    from condition_occurrence group by CONDITION_SOURCE_VALUE")
        icd_po = sqlContext.sql("select PROCEDURE_SOURCE_VALUE SOURCE_VALUE, count(*) COUNT_PO \
                                    from procedure_occurrence group by PROCEDURE_SOURCE_VALUE")
        icd_all = icd_co.join(icd_po,'SOURCE_VALUE', how='outer').fillna(0)
        icd_all = icd_all.withColumn('COUNT', icd_all.COUNT_CO + icd_all.COUNT_PO)
        return icd_all

    #
    #  For a particular icd code, count the number of principal admission diagnosis codes for patients
    #  undergoing each of the procedures.
    #
    def icdGroupingPrimary(self, data):
        icd_co_temp = self.utils.filterDataframeByCodes(data['condition_occurrence'],
                self.inpatient_condition_primary_diagnosis,
                'CONDITION_TYPE_CONCEPT_ID')
        icd_po_temp = self.utils.filterDataframeByCodes(data['procedure_occurrence'],
                self.inpatient_condition_primary_diagnosis,
                'PROCEDURE_TYPE_CONCEPT_ID')
        icd_co_temp.registerTempTable('condition_occurrence_primary')
        icd_po_temp.registerTempTable('procedure_occurrence_primary')
        icd_co = self.sqlContext.sql("select CONDITION_SOURCE_VALUE SOURCE_VALUE, count(*) COUNT_CO \
                                    from condition_occurrence_primary group by CONDITION_SOURCE_VALUE")
        icd_po = self.sqlContext.sql("select PROCEDURE_SOURCE_VALUE SOURCE_VALUE, count(*) COUNT_PO \
                                    from procedure_occurrence_primary group by PROCEDURE_SOURCE_VALUE")
        icd_all = icd_co.join(icd_po,'SOURCE_VALUE', how='outer').fillna(0)
        icd_all = icd_all.withColumn('COUNT', icd_all.COUNT_CO + icd_all.COUNT_PO)
        return icd_all


    #
    # find counts of icd codes
    # If primary_only flag is set, only count those icd codes designated as primary inpatient codes
    #
    def writeCodesAndCount(self, sqlContext, codes, directory, filename, primary_only):
        if not os.path.exists(directory):
            os.makedirs(directory)
        if primary_only:
            # look only for icd codes that are primary inpatient
            icd_all = self.icdGroupingPrimary(sqlContext, self.data, inpatient_condition_primary_diagnosis, inpatient_procedure_primary_diagnosis).toPandas()
        else:
            # look at all icd codes
            icd_all = self.icdGrouping(sqlContext).toPandas()
        icd_def = self.utils.readFileIcd9('icd/icd9/CMS32_DESC_LONG_DX.txt')  # read icd9 definitions into dict
        f = open(os.path.join(directory,filename), "w")
        total_for_all = 0
        for key, value in codes.items():
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
        f.close()

    #
    #  For a particular icd code, count the number of occurrences
    #  This is done by summing the count values in condition_occurrence and procedure_occurrence
    #  Tables condition_occurrence and procedure_occurrence are global
    #
    def readmissionGrouping(self, sqlContext, readmission):
        readmission.registerTempTable('readmissioN')
        icd_count = sqlContext.sql("select SOURCE_VALUE, count(*) COUNT from readmission group by SOURCE_VALUE")
        return icd_count

    #
    # find code counts for readmission event
    #
    def writeReadmissionCodesAndCount(self, sqlContext, codes, readmissionDfs, directory, filename):
        if not os.path.exists(directory):
            os.makedirs(directory)
        icd_def = self.utils.readFileIcd9('icd/icd9/CMS32_DESC_LONG_DX.txt')  # read icd9 definitions into dict
        f = open(os.path.join(directory,filename), "w")
        total_for_all = 0
        for key, value in codes.items():
            icd_all = self.readmissionGrouping(sqlContext, readmissionDfs[key]).toPandas()
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
        f.close()

