import os
import ConfigParser
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
       self.readmissionDfs,self.providerProcedureInfoDfs,self.deaths = self.findReadmissionPatients(data, config, self.diagnostic_codes, self.readmission_codes)

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
    def findReadmissionPatients(self, data, config, diagnostic_codes, readmission_codes):
        # find readmission patients for each procedure
        readmissionDfs = {}  # dict of dataframe of readmission patients for each procedure
        providerProcedureInfoDfs = {}  # dict of provider event counts for each procedure
        deaths = {} # dict of patients who died for each procedure
        for key, value in diagnostic_codes.iteritems():
            # make sure we have readmission codes for this procedure
            if key not in readmission_codes:
                print "No readmission codes for this procedure.  This procedure " + key + " will be skipped. "
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
            readmissionDfs[key] = self.utils.findReadmissionPersons(inpatient_events,
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

