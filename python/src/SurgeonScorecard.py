import os
import pandas as pd
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import Config
import Utils
import Cohort
import Model
import Readmission

#
# http://www.nodalpoint.com/spark-dataframes-from-csv-files/
#
def main():
    # read properties files
    cfg = Config.Config()
    config = cfg.read_config(['local.properties', 'environ.properties'])
    # validate the properties
    cfg.validateProperties(config)
    # get the current branch (from local.properties)
    env = config.get('branch','env')
    datadir = config.get(env+'.data','datadir')
    resultdir = config.get(env+'.readmission','resultdir')
    driver_memory = config.get(env+'.spark','driver_memory')
    shuffle_partitions = config.get(env+'.spark','shuffle_paritions')

    # Spark Configurations
    pyspark_submit_args = ' --driver-memory ' + driver_memory + ' pyspark-shell'
    os.environ["PYSPARK_SUBMIT_ARGS"] = pyspark_submit_args
    conf = SparkConf()
    conf.set("spark.master", "local[*]")
    conf = conf.setAppName('Surgeon Scorecard')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    sqlString = "set spark.sql.shuffle.partitions=" + shuffle_partitions
    sqlContext.sql(sqlString)

    # load utils
    util = Utils.Utils(sqlContext)

    # read in the data
    data = util.loadRawData(sc, datadir)

    # calculate some statistics related to primary diagnosis code
    # write a file with counts of all condition_occurrence CONDITION_TYPE_CONCEPT_ID
    df = util.conditionTypeConceptCount(sqlContext)
    util.saveDataframeAsSingleFile(df,resultdir,"condition_type_concept_count.csv")
    # write a file with counts of all procedure_occurrence PROCEDURE_TYPE_CONCEPT_ID
    df = util.procedureTypeConceptCount(sqlContext)
    util.saveDataframeAsSingleFile(df,resultdir,"procedure_type_concept_count.csv")

    # create a cohort of users and their associated data
    # The cohort and event data is filtered based on properties
    cohort = Cohort.Cohort(data, config, sqlContext)
    print("Number of patients in cohort: " + str(data['person'].count()))

    # Find readmission events for this cohort for procedures of interest
    readmit = Readmission.Readmission(data, config, sc, sqlContext)

    # write the results to csv files
    for key, value in readmit.providerProcedureInfoDfs.iteritems():
        print("Writing provider data for: " + key)
        filename = key + ".csv"
        util.saveDataframeAsSingleFile(value, resultdir, filename)

    # calculate statistics related to codes
    # write a file with counts of all diagnostic icd codes
    readmit.writeCodesAndCount(sqlContext, readmit.diagnostic_codes, resultdir, 'procedure_count_all.txt', False)
    # write a file with counts of all diagnostic icd codes where the code is primary
    readmit.writeCodesAndCount(sqlContext, readmit.diagnostic_codes, resultdir, 'procedure_count_primary.txt', True)
    # write a file with counts of all readmission icd codes
    readmit.writeCodesAndCount(sqlContext, readmit.readmission_codes, resultdir, 'readmission_count_all.txt', False)
    # write a file with counts of all readmission icd codes where the code is primary
    readmit.writeCodesAndCount(sqlContext, readmit.readmission_codes, resultdir, 'readmission_count_primary.txt', True)
    # write a file with counts of readmission events by code
    readmit.writeReadmissionCodesAndCount(sqlContext, 
        readmit.readmission_codes, 
        readmit.readmissionDfs, 
        resultdir, 
        'readmission_event_code_count.txt')



if __name__ == "__main__":
    main()
