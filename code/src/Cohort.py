import os
import ConfigParser
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import Utils

class Cohort:
    #
    #  Filter out users by using values set in
    #  property files.  If no value is set then the
    #  filter is not applied
    #
    def __init__(self, data, config, sqlContext):
       utils = Utils.Utils(sqlContext)
       self.sqlContext = sqlContext
       # set instance variables based on properties
       self.env = config.get('branch','env')
       self.year_of_birth_min = config.get(self.env+'.cohort','year_of_birth_min')
       self.year_of_birth_max = config.get(self.env+'.cohort','year_of_birth_max')
       self.events_start_date = config.get(self.env+'.cohort','events_start_date')
       self.events_end_date = config.get(self.env+'.cohort','events_end_date')
       self.filter_dead = config.get(self.env+'.cohort','filter_dead')
       self.filter_alive = config.get(self.env+'.cohort','filter_alive')
       self.filter_male = config.get(self.env+'.cohort','filter_male')
       self.filter_female = config.get(self.env+'.cohort','filter_female')
       self.write_csv_output = config.get(self.env+'.cohort','write_csv_output')
       self.csv_output_dir = config.get(self.env+'.cohort','csv_output_dir')
       self.csv_output_codec = config.get(self.env+'.cohort','csv_output_codec')
       self.filter_care_sites = config.get(self.env+'.cohort','filter_care_sites').split(",")
       self.inpatient_only = config.get(self.env+'.cohort','inpatient_only')
       if not self.filter_care_sites[0]:
            self.filter_care_sites = []
       self.include_care_sites = config.get(self.env+'.cohort','include_care_sites').split(",")
       if not self.include_care_sites[0]:
            self.include_care_sites = []

       # apply filter functions
       if self.year_of_birth_max is not None:
           self.filterByMaxYearOfBirth(data)
       if self.year_of_birth_min is not None:
           self.filterByMinYearOfBirth(data)
       if self.filter_male == "True":
           self.filterMale(data)
       if self.filter_female == "True":
           self.filterFemale(data)
       if len(self.filter_care_sites) > 0:
           self.filterCareSites(data)
       if len(self.include_care_sites) > 0:
           self.includeCareSites(data)
       if len(self.events_start_date) != 0 or len(self.events_end_date) != 0:
           self.filterByEventDate(data, self.events_start_date, self.events_end_date)
       # write the filtered data back out to files
       if self.write_csv_output == "True":
          utils.writeRawData(data,self.csv_output_codec,self.csv_output_dir)
       # filter by users who have not had an inpatient stay
       if self.inpatient_only == "True":
          self.filterInpatientOnly(data)
       # after filtering data, reset the caches
       self.resetDataCache(data)
           
    # reset data caches
    def resetDataCache(self, data):
        for key, value in data.iteritems():
            data[key].registerTempTable(key)
            data[key].cache()
            

    # filter by maximum year of birth
    def filterByMaxYearOfBirth(self, data):
        data['person'] = data['person'].filter(data['person'].YEAR_OF_BIRTH <= self.year_of_birth_max)

    # filter by minimum year of birth
    def filterByMinYearOfBirth(self, data):
        data['person'] = data['person'].filter(data['person'].YEAR_OF_BIRTH >= self.year_of_birth_min)

    #
    # filter by minimum events
    # currently only filters data from condition_occurrence, procedure_occurrence, visit_occurrence, measurement,
    # observation, and device_exposure
    #
    def filterByEventDate(self, data, start_date, end_date):
         if start_date is not None and len(start_date) != 0:
            data['condition_occurrence'] = data['condition_occurrence'].filter(data['condition_occurrence'].CONDITION_START_DATE >= start_date)
            data['procedure_occurrence'] = data['procedure_occurrence'].filter(data['procedure_occurrence'].PROCEDURE_DATE >= start_date)
            data['visit_occurrence'] = data['visit_occurrence'].filter(data['visit_occurrence'].VISIT_START_DATE >= start_date)
            data['measurement'] = data['measurement'].filter(data['measurement'].MEASUREMENT_DATE >= start_date)
            data['observation'] = data['observation'].filter(data['observation'].OBSERVATION_DATE >= start_date)
            data['device_exposure'] = data['device_exposure'].filter(data['device_exposure'].DEVICE_EXPOSURE_START_DATE >= start_date)
         if end_date is not None and len(end_date) != 0:
            data['condition_occurrence'] = data['condition_occurrence'].filter(data['condition_occurrence'].CONDITION_END_DATE <= end_date)
            data['procedure_occurrence'] = data['procedure_occurrence'].filter(data['procedure_occurrence'].PROCEDURE_DATE <= end_date)
            data['visit_occurrence'] = data['visit_occurrence'].filter(data['visit_occurrence'].VISIT_END_DATE <= end_date)
            data['measurement'] = data['measurement'].filter(data['measurement'].MEASUREMENT_DATE <= end_date)
            data['observation'] = data['observation'].filter(data['observation'].OBSERVATION_DATE <= end_date)
            data['device_exposure'] = data['device_exposure'].filter(data['device_exposure'].DEVICE_EXPOSURE_END_DATE <= end_date)
        

    # filter out Male patients
    def filterMale(self, data):
        data['person'] = data['person'].filter(data['person'].GENDER_SOURCE_VALUE != 'Male')

    # filter out Female patients
    def filterFemale(self, data):
        data['person'] = data['person'].filter(data['person'].GENDER_SOURCE_VALUE != 'Female')

    # filter out dead patients
    def filterDead(self, data):
        pass

    # filter out alive patients
    def filterAlive(self, data):
        pass

    # filter out persons who have these primary care sites
    def filterCareSites(self, data):
        # make sure null is not filtered also when filtering my list by adding an additional check
        data['person'] = data['person'].filter(~data['person'].CARE_SITE_ID.isin(self.filter_care_sites) | data['person'].CARE_SITE_ID.isNull())

    # include only persons that have primary care sites
    def includeCareSites(self, data):
        data['person'] = data['person'].where(data['person'].CARE_SITE_ID.isin(self.include_care_sites))

    # filter out users that have not had an inpatient stay
    # first join person with visit_occurrence then drop the visit_occurrence column and get rid of duplicates
    def filterInpatientOnly(self, data):
        df = self.sqlContext.sql("select person.*, visit_occurrence.VISIT_OCCURRENCE_ID from person inner join visit_occurrence on person.PERSON_ID=visit_occurrence.PERSON_ID")
        df = df.drop('VISIT_OCCURRENCE_ID')
        df = df.dropDuplicates(['PERSON_ID'])
        data['person'] = df
