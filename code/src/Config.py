import os
import sys
import ConfigParser

class Config:
    def __init__(self):
       pass

    #
    # a simple function to read an array of configuration files into a config object
    #
    def read_config(self, cfg_files):
        if(cfg_files != None):
            config = ConfigParser.RawConfigParser()
        # merges all files into a single config
        for i, cfg_file in enumerate(cfg_files):
            if(os.path.exists(cfg_file)):
                config.read(cfg_file)
        if(config == None):
            print "####################################"
            print "Did not find any configuration files"
            print "####################################"
            sys.exit(0)
        return config

    #
    # Validate properties
    #
    def validateProperties(self, config):
        env = config.get('branch','env')

        # Cannot set both filter_care_sites and include_care_sites
        filter_care_sites = config.get(env+'.cohort','filter_care_sites').split(",")
        if not filter_care_sites[0]:
            filter_care_sites = []
        include_care_sites = config.get(env+'.cohort','include_care_sites').split(",")
        if not include_care_sites[0]:
            include_care_sites = []
        if (len(filter_care_sites) > 0 and len(include_care_sites) > 0):
            print "###########################################################################"
            print "Cannot set both filter_care_sites and include_care_sites in properties file"
            print "###########################################################################"
            sys.exit(0)

        # If the user wants to dump the cohort back out to csv files, make sure the files
        # do not already exist
        write_csv_output = config.get(env+'.cohort','write_csv_output')
        csv_output_dir = config.get(env+'.cohort','csv_output_dir')
        if (write_csv_output == "True" and len(os.listdir(csv_output_dir)) > 0):
            print "########################################"
            print " Files already exist in output directory"
            print "########################################"
            sys.exit(0)

