
Surgeon Scorecard
-----------------

The Surgeon Scorecard calculates readmission complication rates for surgeons based on a configurable period.  This is an attempt to reproduce the Surgeon Scorecard work done by Pro Publica to publically identify the performance of surgeons.   Though they have extensive documentation on [their methods](https://static.propublica.org/projects/patient-safety/methodology/surgeon-level-risk-methodology.pdf) they did not release source code.

It uses the following code libraries included in this github repository:

Cohort.py -  The command line Cohort tool can take a OMOP dataset that is too large to fit into a relational database and pull a subset of the data that can fit.  It is meant to be a command line replacement for the existing OMOP tool CIRCE when the datasize is too large to fit into a relational database.  It provides the ability to filter the data and write it out into CSV files that can be loaded into a relational database.

Readmission.py - The Readmission library takes a Cohort and finds readmissions after an inpatient stay.

## Installation

1. `git clone https://github.com/opme/SurgeonScorecard.git`
    
### Requirements

* Scala 2.x
* Python 2.7 with Pandas
* File system for holding CSV files
* More than 8GB of RAM (Smaller ram sizes can handle the development dataset such as data1k but 40G is needed to hold the full Medicare SynPuf data in memory without using any disk.  Run time can increase 3 to 5x when the data is not able to fit into memory)

## Example

Edit the file run.sh to point to the correct location of the spark binary

```
$ cd code
$ ./run.sh

```

## Program Options

The program is driven by a local.properties and environ.properties file.

```
spark properties:
 driver_memory - amount of memory for the driver to use.  Larger values allow more data to be cached in memory
 shuffle_partition - Amount of partitions to be used in a dataframe.  Larger data sizes need more partitions

data properties
 datadir- directory for omop source data
 icd_conversion- not currently implemented.  
 date_input_format- data format used such as %Y%m%d or %Y-%m-%d

cohort properties:
 year_of_birth_min- minimum year of birth for patients in the cohort
 year_of_birth_max- maximum year of birth for patients in the cohort.  no setting means no limit
 events_start_date- filter events that occur before this date
 events_end_date- filter events that occur after this date
 filter-dead- filter out dead patients
 filter-alive- filter out alive patients
 filter-male- filter out male patients
 filter-female- filter out female patients
 filter_care_sites- filter out patients from particular care sites.
    This is a comma seperated list of care site id's
 include_care_sites- include patients from only specific care sites
    This is a comma seperated list of care site id's
    This cannot be used if you are filtering on care sites
 csv_output_codec - choices of bzip2, deflate, uncompressed, lz4, gzip, snappy, none

readmission properties:
 readmission_days- number of days to check for readmission complication
 readmission_code_file- file that contains the procedure and readmission codes
 diagnostic_code_file- file that contains the procudure and diagnostic codes
 comorbidies_code_file- file that contains the comorbity codes
 icd_diagnosis- property to show whether the diagnostic icd codes are icd9 or icd10
 icd_readmission- property to show whether the diagnostic icd codes are icd9 or icd10
 inpatient_condition_primary_diagnosis- OMOP codes for condition principal diagnosis
 inpatient_procedure_primary_diagnosis- OMOP codes for procedure principal diagnosis

```

## Data

These are the options for input dataset

1. a 1000 patient subset of the Medicare synpuf database is included in the data1k folder.  It is compressed and the program can read it this way.
2. Scripts in the dscripts can be used to download the full Medicare Synpuf dataset that has been converted to an OMOP framework from the OHDSI ftp site.  This data is compressed and the program can read it this way.
3. Use the real Medicare CMS data and run the [etl-cms](https://github.com/OHDSI/ETL-CMS/tree/unm-improvements/python_etl) project to covert it to an OMOP format

## Test

The Surgeon Scorecard is a test application for the Cohort and Readmission classes.

There are no unit tests

## Future Work

* Other Complimentary python libraries such as one that convert between medical codes.  For example, icd9 to icd10.
* Suppport for mixed icd9 (pre 2015) and icd10 codes (2015+) CMS Medicare data.
* Publish python classes as a library in Pypi

## Reference Paper

The reference paper is included in the git repository [here](https://github.com/opme/SurgeonScorecard/blob/master/Surgeon_Scorecard.pdf)
