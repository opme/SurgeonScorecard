#!/bin/sh
export SPARK_HOME=/opt/spark
export PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH
export PYTHONPATH=$SPARK_HOME/python/lib/py4j-0.10.3-src.zip:$PYTHONPATH

# remove the old datacohort files
#rm -r -f ../datacohort/*

# run main
python ./scorecard/SurgeonScorecard.py
