
set SPARK_HOME = C:\Users\ext-shambmi\spark-2.4.3-bin-hadoop2.7
set HADOOP_HOME = C:\Users\ext-shambmi\winutils
set PATH += C:\Users\ext-shambmi\spark-2.4.3-bin-hadoop2.7\bin
set PATH += C:\ProgramData\Anaconda3\


rem export PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH
rem export PYTHONPATH=$SPARK_HOME/python/lib/py4j-0.10.3-src.zip:$PYTHONPATH

rem remove the old datacohort files
rem rm -r -f ../datacohort/*

rem run main
python ./scorecard/SurgeonScorecard.py
