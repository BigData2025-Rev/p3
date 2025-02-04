This is a standalone python application that loads the clean_data.orc file from HDFS, 
and then performs an analysis to create a dataframe that can be imported into PowerBI.

.env contains constants that are loaded into the os environment, and keep different configurations seperate.

I am reusing some classes created earlier.

Use cd to change your current working directory to this folder, and then run the app with python3 main.py.