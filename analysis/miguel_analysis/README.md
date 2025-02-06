This is a standalone python application that loads the clean_data.orc file from HDFS, 
and then performs an analysis to create a dataframe that can be imported into PowerBI.

.env contains constants that are loaded into the os environment, and keep different configurations seperate.
It currently looks like this:

HDFS_MASTER=hdfs://localhost:9000
HDFS_USER=miguel674-rev
HDFS_FILENAME=clean_data.orc

I am reusing some classes created in earlier projects for quicker development.

Use cd to change your current working directory to this folder, and then run the app with python3 main.py.
You will otherwise get the error that the directory None/user/None/None does not exist, this is because the load_dotenv function uses relative paths.
