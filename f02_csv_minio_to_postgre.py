##### Import Library

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import *



##### Initiation

HOST_TEMP = sys.argv[1]
PORT_TEMP = sys.argv[2]
USER_TEMP = sys.argv[3]
PASSWORD_TEMP = sys.argv[4]
DB_TEMP = sys.argv[5]
target_table = sys.argv[6]

client = sys.argv[7]

FILENAME = sys.argv[8]
SCHEMA = sys.argv[9]

DELIMITER = sys.argv[10]

MODE = sys.argv[11]

ROW_LIMIT = int(sys.argv[12])





##### Spark Session

scSpark = SparkSession.builder.getOrCreate()
print("BERHASIL BUILD SPARK SESSION BERIKUT INI: ", scSpark)



##### Function (PostgreSQL)

def extract_data_from_minio_csv(client_name,filename,csv_schema,delimiter,row_limit,spark_session=scSpark):
    
    filepath = f"{client_name}/files/{filename}"
    print(f"Read data from {filepath}")
    
    data_csv = spark_session.read.format("csv") \
            .option("header", "true") \
            .option("delimiter", delimiter) \
            .schema(csv_schema) \
            .load(f"s3a://{filepath}") \
            .limit(row_limit)
    
    return data_csv

def load_data_transformation(dataframe, host='', port='', user='', password='', db='', table='', mode='overwrite', spark_session=scSpark):
    
    # JDBC connection details
    driver = "org.postgresql.Driver"
    URI = f"jdbc:postgresql://{host}:{port}/{db}"
    
    # JDBC Connection and load table in Dataframe
    dataframe.write.format('jdbc').options(
        url=URI,
        driver=driver,
        dbtable=table,
        user=user,
        password=password).mode(mode).save()

    return print(f"{table} has been loaded to temporary database.")



processed_data = extract_data_from_minio_csv(client_name=client, 
                                             filename=FILENAME, 
                                             csv_schema=SCHEMA,
                                             delimiter=DELIMITER,
                                             row_limit=ROW_LIMIT, 
                                             spark_session=scSpark)

##################################################################################

load_data_transformation(host=HOST_TEMP, 
                        port=PORT_TEMP, 
                        user=USER_TEMP, 
                        password=PASSWORD_TEMP, 
                        db=DB_TEMP, 
                        table=target_table, 
                        dataframe=processed_data,
                        mode='overwrite', 
                        spark_session=scSpark)


scSpark.stop()