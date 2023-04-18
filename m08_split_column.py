##### Import Library

import sys

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import functions
from pyspark.sql.window import Window
from pyspark.sql.functions import lit, when
from pyspark.sql.functions import col
from pyspark.sql.functions import regexp_replace, concat
from pyspark.sql.functions import lower, upper, initcap
from pyspark.sql.functions import row_number,rank,dense_rank,lag,lead,avg,sum,count
from pyspark.sql.functions import split
from pyspark.sql.functions import substring
from pyspark.sql.functions import datediff,months_between,current_date, expr, date_format,unix_timestamp,date_add,date_sub
from pyspark.sql.functions import *



##### Initiation

HOST_TEMP = sys.argv[1]
PORT_TEMP = sys.argv[2]
USER_TEMP = sys.argv[3]
PASSWORD_TEMP = sys.argv[4]
DB_TEMP = sys.argv[5]
source_table = sys.argv[6]
target_table = sys.argv[7]

client = sys.argv[8]

INPUT_COLUMN_SOURCE = (sys.argv[9])
print(INPUT_COLUMN_SOURCE)

INPUT_DELIMITER = (sys.argv[10])
print(INPUT_DELIMITER)

INPUT_COLUMN_SOURCE_IS_KEEP = (sys.argv[11])
print(INPUT_COLUMN_SOURCE_IS_KEEP)

INPUT_COLUMN_TARGET_COMMA_SEPARATED = (sys.argv[12])
print(INPUT_COLUMN_TARGET_COMMA_SEPARATED)


##### Spark Session

scSpark = SparkSession.builder.getOrCreate()
print("BERHASIL BUILD SPARK SESSION BERIKUT INI: ", scSpark)



##### Function (PostgreSQL)

def extract_data_transformation(host='', port='', user='', password='', db='', table='', temp_view_output='temp_view_table', spark_session=scSpark):
    
    # temp_view_input = f"{client}_tbl_{temp_view_input}"
    temp_view_output = f"{client}_tbl_{temp_view_output}"
    
    # JDBC connection details
    driver = "org.postgresql.Driver"
    URI = f"jdbc:postgresql://{host}:{port}/{db}"
    
    # JDBC Connection and load table in Dataframe
    input_data = spark_session.read.format("jdbc") \
                .option("driver", driver) \
                .option("url", URI) \
                .option("dbtable", table) \
                .option("user", user) \
                .option("password", password) \
                .load()

    input_data.createOrReplaceTempView(temp_view_output)
    print(f"{temp_view_output} has been created!")
    
    return temp_view_output



def load_data_transformation(host='', port='', user='', password='', db='', table='', temp_view_input='temp_view_table', mode='overwrite', spark_session=scSpark):
    
    temp_view_input = f"{client}_tbl_{temp_view_input}"
    # temp_view_output = f"{client}_tbl_{temp_view_output}"
    
    # JDBC connection details
    driver = "org.postgresql.Driver"
    URI = f"jdbc:postgresql://{host}:{port}/{db}"
    
    output_data = spark_session.sql(f"SELECT * FROM {temp_view_input}")

    # JDBC Connection and load table in Dataframe
    output_data.write.format('jdbc').options(
        url=URI,
        driver=driver,
        dbtable=table,
        user=user,
        password=password).mode(mode).save()

    print(f"{temp_view_input} has been exported!")
    
    return spark_session.sql(f"SELECT * FROM {temp_view_input}").show()



def split_column(obj,spark_session=scSpark):
    temp_view_input = f"{client}_tbl_{obj['temp_view_input']}"
    temp_view_output = f"{client}_tbl_{obj['temp_view_output']}"
    df = spark_session.sql(f"select * from {temp_view_input}")
    if obj['keep'] == True:
        split_col = split(df[obj['column']],obj['delimiter'],limit = len(obj['new_col']))
    else:
        split_col = split(df[obj['column']],obj['delimiter'])
    
    for i,val in enumerate(obj['new_col']):
        df = df.withColumn(val,split_col.getItem(i))
    #create output
    df.createOrReplaceTempView(temp_view_output)
    spark_session.catalog.dropTempView(temp_view_input)
    return temp_view_output




##### Operator Execution

OPERATOR_ID_PREV = ''     # Previous Operator ID
OPERATOR_ID_CURR = 'B001' # Current Operator ID

temp_view_next = extract_data_transformation(host=HOST_TEMP, 
                                             port=PORT_TEMP, 
                                             user=USER_TEMP, 
                                             password=PASSWORD_TEMP, 
                                             db=DB_TEMP, 
                                             table=source_table, 
                                             temp_view_output=OPERATOR_ID_CURR, 
                                             spark_session=scSpark)
##################################################################################
if INPUT_COLUMN_SOURCE_IS_KEEP.lower() == 'true':
    INPUT_COLUMN_SOURCE_IS_KEEP = True
elif INPUT_COLUMN_SOURCE_IS_KEEP.lower() == 'false':
    INPUT_COLUMN_SOURCE_IS_KEEP = False
else:
    INPUT_COLUMN_SOURCE_IS_KEEP = None

NEW_COLUMN_TARGET = [i.strip() for i in INPUT_COLUMN_TARGET_COMMA_SEPARATED.split(",")]

OPERATOR_ID_PREV = 'B001' # Previous Operator ID
OPERATOR_ID_CURR = 'B002' # Current Operator ID

obj_split = {
    'column': INPUT_COLUMN_SOURCE,
    'delimiter': INPUT_DELIMITER,
    'keep': INPUT_COLUMN_SOURCE_IS_KEEP,
    'new_col': NEW_COLUMN_TARGET,
    'temp_view_input': OPERATOR_ID_PREV,
    'temp_view_output': OPERATOR_ID_CURR
}

split_temp_view = split_column(obj_split,spark_session=scSpark)
scSpark.sql(f"select * from {split_temp_view}").show()
##################################################################################
OPERATOR_ID_PREV = 'B002' # Previous Operator ID
OPERATOR_ID_CURR = 'B003' # Current Operator ID

load_data_transformation(host=HOST_TEMP, 
                         port=PORT_TEMP, 
                         user=USER_TEMP, 
                         password=PASSWORD_TEMP, 
                         db=DB_TEMP, 
                         table=target_table, 
                         temp_view_input=OPERATOR_ID_PREV, 
                         mode='overwrite', 
                         spark_session=scSpark)


scSpark.stop()