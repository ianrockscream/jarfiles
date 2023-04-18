##### Import Library

import sys
import json

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
source_table_1 = sys.argv[6]
source_table_2 = sys.argv[7]
target_table = sys.argv[8]

client = sys.argv[9]

HOW = sys.argv[10]
JOIN_ON = sys.argv[11]
isduplicate = sys.argv[12]
dataset = sys.argv[13]
duplicate_col = sys.argv[14]




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



def join_table(obj,spark_session = scSpark):
    temp_view_input_left = f"{client}_tbl_{obj['left']}"
    temp_view_input_right = f"{client}_tbl_{obj['right']}"
    temp_view_output = f"{client}_tbl_{obj['temp_view_output']}"
    #rename duplicate column
    df_left = spark_session.sql(f"select * from {temp_view_input_left}")
    if obj['isDuplicate']:
        for kolom in obj['duplicate_col']:
            df_left = df_left.withColumnRenamed(kolom,f"{obj['dataset']}_{kolom}")
    df_left.createOrReplaceTempView(f"{temp_view_input_left}")
    del df_left

    #selecting how
    if obj['how'] == 'left':
        query = f"SELECT * from {temp_view_input_left} l LEFT JOIN {temp_view_input_right} r ON l.{obj['on'][0]['left_on']} == r.{obj['on'][0]['right_on']}"
    elif obj['how'] == 'right':
        query = f"SELECT * from {temp_view_input_left} l RIGHT JOIN {temp_view_input_right} r ON l.{obj['on'][0]['left_on']} == r.{obj['on'][0]['right_on']}"
    elif obj['how'] == 'inner':
        query = f"SELECT * from {temp_view_input_left} l INNER JOIN {temp_view_input_right} r ON l.{obj['on'][0]['left_on']} == r.{obj['on'][0]['right_on']}"
    elif obj['how'] == 'outer':
        query = f"SELECT * from {temp_view_input_left} l FULL OUTER JOIN {temp_view_input_right} r ON l.{obj['on'][0]['left_on']} == r.{obj['on'][0]['right_on']}"
    # multiple condition merge
    for i,val in enumerate(obj['on'][1:]):
        query = query + f" AND l.{obj['on'][i+1]['left_on']} == r.{obj['on'][i+1]['right_on']}"
    #creating output
    print(query)
    spark_session.sql(query).createOrReplaceTempView(temp_view_output)
    spark_session.catalog.dropTempView(temp_view_input_left)
    spark_session.catalog.dropTempView(temp_view_input_right)
    return temp_view_output



##### Operator Execution

OPERATOR_ID_PREV = ''     # Previous Operator ID
OPERATOR_ID_CURR = 'B001' # Current Operator ID

temp_view_next = extract_data_transformation(host=HOST_TEMP, 
                                             port=PORT_TEMP, 
                                             user=USER_TEMP, 
                                             password=PASSWORD_TEMP, 
                                             db=DB_TEMP, 
                                             table=source_table_1, 
                                             temp_view_output=OPERATOR_ID_CURR, 
                                             spark_session=scSpark)

OPERATOR_ID_PREV = ''     # Previous Operator ID
OPERATOR_ID_CURR = 'B002' # Current Operator ID

temp_view_next = extract_data_transformation(host=HOST_TEMP, 
                                             port=PORT_TEMP, 
                                             user=USER_TEMP, 
                                             password=PASSWORD_TEMP, 
                                             db=DB_TEMP, 
                                             table=source_table_2, 
                                             temp_view_output=OPERATOR_ID_CURR, 
                                             spark_session=scSpark)
##################################################################################
JOIN_ON = json.loads(JOIN_ON)
duplicate_col = json.loads(duplicate_col)
if isduplicate.lower() == 'true':
    isduplicate = True
elif isduplicate.lower() == 'false':
    isduplicate = False
else:
    isduplicate = None

OPERATOR_ID_PREV = ['B001' , 'B002'] # Previous Operator ID
OPERATOR_ID_CURR = 'B003' # Current Operator ID

#how : left, right, inner, outer
operator_id = 'OPR001'
obj_join = {
    'left': OPERATOR_ID_PREV[0],
    'right': OPERATOR_ID_PREV[1],
    'how': HOW,
    'on': JOIN_ON,
    'isDuplicate':isduplicate,
    'dataset':dataset,
    'duplicate_col':duplicate_col,
    'temp_view_output': OPERATOR_ID_CURR
}

join_table_temp_view = join_table(obj = obj_join, spark_session =scSpark)
scSpark.sql(f"select * from {join_table_temp_view}").show()
##################################################################################
OPERATOR_ID_PREV = 'B003' # Previous Operator ID
OPERATOR_ID_CURR = 'B004' # Current Operator ID

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