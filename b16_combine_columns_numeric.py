##### Import Library

import sys
import json

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import functions
from pyspark.sql.functions import lit, when
from pyspark.sql.functions import col
from pyspark.sql.functions import regexp_replace, concat
from pyspark.sql.functions import lower, upper, initcap


##### Initiation

HOST_TEMP = sys.argv[1]
PORT_TEMP = sys.argv[2]
USER_TEMP = sys.argv[3]
PASSWORD_TEMP = sys.argv[4]
DB_TEMP = sys.argv[5]
source_table = sys.argv[6]
target_table = sys.argv[7]

client = sys.argv[8]

INPUT_COLUMN = sys.argv[9]
print(INPUT_COLUMN)




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



def combine_columns_numeric(first_source_column='first_source', second_source_column='second_source', target_column='target', operation='multiply', temp_view_input='temp_view_table', temp_view_output='temp_view_table', spark_session=scSpark):
    
    temp_view_input = f"{client}_tbl_{temp_view_input}"
    temp_view_output = f"{client}_tbl_{temp_view_output}"

    df_processed = spark_session.sql(f"SELECT * FROM {temp_view_input}")
    if operation == 'multiply':
        df_final = df_processed.withColumn(target_column, col(first_source_column) * col(second_source_column))
    elif operation == 'devide':
        df_final = df_processed.withColumn(target_column, col(first_source_column) / col(second_source_column))
    elif operation == 'add':
        df_final = df_processed.withColumn(target_column, col(first_source_column) + col(second_source_column))
    elif operation == 'substract':
        df_final = df_processed.withColumn(target_column, col(first_source_column) - col(second_source_column))
    df_final.printSchema()
    
    df_final.createOrReplaceTempView(temp_view_output)
    # spark_session.catalog.dropTempView(temp_view_input)
    
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
INPUT_COLUMN = json.loads(INPUT_COLUMN)
for input in INPUT_COLUMN:
    FIRST_SOURCE_COLUMN_NAME = input['firstColumn']
    SECOND_SOURCE_COLUMN_NAME = input['secondColumn']
    TARGET_COLUMN_NAME = input['targetColumn']
    OPERATION = input['operation']

    OPERATOR_ID_PREV = 'B001'     # Previous Operator ID
    OPERATOR_ID_CURR = 'B001' # Current Operator ID

    temp_view_next = combine_columns_numeric(first_source_column=FIRST_SOURCE_COLUMN_NAME, 
                                         second_source_column=SECOND_SOURCE_COLUMN_NAME, 
                                         target_column=TARGET_COLUMN_NAME, 
                                         operation=OPERATION, 
                                         temp_view_input=OPERATOR_ID_PREV, temp_view_output=OPERATOR_ID_CURR, 
                                         spark_session=scSpark)
##################################################################################
OPERATOR_ID_PREV = 'B001' # Previous Operator ID
OPERATOR_ID_CURR = 'B002' # Current Operator ID

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