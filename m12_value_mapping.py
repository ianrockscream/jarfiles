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
source_table = sys.argv[6]
target_table = sys.argv[7]

client = sys.argv[8]

INPUT_WHEN = (sys.argv[9])
print(INPUT_WHEN)
INPUT_ELSE = (sys.argv[10])
print(INPUT_ELSE)
INPUT_NEW_COLUMN = (sys.argv[11])
print(INPUT_NEW_COLUMN)



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



def value_mapping(obj,spark_session = scSpark):
    query = 'CASE '
    temp_view_input = f"{client}_tbl_{obj['temp_view_input']}"
    temp_view_output = f"{client}_tbl_{obj['temp_view_output']}"

    def cast_float(value_cond):
        try : 
            value_cond = float(value_cond)
        except ValueError :
            value_cond = "'{}'".format(value_cond)
        return value_cond

    for when in obj['when']:
        query = query + "WHEN "
        if len(when['conditions'])==1:
            condition = when['conditions'][0]
            value_cond = cast_float(condition['value'])
            asValue_when = cast_float(when['asValue'])
            query = query + "{} {} {} THEN {} ".format(condition['column'],condition['operation'],value_cond,asValue_when)
            print(query)
        else :
            for index, condition in enumerate(when['conditions']) :
                value_cond = cast_float(condition['value'])
                query = query + "{} {} {} {} ".format(condition['column'],condition['operation'],value_cond,when['relation'][index])
            asValue_when = cast_float(when['asValue'])
            query = query + f"THEN {asValue_when} "
            print(query)
    value_else = cast_float(obj['else'])
    query = query + f"ELSE {value_else} END"
    print(query)

    df = spark_session.sql(f"select * from {temp_view_input}")
    df = df.withColumn(obj['newColumn'],expr(query))
    df.printSchema()
    df.show()
    df.createOrReplaceTempView(temp_view_output)
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
INPUT_WHEN = json.loads(INPUT_WHEN)
print(INPUT_WHEN)

OPERATOR_ID_PREV = 'B001' # Previous Operator ID
OPERATOR_ID_CURR = 'B002' # Current Operator ID

obj_val = {
    'when': INPUT_WHEN,
    'else': INPUT_ELSE,
    'newColumn':INPUT_NEW_COLUMN,
    'temp_view_input': OPERATOR_ID_PREV,
    'temp_view_output': OPERATOR_ID_CURR
}

val_temp_view = value_mapping(obj_val,spark_session=scSpark)
scSpark.sql(f"select * from {val_temp_view}").show()
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