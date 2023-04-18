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

INPUT_COLUMN_PARTITION = (sys.argv[9])
print(INPUT_COLUMN_PARTITION)

INPUT_ORDER_BY = (sys.argv[10])
print(INPUT_ORDER_BY)

INPUT_ORDER = (sys.argv[11])
print(INPUT_ORDER)

INPUT_FUNCTION = (sys.argv[12])
print(INPUT_FUNCTION)



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



def window_function(obj,spark_session=scSpark):
    temp_view_input = f"{client}_tbl_{obj['temp_view_input']}"
    temp_view_output = f"{client}_tbl_{obj['temp_view_output']}"
    df = spark_session.sql(f"select * from {temp_view_input}")
    # define the window function
    if obj['partition'] == '':
        if obj['order']['order'] == 'desc':
            windowSpec  = Window.orderBy(col(obj['order']['order_by']).desc())
        elif obj['order']['order'] == 'asc':
            windowSpec  = Window.orderBy(col(obj['order']['order_by']).asc())
    else:
        if obj['order']['order'] == 'desc':
            windowSpec  = Window.partitionBy(obj['partition']).orderBy(col(obj['order']['order_by']).desc())
        elif obj['order']['order'] == 'asc':
            windowSpec  = Window.partitionBy(obj['partition']).orderBy(col(obj['order']['order_by']).asc())
    # make new col with aggregate and function
    for i in obj['function']:
        if i['func'] == 'rank':
            df = df.withColumn(i['new_column'],rank().over(windowSpec))
        elif i['func'] == 'dense_rank':
            df = df.withColumn(i['new_column'],dense_rank().over(windowSpec))
        elif i['func'] == 'row_number':
            df = df.withColumn(i['new_column'],row_number().over(windowSpec))
        elif i['func'] == 'lag':
            df = df.withColumn(i['new_column'],lag(i['column'],i['offset']).over(windowSpec))
        elif i['func'] == 'lead':
            df = df.withColumn(i['new_column'],lead(i['column'],i['offset']).over(windowSpec))
        elif i['func'] == 'sum':
            df = df.withColumn(i['new_column'],sum(col(i['column'])).over(windowSpec))
        elif i['func'] == 'avg':
            df= df.withColumn(i['new_column'],avg(col(i['column'])).over(windowSpec))
        elif i['func'] == 'count':
            df = df.withColumn(i['new_column'],count(col(i['column'])).over(windowSpec))
    
    df.createOrReplaceTempView(temp_view_output)
    spark_session.catalog.dropTempView(temp_view_input)
    return df.show()



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
FUNCTION = json.loads(INPUT_FUNCTION)
print(FUNCTION)

OPERATOR_ID_PREV = 'B001' # Previous Operator ID
OPERATOR_ID_CURR = 'B002' # Current Operator ID

# func : rank, dense_rank, row_number, sum, avg, count, lag, lead
# order : desc, asc
obj_window = {
    'function': FUNCTION,
    'order':{
        'order_by': INPUT_ORDER_BY,
        'order': INPUT_ORDER
    },
    'partition': INPUT_COLUMN_PARTITION,
    'temp_view_input': OPERATOR_ID_PREV,
    'temp_view_output': OPERATOR_ID_CURR
}

window_function(obj=obj_window,spark_session=scSpark)
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