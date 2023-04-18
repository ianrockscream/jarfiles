##### Import Library

import sys

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

INPUT_COLUMNS = (sys.argv[9])
print(INPUT_COLUMNS)

INPUT_VALUE = (sys.argv[10])
print(INPUT_VALUE)

INPUT_IS_NUMERIC = (sys.argv[11])
print(INPUT_IS_NUMERIC)


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



def fill_null(list_null_column=['id'], fill_value=0, temp_view_input='temp_view_table', temp_view_output='temp_view_table', spark_session=scSpark):
    
    temp_view_input = f"{client}_tbl_{temp_view_input}"
    temp_view_output = f"{client}_tbl_{temp_view_output}"

    df_processed = spark_session.sql(f"SELECT * FROM {temp_view_input}")
    df_final = df_processed.fillna(value=fill_value, subset=list_null_column)
    df_final.printSchema()
    
    df_final.createOrReplaceTempView(temp_view_output)
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
if INPUT_IS_NUMERIC.lower() == "true":
    try:
        INPUT_VALUE = int(INPUT_VALUE)
    except ValueError:
        INPUT_VALUE = float(INPUT_VALUE)
    except:
        INPUT_VALUE = "RE-CHECK INPUT VALUE"

INPUT_FILL_NULL = ([i.strip() for i in INPUT_COLUMNS.split(",")], INPUT_VALUE)
NULL_COLUMNS = list(list(INPUT_FILL_NULL)[0])
NULL_REPLACE_VALUE = list(INPUT_FILL_NULL)[1]

OPERATOR_ID_PREV = 'B001'     # Previous Operator ID
OPERATOR_ID_CURR = 'B002' # Current Operator ID

temp_view_next = fill_null(list_null_column=NULL_COLUMNS, 
                           fill_value=NULL_REPLACE_VALUE, 
                           temp_view_input=OPERATOR_ID_PREV, temp_view_output=OPERATOR_ID_CURR, 
                           spark_session=scSpark)
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