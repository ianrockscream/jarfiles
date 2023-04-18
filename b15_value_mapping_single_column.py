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

INPUT_COLUMN_SOURCE = (sys.argv[9])
print(INPUT_COLUMN_SOURCE)

INPUT_COLUMN_TARGET = (sys.argv[10])
print(INPUT_COLUMN_TARGET)

INPUT_COND = (sys.argv[11])
print(INPUT_COND)

INPUT_VAL = (sys.argv[12])
print(INPUT_VAL)

INPUT_VAL_IS_NUMERIC = (sys.argv[13])
print(INPUT_VAL_IS_NUMERIC)

INPUT_RESULT_TRUE = (sys.argv[14])
print(INPUT_RESULT_TRUE)

INPUT_RESULT_FALSE = (sys.argv[15])
print(INPUT_RESULT_FALSE)

INPUT_RESULT_IS_NUMERIC = (sys.argv[16])
print(INPUT_RESULT_IS_NUMERIC)



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



def value_mapping_single_column(reference_column='referenceColumn', target_column='targetColumn', condition='eq', single_value=0, value_if_true=1, value_if_false=None,  temp_view_input='temp_view_table', temp_view_output='temp_view_table', spark_session=scSpark):
    
    temp_view_input = f"{client}_tbl_{temp_view_input}"
    temp_view_output = f"{client}_tbl_{temp_view_output}"

    df_processed = spark_session.sql(f"SELECT * FROM {temp_view_input}")
    if condition == 'eq':
        df_final = df_processed.withColumn(target_column, when(col(reference_column) == single_value, value_if_true).otherwise(value_if_false))
    elif condition == 'neq':
        df_final = df_processed.withColumn(target_column, when(col(reference_column) != single_value, value_if_true).otherwise(value_if_false))
    elif condition == 'gt':
        df_final = df_processed.withColumn(target_column, when(col(reference_column) > single_value, value_if_true).otherwise(value_if_false))
    elif condition == 'gte':
        df_final = df_processed.withColumn(target_column, when(col(reference_column) >= single_value, value_if_true).otherwise(value_if_false))
    elif condition == 'lt':
        df_final = df_processed.withColumn(target_column, when(col(reference_column) < single_value, value_if_true).otherwise(value_if_false))
    elif condition == 'lte':
        df_final = df_processed.withColumn(target_column, when(col(reference_column) <= single_value, value_if_true).otherwise(value_if_false))
    elif condition == 'null':
        df_final = df_processed.withColumn(target_column, when(col(reference_column).isNull(), value_if_true).otherwise(value_if_false))
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
if INPUT_VAL_IS_NUMERIC.lower() == "true":
    try:
        INPUT_VAL = int(INPUT_VAL)
    except ValueError:
        INPUT_VAL = float(INPUT_VAL)
    except:
        INPUT_VAL = "RE-CHECK INPUT VALUE"

if INPUT_RESULT_IS_NUMERIC.lower() == "true":
    try:
        INPUT_RESULT_TRUE = int(INPUT_RESULT_TRUE)
        INPUT_RESULT_FALSE = int(INPUT_RESULT_FALSE)
    except ValueError:
        INPUT_RESULT_TRUE = float(INPUT_RESULT_TRUE)
        INPUT_RESULT_FALSE = float(INPUT_RESULT_FALSE)
    except:
        INPUT_RESULT_TRUE = "RE-CHECK INPUT RESULT IF TRUE"
        INPUT_RESULT_FALSE = "RE-CHECK INPUT RESULT IF FALSE"

INPUT_VALUE_MAPPING_SINGLE_COLUMN = (INPUT_COLUMN_SOURCE, INPUT_COLUMN_TARGET, INPUT_COND, INPUT_VAL, INPUT_RESULT_TRUE, INPUT_RESULT_FALSE)
REFERENCE_COLUMN_NAME = list(INPUT_VALUE_MAPPING_SINGLE_COLUMN)[0]
TARGET_COLUMN_NAME = list(INPUT_VALUE_MAPPING_SINGLE_COLUMN)[1]
CONDITION = list(INPUT_VALUE_MAPPING_SINGLE_COLUMN)[2]
SINGLE_VALUE = list(INPUT_VALUE_MAPPING_SINGLE_COLUMN)[3]
VALUE_IF_TRUE = list(INPUT_VALUE_MAPPING_SINGLE_COLUMN)[4]
VALUE_IF_FALSE = list(INPUT_VALUE_MAPPING_SINGLE_COLUMN)[5]

OPERATOR_ID_PREV = 'B001' # Previous Operator ID
OPERATOR_ID_CURR = 'B002' # Current Operator ID

temp_view_next = value_mapping_single_column(reference_column=REFERENCE_COLUMN_NAME, 
                                             target_column=TARGET_COLUMN_NAME, 
                                             condition=CONDITION, 
                                             single_value=SINGLE_VALUE, 
                                             value_if_true=VALUE_IF_TRUE, 
                                             value_if_false=VALUE_IF_FALSE, 
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