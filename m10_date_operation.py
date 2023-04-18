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

INPUT_OPERATION = (sys.argv[9])
print(INPUT_OPERATION)



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



def date_operation(obj,spark_session = scSpark):
    temp_view_input = f"{client}_tbl_{obj['temp_view_input']}"
    temp_view_output = f"{client}_tbl_{obj['temp_view_output']}"
    df = spark_session.sql(f"select * from {temp_view_input}")
    for operation in obj['operation']:
        #add to date
        if operation['o_type'] == 'add_to_date':
            if (operation['v1_type']=='column') & (operation['v2_type']=='column'):
                if operation['unit']=='days':
                    df=df.withColumn(operation['new_column'],expr(f"date_add({operation['value1']},{operation['value2']})").cast('timestamp'))
                elif operation['unit']=='weeks':
                    df=df.withColumn(operation['new_column'],expr(f"date_add({operation['value1']},{operation['value2']}*7)").cast('timestamp'))
                elif operation['unit']=='hours':
                    df=df.withColumn(operation['new_column'], date_format((unix_timestamp(col(operation['value1']).cast('timestamp'), 'yyyy-MM-dd HH:mm:ss') + (col(operation['value2'])*3600)).cast('timestamp'), 'yyyy-MM-dd HH:mm:ss'))
                elif operation['unit']=='minutes':
                    df=df.withColumn(operation['new_column'], date_format((unix_timestamp(col(operation['value1']).cast('timestamp'), 'yyyy-MM-dd HH:mm:ss') + (col(operation['value2'])*60)).cast('timestamp'), 'yyyy-MM-dd HH:mm:ss'))
                elif operation['unit']=='seconds':
                    df=df.withColumn(operation['new_column'], date_format((unix_timestamp(col(operation['value1']).cast('timestamp'), 'yyyy-MM-dd HH:mm:ss') + (col(operation['value2']))).cast('timestamp'), 'yyyy-MM-dd HH:mm:ss'))
            elif (operation['v1_type']=='value') & (operation['v2_type']=='column'):
                df = df.withColumn('temporary',lit(operation['value1']).cast('timestamp'))
                if operation['unit']=='days':
                    df=df.withColumn(operation['new_column'],expr(f"date_add(temporary,{operation['value2']})").cast('timestamp')).drop('temporary')
                elif operation['unit']=='weeks':
                    df=df.withColumn(operation['new_column'],expr(f"date_add(temporary,{operation['value2']}*7)").cast('timestamp')).drop('temporary')
                elif operation['unit']=='hours':
                    df=df.withColumn(operation['new_column'], date_format((unix_timestamp(col('temporary').cast('timestamp'), 'yyyy-MM-dd HH:mm:ss') + (col(operation['value2'])*3600)).cast('timestamp'), 'yyyy-MM-dd HH:mm:ss')).drop('temporary')
                elif operation['unit']=='minutes':
                    df=df.withColumn(operation['new_column'], date_format((unix_timestamp(col('temporary').cast('timestamp'), 'yyyy-MM-dd HH:mm:ss') + (col(operation['value2'])*60)).cast('timestamp'), 'yyyy-MM-dd HH:mm:ss')).drop('temporary')
                elif operation['unit']=='seconds':
                    df=df.withColumn(operation['new_column'], date_format((unix_timestamp(col('temporary').cast('timestamp'), 'yyyy-MM-dd HH:mm:ss') + (col(operation['value2']))).cast('timestamp'), 'yyyy-MM-dd HH:mm:ss')).drop('temporary')
            elif (operation['v1_type']=='column') & (operation['v2_type']=='value'):
                df = df.withColumn(operation['new_column'],col(operation['value1'])+expr(f"INTERVAL {operation['value2']} {operation['unit']}"))
            elif (operation['v1_type']=='value') & (operation['v2_type']=='value'):
                df = df.withColumn('temporary',lit(operation['value1']).cast('timestamp'))\
                    .withColumn(operation['new_column'],col('temporary')+expr(f"INTERVAL {operation['value2']} {operation['unit']}")).drop('temporary')
        #sub from date
        elif operation['o_type'] == 'sub_from_date':
            if (operation['v1_type']=='column') & (operation['v2_type']=='column'):
                if operation['unit']=='days':
                    df=df.withColumn(operation['new_column'],expr(f"date_sub({operation['value1']},{operation['value2']})").cast('timestamp'))
                elif operation['unit']=='weeks':
                    df=df.withColumn(operation['new_column'],expr(f"date_sub({operation['value1']},{operation['value2']}*7)").cast('timestamp'))
                elif operation['unit']=='hours':
                    df=df.withColumn(operation['new_column'], date_format((unix_timestamp(col(operation['value1']).cast('timestamp'), 'yyyy-MM-dd HH:mm:ss') - (col(operation['value2'])*3600)).cast('timestamp'), 'yyyy-MM-dd HH:mm:ss'))
                elif operation['unit']=='minutes':
                    df=df.withColumn(operation['new_column'], date_format((unix_timestamp(col(operation['value1']).cast('timestamp'), 'yyyy-MM-dd HH:mm:ss') - (col(operation['value2'])*60)).cast('timestamp'), 'yyyy-MM-dd HH:mm:ss'))
                elif operation['unit']=='seconds':
                    df=df.withColumn(operation['new_column'], date_format((unix_timestamp(col(operation['value1']).cast('timestamp'), 'yyyy-MM-dd HH:mm:ss') - (col(operation['value2']))).cast('timestamp'), 'yyyy-MM-dd HH:mm:ss'))
            elif (operation['v1_type']=='value') & (operation['v2_type']=='column'):
                df = df.withColumn('temporary',lit(operation['value1']).cast('timestamp'))
                if operation['unit']=='days':
                    df=df.withColumn(operation['new_column'],expr(f"date_sub(temporary,{operation['value2']})").cast('timestamp')).drop('temporary')
                elif operation['unit']=='weeks':
                    df=df.withColumn(operation['new_column'],expr(f"date_sub(temporary,{operation['value2']}*7)").cast('timestamp')).drop('temporary')
                elif operation['unit']=='hours':
                    df=df.withColumn(operation['new_column'], date_format((unix_timestamp(col('temporary').cast('timestamp'), 'yyyy-MM-dd HH:mm:ss') - (col(operation['value2'])*3600)).cast('timestamp'), 'yyyy-MM-dd HH:mm:ss')).drop('temporary')
                elif operation['unit']=='minutes':
                    df=df.withColumn(operation['new_column'], date_format((unix_timestamp(col('temporary').cast('timestamp'), 'yyyy-MM-dd HH:mm:ss') - (col(operation['value2'])*60)).cast('timestamp'), 'yyyy-MM-dd HH:mm:ss')).drop('temporary')
                elif operation['unit']=='seconds':
                    df=df.withColumn(operation['new_column'], date_format((unix_timestamp(col('temporary').cast('timestamp'), 'yyyy-MM-dd HH:mm:ss') - (col(operation['value2']))).cast('timestamp'), 'yyyy-MM-dd HH:mm:ss')).drop('temporary')
            elif (operation['v1_type']=='column') & (operation['v2_type']=='value'):
                df = df.withColumn(operation['new_column'],col(operation['value1'])-expr(f"INTERVAL {operation['value2']} {operation['unit']}"))
            elif (operation['v1_type']=='value') & (operation['v2_type']=='value'):
                df = df.withColumn('temporary',lit(operation['value1']).cast('timestamp'))\
                    .withColumn(operation['new_column'],col('temporary')-expr(f"INTERVAL {operation['value2']} {operation['unit']}")).drop('temporary')
        #date diff
        elif operation['o_type'] == 'date_diff':
            if(operation['v1_type']=='column') & (operation['v2_type']=='column'):
                if operation['unit'] == 'days':
                    df = df.withColumn(operation['new_column'],round((unix_timestamp(col(operation['value1']).cast('timestamp'),'yyyy-MM-dd HH:mm:ss') - unix_timestamp(col(operation['value2']).cast('timestamp'),'yyyy-MM-dd HH:mm:ss'))/(24*3600),2))
                if operation['unit'] == 'hours':
                    df = df.withColumn(operation['new_column'],round((unix_timestamp(col(operation['value1']).cast('timestamp'),'yyyy-MM-dd HH:mm:ss') - unix_timestamp(col(operation['value2']).cast('timestamp'),'yyyy-MM-dd HH:mm:ss'))/3600,2))
                if operation['unit'] == 'minutes':
                    df = df.withColumn(operation['new_column'],round((unix_timestamp(col(operation['value1']).cast('timestamp'),'yyyy-MM-dd HH:mm:ss') - unix_timestamp(col(operation['value2']).cast('timestamp'),'yyyy-MM-dd HH:mm:ss'))/60,2))
                if operation['unit'] == 'seconds':
                    df = df.withColumn(operation['new_column'],round((unix_timestamp(col(operation['value1']).cast('timestamp'),'yyyy-MM-dd HH:mm:ss') - unix_timestamp(col(operation['value2']).cast('timestamp'),'yyyy-MM-dd HH:mm:ss')),2))
            elif(operation['v1_type']=='column') & (operation['v2_type']=='value'):
                df = df.withColumn('temporary',lit(operation['value2']))
                if operation['unit'] == 'days':
                    df = df.withColumn(operation['new_column'],round((unix_timestamp(col(operation['value1']).cast('timestamp'),'yyyy-MM-dd HH:mm:ss') - unix_timestamp(col('temporary').cast('timestamp'),'yyyy-MM-dd HH:mm:ss'))/(24*3600),2)).drop('temporary')
                if operation['unit'] == 'hours':
                    df = df.withColumn(operation['new_column'],round((unix_timestamp(col(operation['value1']).cast('timestamp'),'yyyy-MM-dd HH:mm:ss') - unix_timestamp(col('temporary').cast('timestamp'),'yyyy-MM-dd HH:mm:ss'))/3600,2)).drop('temporary')
                if operation['unit'] == 'minutes':
                    df = df.withColumn(operation['new_column'],round((unix_timestamp(col(operation['value1']).cast('timestamp'),'yyyy-MM-dd HH:mm:ss') - unix_timestamp(col('temporary').cast('timestamp'),'yyyy-MM-dd HH:mm:ss'))/60,2)).drop('temporary')
                if operation['unit'] == 'seconds':
                    df = df.withColumn(operation['new_column'],round((unix_timestamp(col(operation['value1']).cast('timestamp'),'yyyy-MM-dd HH:mm:ss') - unix_timestamp(col('temporary').cast('timestamp'),'yyyy-MM-dd HH:mm:ss')),2)).drop('temporary')
            elif(operation['v1_type']=='value') & (operation['v2_type']=='column'):
                df = df.withColumn('temporary',lit(operation['value1']))
                if operation['unit'] == 'days':
                    df = df.withColumn(operation['new_column'],round((unix_timestamp(col('temporary').cast('timestamp'),'yyyy-MM-dd HH:mm:ss') - unix_timestamp(col(operation['value2']).cast('timestamp'),'yyyy-MM-dd HH:mm:ss'))/(24*3600),2)).drop('temporary')
                if operation['unit'] == 'hours':
                    df = df.withColumn(operation['new_column'],round((unix_timestamp(col('temporary').cast('timestamp'),'yyyy-MM-dd HH:mm:ss') - unix_timestamp(col(operation['value2']).cast('timestamp'),'yyyy-MM-dd HH:mm:ss'))/3600,2)).drop('temporary')
                if operation['unit'] == 'minutes':
                    df = df.withColumn(operation['new_column'],round((unix_timestamp(col('temporary').cast('timestamp'),'yyyy-MM-dd HH:mm:ss') - unix_timestamp(col(operation['value2']).cast('timestamp'),'yyyy-MM-dd HH:mm:ss'))/60,2)).drop('temporary')
                if operation['unit'] == 'seconds':
                    df = df.withColumn(operation['new_column'],round((unix_timestamp(col('temporary').cast('timestamp'),'yyyy-MM-dd HH:mm:ss') - unix_timestamp(col(operation['value2']).cast('timestamp'),'yyyy-MM-dd HH:mm:ss')),2)).drop('temporary')  
            elif(operation['v1_type']=='value') & (operation['v2_type']=='value'):
                df = df.withColumn('temporary1',lit(operation['value1']))\
                    .withColumn('temporary2',lit(operation['value2']))
                if operation['unit'] == 'days':
                    df = df.withColumn(operation['new_column'],round((unix_timestamp(col('temporary1').cast('timestamp'),'yyyy-MM-dd HH:mm:ss') - unix_timestamp(col('temporary2').cast('timestamp'),'yyyy-MM-dd HH:mm:ss'))/(24*3600),2)).drop('temporary1').drop('temporary2')
                if operation['unit'] == 'hours':
                    df = df.withColumn(operation['new_column'],round((unix_timestamp(col('temporary1').cast('timestamp'),'yyyy-MM-dd HH:mm:ss') - unix_timestamp(col('temporary2').cast('timestamp'),'yyyy-MM-dd HH:mm:ss'))/3600,2)).drop('temporary1').drop('temporary2')
                if operation['unit'] == 'minutes':
                    df = df.withColumn(operation['new_column'],round((unix_timestamp(col('temporary1').cast('timestamp'),'yyyy-MM-dd HH:mm:ss') - unix_timestamp(col('temporary2').cast('timestamp'),'yyyy-MM-dd HH:mm:ss'))/60,2)).drop('temporary1').drop('temporary2')
                if operation['unit'] == 'seconds':
                    df = df.withColumn(operation['new_column'],round((unix_timestamp(col('temporary1').cast('timestamp'),'yyyy-MM-dd HH:mm:ss') - unix_timestamp(col('temporary2').cast('timestamp'),'yyyy-MM-dd HH:mm:ss')),2)).drop('temporary1').drop('temporary2')
        #year of date            
        elif(operation['o_type']=='year_of_date'):
            if(operation['v1_type']=='column'):
                df = df.withColumn(operation['new_column'],year(col(operation['value1'])))
            elif(operation['v1_type']=='value'):
                df = df.withColumn('temporary',lit(operation['value1']))\
                    .withColumn(operation['new_column'],year(col('temporary'))).drop('temporary')
        #quarter of date
        elif(operation['o_type']=='quarter_of_date'):
            if(operation['v1_type']=='column'):
                df = df.withColumn(operation['new_column'],quarter(col(operation['value1'])))
            elif(operation['v1_type']=='value'):
                df = df.withColumn('temporary',lit(operation['value1']))\
                    .withColumn(operation['new_column'],quarter(col('temporary'))).drop('temporary')
        #month of date
        elif(operation['o_type']=='month_of_date'):
            if(operation['v1_type']=='column'):
                df = df.withColumn(operation['new_column'],month(col(operation['value1'])))
            elif(operation['v1_type']=='value'):
                df = df.withColumn('temporary',lit(operation['value1']))\
                    .withColumn(operation['new_column'],month(col('temporary'))).drop('temporary')
        #day of year
        elif(operation['o_type']=='day_of_year'):
            if(operation['v1_type']=='column'):
                df = df.withColumn(operation['new_column'], date_format(to_timestamp(col(operation['value1'])),'D'))
            elif(operation['v1_type']=='value'):
                df = df.withColumn('temporary',lit(operation['value1']))\
                    .withColumn(operation['new_column'],date_format(to_timestamp(col('temporary')),'D')).drop('temporary')
        #day of month
        elif(operation['o_type']=='day_of_month'):
            if(operation['v1_type']=='column'):
                df = df.withColumn(operation['new_column'], date_format(to_timestamp(col(operation['value1'])),'d'))
            elif(operation['v1_type']=='value'):
                df = df.withColumn('temporary',lit(operation['value1']))\
                    .withColumn(operation['new_column'],date_format(to_timestamp(col('temporary')),'d')).drop('temporary')
        #day of week
        elif(operation['o_type']=='day_of_week'):
            if(operation['v1_type']=='column'):
                df = df.withColumn(operation['new_column'],dayofweek(col(operation['value1'])))
            elif(operation['v1_type']=='value'):
                df = df.withColumn('temporary',lit(operation['value1']))\
                    .withColumn(operation['new_column'],dayofweek(col('temporary'))).drop('temporary')
        #week of year
        elif(operation['o_type']=='week_of_year'):
            if(operation['v1_type']=='column'):
                df = df.withColumn(operation['new_column'],weekofyear(col(operation['value1'])))
            elif(operation['v1_type']=='value'):
                df = df.withColumn('temporary',lit(operation['value1']))\
                    .withColumn(operation['new_column'],weekofyear(col('temporary'))).drop('temporary')

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
INPUT_OPERATION = json.loads(INPUT_OPERATION)
print(INPUT_OPERATION)

OPERATOR_ID_PREV = 'B001' # Previous Operator ID
OPERATOR_ID_CURR = 'B002' # Current Operator ID

# o_type add_to_date, sub_from_date, date_diff, year_of_date, month_of_date, quarter_of_date, day_of_year, day_of_month, day_of_week, week_of_year
obj_date = {
    'operation': INPUT_OPERATION,
    'temp_view_input': OPERATOR_ID_PREV,
    'temp_view_output': OPERATOR_ID_CURR
}

date_temp_view = date_operation(obj_date,spark_session=scSpark)
scSpark.sql(f"select * from {date_temp_view}").show()
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