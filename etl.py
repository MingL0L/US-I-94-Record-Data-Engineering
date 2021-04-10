import os
import configparser
from datetime import datetime, timedelta
import pandas as pd, re
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lower, dayofmonth, month, year, weekofyear, date_format

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')


def stage_i94(spark, input_data, output_data, table):
    '''
    Processing i94 immigrition data, load them into Spark session, 
    clean and write stage tables in parquet 

    Parameters:
        spark(object): spark session
        input_data(str): input file path 
                         e.g.,'/data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat'
        table: staging data name 
                        e.g.,'stage_i94_immigration'
        output_data (str): output file path
    Returns: spark dataframe
    '''
    print("stage_i94 start")
    
    # load i94 immigration dataset
    df_spark_i94 = spark.read.format('com.github.saurfang.sas.spark')\
                    .load(input_data)
    # Create dictionary of valid i94port codes
    re_filter = re.compile(r'\'(.*)\'.*\'(.*)\'')
    i94port_map = {}
    with open('mappings/i94prtl_valid.txt') as f:
        for line in f:
            groups = re_filter.search(line)
            i94port_map[groups[1]]=[groups[2]]
    # Clean i94 immigration data
    df_spark_i94_clr = df_spark_i94.dropDuplicates()\
                        .na.drop(subset=["depdate"])\
                        .filter(df_spark_i94.i94port.isin(list(i94port_map.keys())))\
                        .na.drop(subset=["matflag"])
    # Write to parquet partitioned by arrdate
    df_spark_i94_clr.write.partitionBy("arrdate")\
                    .parquet(os.path.join(output_data, table), mode="overwrite")
    print("stage_i94 end")
    return df_spark_i94_clr
    
def stage_demographics(spark, input_data, output_data, table):
    '''
    Processing US cities demographics files, load them into Spark session, 
    clean and write stage tables in parquet 

    Parameters:
        spark(object): spark session
        input_data(str): input file path 
                         e.g.,"us-cities-demographics.csv"
        table: staging data name 
                        e.g.,"us_cities"
        output_data (str): output file path
    Returns: spark dataframe
    '''
    print("stage_demographics start")
    df_spark_dem = spark.read.options(header='True',inferSchema='True',delimiter=';')\
                        .csv(input_data)
    # Clean us-cities-demographics data
    df_spark_dem = df_spark_dem.toDF(*(c.replace(' ', '_') for c in df_spark_dem.columns))
    df_spark_dem = df_spark_dem.toDF(*(c.replace('-', '_') for c in df_spark_dem.columns))
    df_spark_dem = df_spark_dem.toDF(*[c.lower() for c in df_spark_dem.columns])
    # Remove race count columns
    df_spark_dem_clr = df_spark_dem.drop("race", "count")\
                    .dropDuplicates()\
                    .na.drop(subset=["city","state"])\
                    .withColumn("city", lower(col("city")))\
                    .withColumn("state", lower(col("state")))
    # Write us-cities-demographics to parquet 
    df_spark_dem_clr.write.parquet(os.path.join(output_data, table), mode="overwrite")
    print("stage_demographics end")
    return df_spark_dem_clr

def stage_temperatures(spark, input_data, output_data, table):
    '''
    Processing global cities temperatures files, load them into Spark session, 
    clean and write stage tables in parquet 

    Parameters:
        spark(object): spark session
        input_data(str): input file path 
                         e.g.,"city_temperature.csv"
        table: staging data name 
                        e.g.,"stage_uscities_temperatures"
        output_data (str): output file path
    Returns: spark dataframe
    '''
    print("stage_temperatures start")
    
    # Load city_temperature
    df_spark_temp = spark.read.options(header='True',inferSchema='True')\
                        .csv(input_data)
    # Clean cities_temperatures data, convert city and state name to lower case
    df_spark_temp = df_spark_temp.toDF(*[c.lower() for c in df_spark_temp.columns])
    df_spark_temp_clr = df_spark_temp.dropDuplicates()\
                                    .filter(df_spark_temp.country=="US")\
                                    .filter(df_spark_temp.year==2016)\
                                    .filter(df_spark_temp.month==4)\
                                    .withColumn("state", lower(col("state")))\
                                    .withColumn("city", lower(col("city")))
    # Write cities temperatures data to parquet
    df_spark_temp_clr.write.parquet(os.path.join(output_data, table),\
                                    mode="overwrite")
    print("stage_temperatures end")
    return df_spark_temp_clr

def process_mappings(spark, input_data, column_names, dimension, separator):
    '''
    Process the mapping text files from input path, clean them 
    and transform into Spark dataframes.

    Parameters:
        spark: SparkSession
        input_data: input file path
        column_names: name of the columns
        dimension: name of the dimension
        separator: separator used from the text file
    Returns: spark dataframe
    '''
    # load
    df = pd.read_csv(input_data, sep=separator, header=None, \
                     engine='python',  names = column_names, skipinitialspace = True)
    
    # remove single quotes from the column at index 1
    df.iloc[ : , 1 ] = df.iloc[ : , 1 ].str.replace("'", "")
    
    # replace invalid codes with Other
    if(dimension == 'country'):
        df["country"] = df["country"]\
                        .replace(to_replace=["No Country.*", "INVALID.*", "Collapsed.*"], \
                                 value="Other", regex=True)
    elif(dimension == 'us_state'):
        df.iloc[ : , 0 ] = df.iloc[ : , 0].str.replace("'", "").str.replace("\t", "")
        df['state']=df['state'].str.lower()
    elif(dimension == 'us_port'):
        df.iloc[ : , 0 ] = df.iloc[ : , 0].str.replace("'", "")
        # splitting city and state by ", " from the city column
        new = df["city"].str.split(", ", n = 1, expand = True) 
        # making separate state column from new data frame 
        df["state_code"]= new[1].str.strip()
        # replacing the value of city column from new data frame 
        df["city"]= new[0].str.lower()
        
    return spark.createDataFrame(df)

def load_dim_port(spark, output_data, table):
    '''
    Processing US port of entry mapping files, load them into Spark session, 
    clean and write dimension parquet 

    Parameters:
        spark(object): spark session
        table: staging data name 
                        e.g.,"dim_us_ports"
        output_data (str): output file path
    Returns: spark dataframe
    '''
    print("load_dim_port start")
    
    # Get US State table from mapping text
    df_us_state = process_mappings(spark, "mappings/i94addrl.txt", \
                                   ["state_code", "state"], "us_state", "=")
    # Get US Port of entry table from mapping text
    df_us_port = process_mappings(spark, 'mappings/i94prtl.txt', \
                                      ["port_code", "city"], "us_port", "	=	")
    # Keep only port of entry which belongs to US
    df_us_port = df_us_port.join(df_us_state, on='state_code', how='inner')
    # write dimension into parquet
    df_us_port.write.parquet(os.path.join(output_data, table),\
                                        mode="overwrite")
    print("load_dim_port end")
    return df_us_port
    
def load_dim_country(spark, output_data, table):
    '''
    Processing world country mapping files, load them into Spark session, 
    clean and write dimension parquet 

    Parameters:
        spark(object): spark session
        table: staging data name 
                        e.g.,"dim_countries"
        output_data (str): output file path
    Returns: spark dataframe
    '''
    print("load_dim_country start")
    # Get country table from mapping text
    df_country = process_mappings(spark, 'mappings/i94cntyl.txt', \
                                    ["country_code", "country"], "country", " =  ")
    # write dimension into parquet
    df_country.write.parquet(os.path.join(output_data, table),\
                                        mode="overwrite")
    print("load_dim_country end")
    return df_country

def load_dim_visa(spark, output_data, table):
    '''
    Processing US visa type mapping files, load them into Spark session, 
    clean and write into dimension parquet 

    Parameters:
        spark(object): spark session
        table: staging data name 
                        e.g.,"dim_visa"
        output_data (str): output file path
    Returns: spark dataframe
    '''
    print("load_dim_visa start")
    
    # Get visa type table from mapping text
    df_visa = process_mappings(spark, 'mappings/I94VISA.txt', \
                                   ["visa_code", "visa"], "visa", " = ")
    # write dimension into parquet
    df_visa.write.parquet(os.path.join(output_data, table),\
                                        mode="overwrite")
    print("load_dim_visa end")
    return df_visa
    
def load_dim_travelmode(spark, output_data, table):
    '''
    Processing transportation mode mapping files, load them into Spark session, 
    clean and write into dimension parquet 

    Parameters:
        spark(object): spark session
        table: staging data name 
                        e.g.,"dim_travelmode"
        output_data (str): output file path
    Returns: spark dataframe
    '''
    print("load_dim_travelmode start")
    # Get travel mode table from mapping text
    df_mode = process_mappings(spark, 'mappings/i94model.txt', \
                                      ["mode_code", "mode"], "mode", " = ")
    # write dimension into parquet
    df_mode.write.parquet(os.path.join(output_data, table),\
                                        mode="overwrite")
    print("load_dim_travelmode end")
    return df_mode

def load_dim_demographics(spark, output_data, table):
    '''
    Processing US cities demographics data, load them into Spark session, 
    clean and write into dimension parquet  

    Parameters:
        spark(object): spark session
        table: staging data name 
                        e.g.,"dim_demographics"
        output_data (str): output file path
    Returns: spark dataframe
    '''
    print("load_dim_demographics start")
    # load staging data cities_demographics
    df_demographics = spark.read.parquet(os.path.join(output_data, "stage_cities_demographics"))
    # load dim_us_ports
    dim_us_ports = spark.read.parquet(os.path.join(output_data, "dim_us_ports"))
    # Join dim_us_port
    df_demographics = df_demographics.join(dim_us_ports.select("port_code","state","city"),\
                                           on=["state","city"],\
                                           how="inner").drop("state","city")

    # write dimension into parquet
    df_demographics.write.parquet(os.path.join(output_data, table),\
                                        mode="overwrite")
    print("load_dim_demographics end")
    return df_demographics

def build_fact_i94visits(spark, output_data, table):
    '''
    Processing i94 staging table, join with staging temperatures table
    to build fact table i94 visits, and write the fact parquet.

    Parameters:
        spark(object): spark session
        table: staging data name 
                        e.g.,"fact_i94visits"
        output_data (str): output file path
    Returns: spark dataframe
    '''
    print("build_fact_i94visits start")
    # load staging i94 parquet
    df_fact_i94_visits = spark.read.parquet(os.path.join(output_data, "stage_i94_immigration"))
    # load staging temperatures
    df_temperatures = spark.read.parquet(os.path.join(output_data, "stage_uscities_temperatures"))
    # load dimension us ports
    dim_port = spark.read.parquet(os.path.join(output_data, "dim_us_ports"))

    # Convert SAS date to ISO date
    get_date = udf(lambda x: (datetime(1960, 1, 1).date() + timedelta(x)).isoformat() if x else None)
    get_stay = udf(lambda x, y: int(x-y))
    df_fact_i94_visits = df_fact_i94_visits.na.drop(subset=["depdate"])\
                                         .withColumn("arrdate_iso", get_date("arrdate"))\
                                         .withColumn("depdate_iso", get_date("depdate"))\
                                         .withColumn("stay", get_stay("depdate","arrdate"))\
                                         .select("cicid","arrdate_iso","depdate_iso","stay","i94port",\
                                                 "i94cit","i94mode","i94visa","i94bir")\
                                        .withColumn("year", year(col("arrdate_iso")))\
                                        .withColumn("month", month(col("arrdate_iso")))\
                                        .withColumn("day", dayofmonth(col("arrdate_iso")))

    # Join dimension port
    df_fact_i94_visits = df_fact_i94_visits.join(dim_port, \
                                                 df_fact_i94_visits.i94port==dim_port.port_code, \
                                                 how = "inner").drop(dim_port.port_code)
    # Join temperature
    df_fact_i94_visits = df_fact_i94_visits.join(df_temperatures, \
                                                 on = ["year","month","day","state","city"], \
                                                 how = "left")
    # Drop useless columns
    df_fact_i94_visits = df_fact_i94_visits.select("cicid","arrdate_iso","depdate_iso","stay",\
                                                   "i94port","i94cit","i94mode","i94visa","i94bir",\
                                                   "avgtemperature")

    # Write fact parquet partitioned by arrdate
    df_fact_i94_visits.write.partitionBy("arrdate_iso")\
                    .parquet(os.path.join(output_data, table), mode="overwrite")
    print("build_fact_i94visits end")
    return df_fact_i94_visits

def load_dim_date(spark, output_data, table):
    '''
    Create dimension date table from i94 visits fact table, 
    clean and write into dimension parquet.

    Parameters:
        spark(object): spark session
        table: staging data name 
                        e.g.,"dim_date"
        output_data (str): output file path
    Returns: spark dataframe
    '''
    print("load_dim_date start")
    # load fact_i94visits
    fact_i94visits = spark.read.parquet(os.path.join(output_data, "fact_i94visits"))
    # create dimension date
    dim_date = fact_i94visits.select("arrdate_iso")\
                                    .dropDuplicates()\
                                    .withColumn("year", year("arrdate_iso"))\
                                    .withColumn("month", month("arrdate_iso"))\
                                    .withColumn("day", dayofmonth("arrdate_iso"))\
                                    .withColumn("week", weekofyear("arrdate_iso"))\
                                    .withColumn("weekday", date_format("arrdate_iso", "E"))
    # write dimension into parquet
    dim_date.write.parquet(os.path.join(output_data, table),\
                                        mode="overwrite")
    print("load_dim_date end")
    return dim_date


def check_count(spark, output_data, table, description):
    '''
    Check number of rows, need to be greater than 0
    Input: Spark dataframe, description
    Output: Print out check result
    '''
    df = spark.read.parquet(os.path.join(output_data, table))
    cnt = df.count()
    if cnt == 0:
        raise ValueError('Quality check failed for {} with zero records!!'.format(description))
    else:
        print("Quality check succes for {} with {} records.".format(description,cnt))


def check_uniquekey(spark, output_data, table, list_columns, description):
    '''
    Check unique key of dataframe
    Input: Spark dataframe, column name list, description
    Output: Print out check result
    '''
    df = spark.read.parquet(os.path.join(output_data, table))
    if df.count() > df.dropDuplicates(list_columns).count():
        raise ValueError('Key has duplicates for {}!!'.format(description))
    else:
        print("Unique key check succes for {}.".format(description))


def create_spark_session():
    '''
    Create a new spark session
    
    Returns:
      spark(object): spark session
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11") \
        .enableHiveSupport()\
        .getOrCreate()
    
    return spark


def main():
    '''
    ETL pipeline process dataset i94 immigration data, US city temperature,
    and city demographics into dimensions and fact table on datalake.
    '''    
    spark = create_spark_session()
    input_data = config.get('DATA','INPUT')
    output_data = config.get('DATA','OUTPUT')
    
    print("input path: ",input_data)
    print("ouput path: ",output_data)
    # Load, Clean and Stage I94 immigration dataset
    stage_i94(spark, \
              '/data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat', \
              output_data, 'stage_i94_immigration')
    # Load, Clean and Stage demographics dataset
    stage_demographics(spark, \
              "us-cities-demographics.csv", \
              output_data, 'stage_cities_demographics')
    # Load, Clean and Stage temperatures dataset
    stage_temperatures(spark, \
              "city_temperature.csv", \
              output_data, 'stage_uscities_temperatures')
    # Create and load dimension : dim_us_ports
    load_dim_port(spark, output_data, 'dim_us_ports')
    # Create and load dimension : dim_countries
    load_dim_country(spark, output_data, 'dim_countries')
    # Create and load dimension : dim_visa
    load_dim_visa(spark, output_data, 'dim_us_visa')
    # Create and load dimension : dim_travelmode
    load_dim_travelmode(spark, output_data, 'dim_travelmode')
    # Create and load dimension : dim_demographics
    load_dim_demographics(spark, output_data, 'dim_demographics')
    # Build fact table : dim_demographics
    build_fact_i94visits(spark, output_data, 'fact_i94visits')
    # Create Dimension date_table
    load_dim_date(spark, output_data, 'dim_date')
    # Check count
    check_count(spark, output_data, 'fact_i94visits', "i94 visits")
    # Check unique key
    check_uniquekey(spark, output_data, 'fact_i94visits', ['cicid'], "i94 visits")
    spark.stop()
if __name__ == "__main__":
    main()