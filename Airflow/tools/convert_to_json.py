import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

#parse the config file



def create_spark_session():
    '''
    Create a spark session and return it
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11,org.apache.hadoop:hadoop-aws:2.7.0").enableHiveSupport()\
        .getOrCreate()
    return spark


def convert_immigration_data(spark, output_data):
    '''
    Load SAS  data form input dir, restructure the data and export to output dir as JSON files.
    args:
        spark (obj)= spark session
        output_data (str)= output directory to save the parquet files
    '''
    # get filepath data file
    immi_data = '../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat'
    
    # read data file
    print("Reading the immigration data at {}".format(immi_data))
    immigration_df = spark.read.format('com.github.saurfang.sas.spark').load(immi_data)
    print("Data has been read successfully to spark DataFrame!")

    # extract columns
    immi_df = immigration_df.select(
                col('cicid').alias('id'),
                col('i94yr').alias('year'),
                col('i94mon').alias('month'),
                col('i94res').alias('country_code'),
                col('i94port').alias('city_code'),
                col('arrdate').alias('arrival_date'),
                col('i94mode').alias('travel_mode'),
                col('i94addr').alias('state'),
                col('i94bir').alias('age'),
                col('i94visa').alias('travel_purpose'),
                col('gender').alias('gender'),
                col('fltno').alias('flight_no'),
                col('admnum').alias('admission_number'),
                col('visatype').alias('visa_type'),
                col('dtaddto').alias('visa_expiration')   
    ).dropDuplicates()
    
    immi_df.limit(10).show()
    
    # Write json files
    output_path = os.path.join(output_data,'immigration')
    immi_df.write.json(output_path, mode = 'overwrite')
    print("Immigration data has been successfully converted to json files and saved to {}! ".format(output_path))

def convert_temp_data(spark, output_data):
    '''
    Load CSV data form input dir, restructure the data and export to output dir as JSON files.
    args:
        spark (obj)= spark session
        output_data (str)= output directory to save the parquet files
    '''
    
    temp_data =  '../../data2/GlobalLandTemperaturesByCity.csv'
    
    print("Reading the temperature data at {}".format(temp_data))
    temperature_df = spark.read.load(temp_data,format= "csv", header= "true")
    print("Data has been read successfully to spark DataFrame!")
    
    temp_df = temperature_df.select(
                    col('dt').alias('date'),
                    col('AverageTemperature').alias('ave_temp.'),
                    col('City').alias('city'),
                    col('Country').alias('country'),
                    col('Latitude').alias('latitude'),
                    col('Longitude').alias('longitude') 
    ).dropDuplicates()
    
    temp_df.limit(10).show()
    
    output_path = os.path.join(output_data,'temperature')
    temp_df.write.json(output_path, mode = 'overwrite')
    print("Temperature data has been successfully converted to json files and saved to {}! ".format(output_path))
    
    
def convert_demo_data(spark, output_data):
    '''
    Load CSV data form input dir, restructure the data and export to output dir as JSON files.
    args:
        spark (obj)= spark session
        output_data (str)= output directory to save the parquet files
    '''
    
    demo_data =  './data/us-cities-demographics.csv'
    
    print("Reading the demographics data at {}".format(demo_data))
    demographics_df = spark.read.load(demo_data,format= "csv", header= "true")
    print("Data has been read successfully to spark DataFrame!")
    
    demo_df = demographics_df.select(
                    col('City').alias('city'),
                    col('State').alias('state'),
                    col('Median Age').alias('median_age'),
                    col('Male Population').alias('male_population'),
                    col('Female Population').alias('female_population'),
                    col('Total Population').alias('total_population'),
                    col('Foreign-born').alias('foreign-born'),
                    col('Average Household Size').alias('ave_household_size'),
                    col('State Code').alias('state_code'),
                    col('Race').alias('race'),
                    col('Count').alias('count')
    ).dropDuplicates()
    
    demo_df.limit(10).show()
    
    output_path = os.path.join(output_data,'demogprahics')
    demo_df.write.json(output_path, mode = 'overwrite')
    print("Demographics data has been successfully converted to json files and saved to {}! ".format(output_path))    


def convert_to_json_main():
    '''
    - Create a spark session
    - Define input and output directories (AWS s3 buckets)
    - Call the convert functions
    '''
    #Read the config file
    config = configparser.ConfigParser()
    config.read('../aws.cfg')

    #set environment variables, secret and access keys, from config file
    os.environ['AWS_ACCESS_KEY_ID']=config['AWS_CREDS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_CREDS']['AWS_SECRET_ACCESS_KEY']

    
    #create the spark session
    spark = create_spark_session()
    
    #Determine the s3 directories
    output_data = config["AWS"]["AWS_Bucket"]
    
    #Call the convert functions for immigration, temperature and demographics data
    convert_immigration_data(spark, output_data)
    convert_temp_data(spark, output_data)
    convert_demo_data(spark, output_data)
    

if __name__ == "__main__":
    convert_to_json_main()