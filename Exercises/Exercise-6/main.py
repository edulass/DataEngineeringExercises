from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType
import pyspark.sql.functions as F
import zipfile
import os
import datetime


def SparkDftoCSV(df,output_file_path):
    #Converts PySpark dataframe to excel
    df.coalesce(1) \
    .write \
    .format('csv') \
    .option('header', 'true') \
    .mode('overwrite') \
    .save(output_file_path)
    
    for filename in os.listdir(output_file_path):
        if not filename.endswith('.csv'): # keep only CSV files
            file_path = os.path.join(output_file_path, filename)
            os.remove(file_path)
    
def average_trip_duration(df):
#1.What is the average trip duration per day?

    df = df.withColumn("start_date", F.to_date("start_time"))
    df = df.withColumn("end_date", F.to_date("end_time"))
    df = df.withColumn("duration_sec", F.unix_timestamp("end_time") - F.unix_timestamp("start_time"))
    
    avg_duration_per_day = df.groupBy("start_date").agg(F.avg("duration_sec"))
    avg_duration_per_day = avg_duration_per_day.orderBy("start_date")
    
    output_file_path = '/app/reports/avg_duration_per_day.csv'
   
    SparkDftoCSV(df,output_file_path)
            
def Trips_per_day(df):
#2.How many trips were taken each day?

    df = df.withColumn("start_date", F.to_date("start_time"))
    df = df.withColumn("end_date", F.to_date("end_time"))
     
    Num_Trips_Per_day = df.groupBy("start_date").agg(F.count("start_time").alias("trips_per_day"))
    Num_Trips_Per_day = Num_Trips_Per_day.orderBy("start_date")
       
    output_file_path = '/app/reports/trips_per_day.csv'
   
    SparkDftoCSV(df,output_file_path)
    
def most_popular_station(df):
#3.What was the most popular starting trip station for each month?

    df = df.withColumn('year', F.year('start_time'))
    df = df.withColumn('month', F.month('start_time'))
    
    # Group by year, month and from_station_name and count occurrences
    df_grouped = df.groupBy('year', 'month', 'from_station_name').agg(F.count('*').alias('count'))
    
    # Sort in descending order of count
 
    df_most_popular = df_grouped .groupby('year', 'month').agg({'from_station_name': 'first'})

    # Rename columns
    df_most_popular = df_most_popular.withColumnRenamed('first(from_station_name)', 'most_popular_station')
    
    df_sorted = df_most_popular.orderBy(['year', 'month', 'most_popular_station'], ascending=[1, 1, 0])
    # Show the result
    output_file_path = '/app/reports/most_popular_station.csv'
   
    SparkDftoCSV(df_sorted,output_file_path)
                   
def Top3Stations_Last2weeks(df):
#4.What was the most popular starting trip station for each month?
    df = df.withColumn("start_time", F.col("start_time").cast(TimestampType()))
    
    # Find the maximum date in the DataFrame
    max_date = df.agg({'start_time': 'max'}).collect()[0][0]
    
    # Subtract 14 days from the maximum date to get the start date for the 2-week period
    start_date = max_date - datetime.timedelta(days=14)
    
    # Filter the DataFrame to keep only the last two weeks of data
    df_filtered = df.filter(F.col('start_time') >= start_date)
    
    # Group the data by date and from_station_name
    df_grouped = df_filtered.groupBy(F.date_format('start_time', 'yyyy-MM-dd').alias('date'), 'from_station_name')
    
    # Count the number of trips for each group
    df_counted = df_grouped.count()
    
    # Sort the data in descending order of trip count
    df_sorted = df_counted.orderBy(['date', F.desc('count')])
    
    # For each date, select the top 3 from_station_name
    df_top_3_stations = df_sorted.groupby('date').agg(
        F.collect_list('from_station_name').alias('top_3_stations')
    ).select(
        'date',
        F.col('top_3_stations')[0].alias('1st_most_popular_station'),
        F.col('top_3_stations')[1].alias('2nd_most_popular_station'),
        F.col('top_3_stations')[2].alias('3rd_most_popular_station')
    )
    
    df_top_3_stations=df_top_3_stations.orderBy('date')
    
    output_file_path = '/app/reports/Top3stations.csv'
    
    SparkDftoCSV(df_top_3_stations,output_file_path)

def AverageDurationTripPerGender(df):
#5.Do Males or Females take longer trips on average?

    df = df.withColumn("tripduration", F.unix_timestamp("end_time") - F.unix_timestamp("start_time"))
    df=df.select("tripduration","gender")
    
    grouped_data = df.groupBy('gender')

    # Calculate the average trip duration for each group
    avg_trip_duration_by_gender = grouped_data.agg(F.avg('tripduration'))
    
    avg_trip_duration_by_gender = avg_trip_duration_by_gender.filter(F.col('gender').isNotNull())
    
    output_file_path = '/app/reports/AverageDurationTripPerGender.csv'
    
    SparkDftoCSV(avg_trip_duration_by_gender,output_file_path)
    
def AgeLongestTrips(df):
#6.What is the top 10 ages of those that take the longest trips
    df = df.withColumn("duration_sec", F.unix_timestamp("end_time") - F.unix_timestamp("start_time"))
    df=df.select("duration_sec","birthyear")
    sorted_data = df.orderBy(F.desc("duration_sec"))
    filtered_data = sorted_data.filter(sorted_data.birthyear.isNotNull())
    result = filtered_data.limit(10)
    
    output_file_path = '/app/reports/AgeLongestTrips.csv'
    
    SparkDftoCSV(result,output_file_path)
    
    
def main():

    

    #setting up things
    report_folder_path = "/app/reports"
    
    if not os.path.exists(report_folder_path):
        os.makedirs(report_folder_path)
    
    spark = SparkSession.builder.appName('Exercise6') \
        .enableHiveSupport().getOrCreate()
        
    sc = spark.sparkContext

    sc.setLogLevel("ERROR")
     
    
    #Needs to join both csv files before calling the function that answers some of the questions
    
    with zipfile.ZipFile("/app/data/Divvy_Trips_2019_Q4.zip", "r") as zip_file:
        extracted_file_path1 = zip_file.extract("Divvy_Trips_2019_Q4.csv")
    
    with zipfile.ZipFile("/app/data/Divvy_Trips_2020_Q1.zip", "r") as zip_file:
        extracted_file_path2 = zip_file.extract("Divvy_Trips_2020_Q1.csv")
        
    df1 = spark.read.format("csv").option("header", "true").load(extracted_file_path1)
    
    df1_selected = df1.select("start_time", "end_time")
    
    df2 = spark.read.format("csv").option("header", "true").load(extracted_file_path2)
    
    df2_selected = df2.select("started_at", "ended_at")
    combined_df = df1_selected.union(df2_selected)
    
    #1.What is the average trip duration per day?
    average_trip_duration(combined_df)
    
    #2.How many trips were taken each day?
    Trips_per_day(combined_df)
    
    df1_selected = df1.select("start_time", "from_station_name")
    df2_selected = df2.select("started_at", "start_station_name")
    combined_df = df1_selected.union(df2_selected)
    
    #3.What was the most popular starting trip station for each month?
    most_popular_station(combined_df)
    
    #4.What was the most popular starting trip station for each month?
    Top3Stations_Last2weeks(combined_df)
    df=df1.select("start_time", "end_time","gender")
    
    #5.Do Males or Females take longer trips on average?
    AverageDurationTripPerGender(df)
    
    df=df1.select("start_time", "end_time","birthyear")
    
    #6.What is the top 10 ages of those that take the longest trips
    AgeLongestTrips(df)
    
    os.remove(extracted_file_path1)
    os.remove(extracted_file_path2) 
    



if __name__ == '__main__':
    main()


