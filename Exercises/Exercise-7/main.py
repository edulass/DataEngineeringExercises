from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType,DateType
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import zipfile
import os
import datetime


def FileNameAsColumn(df,FileName):
    #1.Add the file name as a column to the DataFrame and call it source_file.
    df = df.withColumn("source_file", F.lit(FileName))
    return df
    
def DateColumn(df,FileName):
    #2.Pull the date located inside the string of the source_file column. Final data-type must be date or timestamp, not a string. Call the new column file_date.
    FileDate=FileName[11:21]
    df = df.withColumn('date', F.to_date(F.lit(FileDate), 'yyyy-MM-dd').cast(DateType()))
    return df
def BrandColumn(df):
    #3.Add a new column called brand. It will be based on the column model. If the column model has a space ... aka   in it, split on that space. The value found before the space   
    #will be considered the brand. If there is no space to split on, fill in a value called unknown for the brand.

    df = df.withColumn("brand", F.when(df.model.contains(" "), F.split(df.model, " ")[0]).otherwise("unknown"))
    return df
    
def CapacityColumn(df):
    #4.Inspect a column called capacity_bytes. Create a secondary DataFrame that relates capacity_bytes to the model column, create "buckets" / "rankings" 
    df = df.withColumn("capacity_bytes", df["capacity_bytes"].cast("double"))
    capacity_df = df.groupBy("model").sum("capacity_bytes").withColumnRenamed("sum(capacity_bytes)", "capacity")
    ranking_df = capacity_df.withColumn("storage_ranking", F.rank().over(Window.orderBy(F.desc("capacity"))))
    df = df.join(ranking_df, "model")
    return df
    
def CapacityPrimaryKey(df):
    #5.Create a column called primary_key that is hash of columns that make a record umique in this dataset.
    df = df.withColumn("primary_key", F.sha2(F.concat("date", "serial_number", "model"), 256))
    return df
    
def main():

    spark = SparkSession.builder.appName('Exercise7') \
        .enableHiveSupport().getOrCreate()
    
    FileName="hard-drive-2022-01-01-failures.csv"    
    sc = spark.sparkContext

    sc.setLogLevel("ERROR")
    
    with zipfile.ZipFile("/app/data/hard-drive-2022-01-01-failures.csv.zip", "r") as zip_file:
        extracted_file_path = zip_file.extract(FileName)
    
    df = spark.read.format("csv").option("header", "true").load(extracted_file_path)
    
    #df = df.withColumn("source_file", F.lit(FileName))
    
   
    
    df=FileNameAsColumn(df,FileName)


    df=DateColumn(df,FileName)
    

    df=BrandColumn(df)
    
    
    df=CapacityColumn(df)
 
  
    
    df=CapacityPrimaryKey(df)   



    #FileDate=FileName[11:21]
    
    
    
    #df = df.withColumn('date', F.to_date(F.lit(FileDate), 'yyyy-MM-dd').cast(DateType()))
    
    
    #df = df.withColumn("brand", F.when(df.model.contains(" "), F.split(df.model, " ")[0]).otherwise("unknown"))
    
    #df = df.withColumn("capacity_bytes", df["capacity_bytes"].cast("double"))
    # Create a secondary DataFrame with the capacity of each model
    #capacity_df = df.groupBy("model").sum("capacity_bytes").withColumnRenamed("sum(capacity_bytes)", "capacity")
    
    # Add a rank column based on the capacity, where the highest capacity gets the rank 1
    #ranking_df = capacity_df.withColumn("storage_ranking", F.rank().over(Window.orderBy(F.desc("capacity"))))
    
    # Join the original DataFrame with the ranking DataFrame
    #result_df = df.join(ranking_df, "model")
    
    #result_df = result_df.withColumn("primary_key", F.sha2(F.concat("date", "serial_number", "model"), 256))
    df.show()

if __name__ == '__main__':
    main()
