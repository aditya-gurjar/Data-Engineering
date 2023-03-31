import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, round, count, sum
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import mean, stddev, min, max
import argparse
import pandas as pd
#feel free to def new functions if you need

def create_dataframe(filepath, format, spark):
    """
    Create a spark df given a filepath and format.

    :param filepath: <str>, the filepath
    :param format: <str>, the file format (e.g. "csv" or "json")
    :param spark: <str> the spark session

    :return: the spark df uploaded
    """

    #add your code here
    spark_df = spark.read.option("header", "true").format(format).load(filepath)

    return spark_df


def transform_nhis_data(nhis_df):
    """
    Transform df elements

    :param nhis_df: spark df
    :return: spark df, transformed df
    """

    #add your code here
    transformed_df =  nhis_df.withColumn("_AGEG5YR", 
                      when(col("AGE_P").between(18, 24), 1)
                      .when(col("AGE_P").between(25, 29), 2)
                      .when(col("AGE_P").between(30, 34), 3)
                      .when(col("AGE_P").between(35, 39), 4)
                      .when(col("AGE_P").between(40, 44), 5)
                      .when(col("AGE_P").between(45, 49), 6)
                      .when(col("AGE_P").between(50, 54), 7)
                      .when(col("AGE_P").between(55, 59), 8)
                      .when(col("AGE_P").between(60, 64), 9)
                      .when(col("AGE_P").between(65, 69), 10)
                      .when(col("AGE_P").between(70, 74), 11)
                      .when(col("AGE_P").between(75, 79), 12)
                      .when(col("AGE_P") >= 80, 13)
                      .otherwise(14))
  
    transformed_df = transformed_df.withColumn("_IMPRACE", 
                     when((col("HISPAN_I") == "12") & (col("MRACBPI2") == "1"), 1)
                     .when((col("HISPAN_I") == "12") & (col("MRACBPI2") == "2"), 2)
                     .when((col("HISPAN_I") == "12") & (col("MRACBPI2").isin(["6", "7", "12"])), 3)
                     .when((col("HISPAN_I") == "12") & (col("MRACBPI2") == "3"), 4)
                     .when((col("HISPAN_I") == "12") & (col("MRACBPI2").isin(["16", "17"])), 6)
                     .otherwise(5))

    transformed_df = transformed_df.drop('HISPAN_I', 'MRACBPI2', 'AGE_P')
    return transformed_df


def calculate_statistics(joined_df):
    """
    Calculate prevalence statistics

    :param joined_df: the joined df

    :return: None
    """

    #add your code here
    sex_values = {1: "Male", 2: "Female"}
    age_values = {1: "18-24", 2: "25-29", 3: "30-34", 4: "35-39", 5: "40-44", 6: "45-49", 7: "50-54",
                  8: "55-59", 9: "60-64", 10: "65-69", 11: "70-74", 12: "75-79", 13: "80+"}
    race_values = {1: "White, Non-Hispanic", 2: "Black, Non-Hispanic", 3: "Asian, Non-Hispanic", 
                   4: "American Indian/Alaskan Native, Non-Hispanic", 5: "Hispanic", 6: "Other race, Non-Hispanic"}
    
    sex_prevalence = (joined_df.groupBy("SEX")
                  .agg(((sum(when(col("DIBEV1") == 1, 1).otherwise(0)) / count("*")) * 100).alias("Prevalence (in %)"))
                  .orderBy("SEX"))
    sex_prevalence = sex_prevalence.join(spark.createDataFrame(pd.DataFrame(list(sex_values.items()), columns=["SEX", "SEX_VALUE"])), "SEX") \
                  .select("SEX", "SEX_VALUE", "Prevalence (in %)")
    sex_prevalence.show()

    age_prevalence = (joined_df.groupBy("_AGEG5YR")
                  .agg(((sum(when(col("DIBEV1") == 1, 1).otherwise(0)) / count("*")) * 100).alias("prevalence (in %)"))
                  .orderBy("_AGEG5YR"))
    age_prevalence = age_prevalence.join(spark.createDataFrame(pd.DataFrame(list(age_values.items()), columns=["_AGEG5YR", "AGE_VALUE"])), "_AGEG5YR") \
                  .select("_AGEG5YR", "AGE_VALUE", "Prevalence (in %)") \
                  .orderBy("_AGEG5YR")
    age_prevalence.show()

    race_prevalence = (joined_df.groupBy("_IMPRACE")
                  .agg(((sum(when(col("DIBEV1") == 1, 1).otherwise(0)) / count("*")) * 100).alias("Prevalence (in %)"))
                  .orderBy("_IMPRACE"))
    race_prevalence = race_prevalence.join(spark.createDataFrame(pd.DataFrame(list(race_values.items()), columns=["_IMPRACE", "RACE_VALUE"])), \
                                           "_IMPRACE").select("_IMPRACE", "RACE_VALUE", "Prevalence (in %)").orderBy("_IMPRACE")
                                    
    race_prevalence.show()

def join_data(brfss_df, nhis_df):
    """
    Join dataframes

    :param brfss_df: spark df
    :param nhis_df: spark df after transformation
    :return: the joined df

    """
    #add your code here
    brfss_df = brfss_df.na.drop()
    nhis_df = nhis_df.na.drop()
    cols_to_convert = ["_AGEG5YR", "_IMPRACE", "SEX"]
    for col_name in cols_to_convert:
      nhis_df = nhis_df.withColumn(col_name, col(col_name).cast(DoubleType()))
    joined_df = brfss_df.join(nhis_df, ['_AGEG5YR', 'SEX', '_IMPRACE'], "inner") 
    return joined_df

if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    arg_parser.add_argument('nhis', type=str, default=None, help="brfss filename")
    arg_parser.add_argument('brfss', type=str, default=None, help="nhis filename")
    arg_parser.add_argument('-o', '--output', type=str, default=None, help="output path(optional)")

    #parse args
    args = arg_parser.parse_args()
    if not args.nhis or not args.brfss:
        arg_parser.usage = arg_parser.format_help()
        arg_parser.print_usage()
    else:
        brfss_filename = args.nhis
        nhis_filename = args.brfss

        # Start spark session
        spark = SparkSession.builder.getOrCreate()

        # load dataframes
        brfss_df = create_dataframe(brfss_filename, 'json', spark)
        nhis_df = create_dataframe(nhis_filename, 'csv', spark)

        # Perform mapping on nhis dataframe
        nhis_df = transform_nhis_data(nhis_df)
        # Join brfss and nhis df
        joined_df = join_data(brfss_df, nhis_df)
        # Calculate statistics
        calculate_statistics(joined_df)

        # Save
        if args.output:
            joined_df.write.csv(args.output, mode='overwrite', header=True)


        # Stop spark session 
        spark.stop()