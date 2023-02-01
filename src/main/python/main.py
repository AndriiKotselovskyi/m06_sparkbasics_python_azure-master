from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan, when, count, udf, concat_ws
from utils import find_coordinates
import os
import pygeohash as gh

# Starting Spark Session
service_credentials = os.getenv('service_credentials')

spark = SparkSession.builder \
    .master("local[3]") \
    .appName("test") \
    .getOrCreate()

spark.conf.set("fs.azure.account.auth.type.bd201stacc.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.bd201stacc.dfs.core.windows.net",
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.bd201stacc.dfs.core.windows.net",
               "f3905ff9-16d4-43ac-9011-842b661d556d")
spark.conf.set("fs.azure.account.oauth2.client.secret.bd201stacc.dfs.core.windows.net",
               service_credentials)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.bd201stacc.dfs.core.windows.net",
               "https://login.microsoftonline.com/b41b72d0-4e9f-4c26-8a69-f949f367c91d/oauth2/token")

# Reading Hotels data from ADLS
df_hotels_adls = spark.read.option("header", "true") \
    .format('csv') \
    .load("abfss://m06sparkbasics@bd201stacc.dfs.core.windows.net/hotels/")

#  Script to clean data if "null" is a text, not real NULL
# df_hotels_adls = df_hotels_read.withColumn("Latitude",
#                                            when(col("Latitude").rlike(".*null*"), None)
#                                            .otherwise(col("Latitude"))) \
#                                 .withColumn("Longitude",
#                                             when(col("Longitude").rlike(".*null*"), None)
#                                             .otherwise(col("Longitude")))

# Function to populate missed coordinates in Hotels dataset
lat_UDF = udf(lambda z: find_coordinates(z, 'lat'))
lng_UDF = udf(lambda z: find_coordinates(z, 'lng'))

df_hotels = df_hotels_adls \
    .withColumn("Latitude",
                when(df_hotels_adls.Latitude.isNull(), lat_UDF(concat_ws(', ', col("Address"), col("City"))))
                .otherwise(df_hotels_adls.Latitude)) \
    .withColumn("Latitude", col("Latitude").cast("float")) \
    .withColumn("Longitude",
                when(df_hotels_adls.Longitude.isNull(), lng_UDF(concat_ws(', ', col("Address"), col("City"))))
                .otherwise(df_hotels_adls.Longitude)) \
    .withColumn("Longitude", col("Longitude").cast("float"))

# df_hotels.show()

#  Script to check if empty coordinates still present
# df_Columns = ["Latitude", "Longitude"]
# df_check_nulls = df_hotels.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df_Columns])
# df_check_nulls.show()
#

# Encoding latitude and longitude into geohash value
encode_UDF = udf(lambda x, z: gh.encode(x, z, precision=5))

df_hotels_geohash = df_hotels.withColumn("GeoHash", encode_UDF(col("Latitude"), col("Longitude")))
# df_hotels_geohash.show()

# Reading Weather dataset from ADLS
df_weather_adls = spark.read \
        .option("recursiveFileLookup", "true") \
        .option("header", "true") \
        .parquet("abfss://m06sparkbasics@bd201stacc.dfs.core.windows.net/weather")

# df_weather_adls.show()

# Encoding latitude and longitude into geohash value
df_weather_geohash = df_weather_adls.withColumn("GeoHash", encode_UDF(col("lat"), col("lng")))
# df_weather_geohash.show()

# Joining two datasets together
ready_df = df_weather_geohash.join(df_hotels_geohash, df_weather_geohash.GeoHash == df_hotels_geohash.GeoHash,
                                   how="left")
ready_df.show()

