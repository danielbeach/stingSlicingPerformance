from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession.builder.appName('SparkSlice').getOrCreate()

t1 = datetime.now()
fixed_file = '/Users/danielbeach/Desktop/fixed_width_data.txt'

schema_string = "trim(substring(col0))"
df = spark.read.csv(fixed_file, schema=schema_string, header='true', sep=None)
df.show()

df.createOrReplaceTempView('trips')
result = spark.sql("""
                SELECT 
                    trim(substring(`_c0`, 0, 45)) as ride_id,
                    trim(substring(`_c0`, 45, 90)) as rideable_type,
                    trim(substring(`_c0`, 90, 135)) as started_at,
                    trim(substring(`_c0`, 135, 180)) as ended_at,
                    trim(substring(`_c0`, 180, 225)) as start_station_name,
                    trim(substring(`_c0`, 225, 270)) as start_station_id,
                    trim(substring(`_c0`, 270, 315)) as end_station_name,
                    trim(substring(`_c0`, 315, 360)) as end_station_id,
                    trim(substring(`_c0`, 360, 405)) as start_lat,
                    trim(substring(`_c0`, 405, 450)) as start_lng,
                    trim(substring(`_c0`, 450, 495)) as end_lat,
                    trim(substring(`_c0`, 495, 540)) as end_lng,
                    trim(substring(`_c0`, 540, 585)) as member_casual
                FROM trips;
                """)

result.write.mode('overwrite').option("header", "true").csv('/Users/danielbeach/Desktop/results.txt')
t2 = datetime.now()
x = t2-t1
print(f"it took {x}")