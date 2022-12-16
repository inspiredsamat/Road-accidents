from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName('RoadAccidentsAnalysis').getOrCreate()

df = spark.read.csv(
    '/content/drive/MyDrive/Colab Notebooks/final_project_data/road accidents.csv', header=True, inferSchema=True)

avg_age_by_gender = df.groupBy('Gender').agg(F.mean('Age')).show()

total_accidents = df.count()

genders = df.groupBy('Gender').count().withColumn(
    'Percentage', F.round(F.col('count')/total_accidents*100, 2)).show()

df_gender = df.select("Gender", "Driving experience")

df_gender = df_gender.groupBy("Gender")

df_gender = df_gender.agg(
    {"Driving experience": "mean"})

df_gender.show()

year_of_issue = df.select("Year of issue")

year_counts = year_of_issue.groupBy(
    "Year of issue").agg(count("*").alias("count"))

year_counts = year_counts.orderBy(desc("count"))

top_10_years = year_counts.limit(10)

print("Top 10 years with the most number of vehicles:")
top_10_years.show()

city_counts = df.groupBy("City").count()

sorted_counts = city_counts.sort(city_counts["count"].desc())

sorted_counts.show(11)

df = df.groupBy(col("Age"), col("Gender")) \
    .agg(count("*").alias("num_accidents"))

df.orderBy("num_accidents", ascending=False).show(10)
