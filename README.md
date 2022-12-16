# Road-accidents
Distributed big data systems final project
<br>
## Average age of drivers
```
avg_age_by_gender = df.groupBy('Gender').agg(F.mean('Age'))
```
![image](https://user-images.githubusercontent.com/108996318/208049626-78a07d0a-ba32-4838-b530-4ba6c68bf703.png)

## Average driving experience of drivers
```
df_gender = df.select("Gender", "Driving experience")
df_gender = df_gender.groupBy("Gender")
df_gender = df_gender.agg({"Driving experience": "mean"})
```
![image](https://user-images.githubusercontent.com/108996318/208049738-9e880191-2f68-4183-bfe2-489b06818bb0.png)

## Ratio of drivers
```
total_accidents = df.count()
genders = df.groupBy('Gender').count().withColumn('Percentage', F.round(F.col('count')/total_accidents*100, 2))
```
![image](https://user-images.githubusercontent.com/108996318/208049816-b14fd74c-aa37-44bc-8812-8126c988bcc7.png)

## Newest car
```
year_of_issue = df.select("Year of issue")
year_counts = year_of_issue.groupBy("Year of issue").agg(count("*").alias("count"))
year_counts = year_counts.orderBy(desc("count"))
top_10_years = year_counts.limit(10)
```
![image](https://user-images.githubusercontent.com/108996318/208049883-eaa143f8-6fc1-478b-a250-0919688506f6.png)

## Top 10 cities with most number of accidents
```
city_counts = df.groupBy("City").count()
sorted_counts = city_counts.sort(city_counts["count"].desc())
sorted_counts.show(10)
```
![image](https://user-images.githubusercontent.com/108996318/208049927-3424ee9b-4f73-4988-a4b8-b6e970ba34d5.png)


## Most number of accidents grouped by age and gender
```
df = df.groupBy(col("Age"), col("Gender")).agg(count("*").alias("num_accidents"))
df.orderBy("num_accidents", ascending=False)
```
![image](https://user-images.githubusercontent.com/108996318/208049962-22934f74-486a-496e-a8ed-3096050499cc.png)

