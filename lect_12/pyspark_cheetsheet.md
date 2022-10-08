## PySpark

Create Spark session:
```sparksql
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
```

Read and write data:
```sparksql
df.write.csv('foo.csv', header=True)
spark.read.csv('foo.csv', header=True).show()
```

Run SQL:
```sparksql
df = spark.sql("SELECT * FROM csv.`foo.csv`")
df
```

Create View from DataFrame and query:
```sparksql
df.createOrReplaceTempView("people")

sqlDF = spark.sql("SELECT * FROM people")
```


Filter and order by:
```sparksql
df.filter(df2.salary > 5000).orderBy('firstName').show()
```





## Databricks

```sparksql
display(dbutils.fs.ls('/databricks-datasets'))
```

```sparksql
df2 = spark.read.load("/databricks-datasets/learning-spark-v2/people/people-10m.delta")
```