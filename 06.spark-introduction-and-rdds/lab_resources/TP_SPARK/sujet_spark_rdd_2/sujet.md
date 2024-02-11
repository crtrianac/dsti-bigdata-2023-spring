

1. Create a csv:
````
id,author,genre,quantity
1,hunter.fields,romance,15
2,leonard.lewis,thriller,81
3,jason.dawson,thriller,90
4,andre.grant,thriller,25
5,earl.walton,romance,40
6,alan.hanson,romance,24
7,clyde.matthews,thriller,31
8,josephine.leonard,thriller,1
9,owen.boone,sci-fi,27
10,max.mcBride,romance,75
```
=========
echo "id,author,genre,quantity" > books.csv
echo "1,hunter.fields,romance,15" >> books.csv
echo "2,leonard.lewis,thriller,81" >> books.csv
echo "3,jason.dawson,thriller,90" >> books.csv
echo "4,andre.grant,thriller,25" >> books.csv
echo "5,earl.walton,romance,40" >> books.csv
echo "6,alan.hanson,romance,24" >> books.csv
echo "7,clyde.matthews,thriller,31" >> books.csv
echo "8,josephine.leonard,thriller,1" >> books.csv
echo "9,owen.boone,sci-fi,27" >> books.csv
echo "10,max.mcBride,romance,75" >> books.csv

hdfs dfs -put books.csv ./books.csv
__________
import java.io.{File, PrintWriter}

val data = Seq(
 "id,author,genre,quantity",
  "1,hunter.fields,romance,15",
  "2,leonard.lewis,thriller,81",
  "3,jason.dawson,thriller,90",
  "4,andre.grant,thriller,25",
  "5,earl.walton,romance,40",
  "6,alan.hanson,romance,24",
  "7,clyde.matthews,thriller,31",
  "8,josephine.leonard,thriller,1",
  "9,owen.boone,sci-fi,27",
  "10,max.mcBride,romance,75"
)

spark-shell
val pw = new PrintWriter(new File("books.csv"))
data.foreach(pw.println)
pw.close()


2. Load csv into dataframe in Spark

import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder.appName("BooksData").getOrCreate()

val booksDF = spark.read .option("header", "true").option("inferSchema", "true").csv("books.csv")

booksDF.show()

3. Generate number of writes per genre

val genreCounts = booksDF.groupBy("genre").count()
genreCounts.show()


4. Ranking authors per number of written livre:

val authorRanking = booksDF.groupBy("author").agg(sum("quantity").alias("total_books")).orderBy(desc("total_books"))
authorRanking.show()



Ex of output:
ranking,author,genre,quantity
1,jason.dawson,thriller,90
2,leonard.lewis,thriller,81
3,max.mcBride,romance,75
4,earl.walton,romance,40
5,clyde.matthews,thriller,31
6,owen.boone,sci-fi,27
7,andre.grant,thriller,25
8,alan.hanson,romance,24
9,hunter.fields,romance,15
10,josephine.leonard,thriller,1

5. Rename name in a "standard" way 'jason.dawson' => 'Jason Dawson'

https://sparkbyexamples.com/spark/spark-sql-window-functions/

import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.DataFrame

// Define a UDF to convert 'jason.dawson' => 'Jason Dawson'
val standardizeName: String => String = _.split("\\.").map(_.capitalize).mkString(" ")
val standardizeNameUDF = udf(standardizeName)

// Assuming booksDF is your DataFrame
val booksDFStandardized: DataFrame = booksDF.withColumn("author", standardizeNameUDF($"author"))

booksDFStandardized.show()



6. Load following set

val input = Seq(
  ("100","John", Some(35),None),
  ("100","John", None,Some("Georgia")),
  ("101","Mike", Some(25),None),
  ("101","Mike", None,Some("New York")),
  ("103","Mary", Some(22),None),
  ("103","Mary", None,Some("Texas")),
  ("104","Smith", Some(25),None),
  ("105","Jake", None,Some("Florida"))).toDF("id", "name", "age", "city")

scala> input.show
+---+-----+----+--------+
| id| name| age|    city|
+---+-----+----+--------+
|100| John|  35|    null|
|100| John|null| Georgia|
|101| Mike|  25|    null|
|101| Mike|null|New York|
|103| Mary|  22|    null|
|103| Mary|null|   Texas|
|104|Smith|  25|    null|
|105| Jake|null| Florida|
+---+-----+----+--------+


7. Merge cells of same id:

val mergedDF = input.groupBy("id").agg(first("name", ignoreNulls = true).alias("name"),first("age", ignoreNulls = true).alias("age"),first("city", ignoreNulls = true).alias("city"))

mergedDF.show()


scala> solution.show()
+---+-----+----+--------+
|id |name |age |city    |
+---+-----+----+--------+
|100|John |35  |Georgia |
|101|Mike |25  |New York|
|103|Mary |22  |Texas   |
|104|Smith|25  |null    |
|105|Jake |null|Florida |
+---+-----+----+--------+
