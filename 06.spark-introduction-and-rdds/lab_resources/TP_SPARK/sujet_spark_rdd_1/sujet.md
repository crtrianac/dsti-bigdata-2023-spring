1. Create a csv:
  ....
  echo "id,nom,prenom,age" >> myfile.txt
  echo "1,sauvage,pierre,31" >> myfile.txt

  #add other values
  echo "....." >> myfile.txt

=========

If you run spark in cluster, please also put myfile in the cluster

hdfs dfs -put myfile.txt ./myfile.txt

2. load csv into RDD in spark

```
val rdd = sc.textFile("file:///home/<my_user>/monfichier.txt")


spark-shell
val rdd = sc.textFile("file:///home/c.triana-dsti/myfile.txt")


# or from hdfs
val rdd = sc.textFile("hdfs:///user/c.triana-dsti/myfile.txt")

```



2b. Print content of RDD

rdd.collect().foreach(println)  <- this will return all lines

rdd.take(10).foreach(println) <- this will return the first (N) lines  you indicate


2c: Filter rdd : delete lines where age is smaller than 18 apa


val header = rdd.first()
val noHeaderRDD = rdd.filter(line => line != header)
val filteredRDD = noHeaderRDD.filter(line => {
  val parts = line.split(",")
  val age = parts(3).toInt
  age >= 35
})
filteredRDD.collect().foreach(println)

2d: Convert RDD in dataframe (google est votre ami)

val header = rdd.first()

case class Person(id: Int, name: String, Firstname: String, age: Int)

val peopleRDD = filteredRDD.map(line => {
  val parts = line.split(",")
  Person(parts(0).toInt, parts(1), parts(2), parts(3).toInt)
})

val peopleDF = spark.createDataFrame(peopleRDD)
peopleDF.show()

========

3. load csv into dataframe in Spark

3b. Print content of DF

3c: Filtrer DF: delete lines where age < 18

bonus:  In Spark native API, then in SQL (spark.sql("..."))

3d: Print minimal, maximal, average age
