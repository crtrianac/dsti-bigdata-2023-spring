# Big Data Ecosystem

## Lab 4.2: Hive Query Language

### Goals

- Write advanced HQL queries
- Optimize the queries

### Input tables

In this lab we will the 4 IMDb datasets that are located in your group database:

- `imdb_name_basics`
- wget https://datasets.imdbws.com/name.basics.tsv.gz
- gunzip name.basics.tsv.gz

  
- - `imdb_title_basics`
- wget https://datasets.imdbws.com/title.basics.tsv.gz
- gunzip title.basics.tsv.gz

  
- `imdb_title_crew`
- wget https://datasets.imdbws.com/title.crew.tsv.gz
-  gunzip title.crew.tsv.gz

  
- `imdb_title_ratings`
- wget https://datasets.imdbws.com/title.ratings.tsv.gz
- gunzip title.ratings.tsv.gz

Make the directory 
hdfs dfs -mkdir /education/dsti_spoc/resources/lab4/IMDb_datasets
hdfs dfs -put * /education/dsti_spoc/resources/lab4/IMDb_datasets


For more informations about the datasets: [IMDb Datasets](https://www.imdb.com/interfaces/).

I need firts 


### Queries

1. Number of titles with duration superior than 2 hours
Create a Hive Table that matches the structure of the title.basics.tsv file.


SET hivevar:clusterUsername=c.triana_dsti;
SET hivevar:hiveUsername=c_triana_dsti;

CREATE EXTERNAL TABLE IF NOT EXISTS dsti_spoc.c_triana_dsti_imdb_title_basics (
 tconst STRING,
  titleType STRING,
  primaryTitle STRING,
  originalTitle STRING,
  isAdult INT,
  startYear STRING,
  endYear STRING,
  runtimeMinutes STRING,
  genres STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/education/dsti_spoc/resources/lab4/IMDb_datasets/';



SELECT COUNT(*) AS Titles_Over_2_Hours
FROM dsti_spoc.c_triana_dsti_imdb_title_basics
WHERE CAST(runtimeMinutes AS INT) > 120;



3. Average duration of titles containing the word "world" (but not words like "Underworld"). Hint: use `RLIKE` (see [RegExr](https://regexr.com/))

   SELECT AVG(CAST(runtimeMinutes AS INT)) AS Average_Duration
FROM dsti_spoc.c_triana_dsti_imdb_title_basics
WHERE primaryTitle RLIKE '\\bworld\\b'
AND runtimeMinutes RLIKE '^[0-9]+$';

5. Average rating of titles having the genre "Comedy"
SELECT AVG(r.averageRating) AS Average_Comedy_Rating
FROM imdb_title_basics b
JOIN imdb_title_ratings r ON b.tconst = r.tconst
WHERE genres RLIKE 'Comedy'
   
7. Top 5 movies directed by Tarantino

For 5., you can first try to do it in 2 seperate queries. Then try to use a single query (tip: `explode` or `array_contains`)
