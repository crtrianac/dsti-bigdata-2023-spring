# Big Data Ecosystem

## Lab 4.1: Hive Warehouse

### Goals

- Create an external table on top of data stored as CSV
- Create a managed ORC table
- Load data from the CSV table to the ORC table (with some transformations)

### Create an external table

For this lab we will be using a very small dataset of NYC taxi drivers.

Using the official [Hive Data Definition Langage](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL):

1. Using the HDFS CLI, take a look at the data used for this lab at `/education/dsti_2023_fallbda_1/resources/lab4/nyc_drivers/drivers.csv`

2. Copy the `nyc_drivers` folder to your user directory in HDFS:

   ```bash
   hdfs dfs -mkdir -p "/education/dsti_2023_fallbda_1/$USER/lab4"

   hdfs dfs -cp /education/dsti_2023_fallbda_1/resources/lab4/nyc_drivers "/education/dsti_2023_fallbda_1/$USER/lab4"
   ```

3. Open a Beeline session by typing `beeline`

4. Create an external table targeting our data with this statement (to be completed, replace `YOUR_USERNAME`):

   ```sql
   SET hivevar:clusterUsername=p.nom-dsti;
   -- DO NOT USE '.' NOR '-' IN HIVEUSERNAME
   SET hivevar:hiveUsername=p_nom_dsti;

   CREATE EXTERNAL TABLE dsti_2023_fallbda_1.${hiveUsername}_nyc_drivers_ext (
     driver_id INT,
     -- COMPLETE HERE
   )
   ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
   STORED AS TEXTFILE
   LOCATION -- COMPLETE HERE
   TBLPROPERTIES ('skip.header.line.count'='1');
   ```

   SET hivevar:clusterUsername=c.triana_dsti;
SET hivevar:hiveUsername=c_triana_dsti;

CREATE EXTERNAL TABLE IF NOT EXISTS dsti_spoc.c_triana_dsti_nyc_drivers_ext (
  driver_id INT,
  name STRING,
  ssn STRING,
  location STRING,
  certified STRING,
  wage_plan STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE
LOCATION '/education/dsti_spoc/c.triana-dsti/lab4/nyc_drivers'
TBLPROPERTIES ('skip.header.line.count'='1');


5. Check that the table is correctly created by selecting all the data in it. **If you see only `NULL` values, your schema is not correct.**

   SELECT * FROM dsti_spoc.c_triana_dsti_nyc_drivers_ext LIMIT 10;


### Create a managed ORC table

**Tip:** to create a managed ORC table, you don't have to specify a `LOCATION` nor a `SERDE`:

```sql
CREATE ...
STORED AS ORC;
```

1. Create a managed ORC table (**not external**) that must have the same schema as the external table created above (`${username}_nyc_drivers_ext`) but with:
   1. The `_ext` prefix removed from the name: `${username}_nyc_drivers`
   2. The column `name` divided into `first_name` and `last_name`
   3. The column `location` renamed as `address` (because `LOCATION` is a Hive keyword)
   4. The column `certified` as a `BOOLEAN`

  CREATE TABLE dsti_spoc.c_triana_dsti_nyc_drivers (
  driver_id INT,
  first_name STRING,
  last_name STRING,
  address STRING,
  certified BOOLEAN,
  wage_plan STRING
)
STORED AS ORC;

      
2. Check that your table was created using the HDFS CLI at `/warehouse/tablespace/managed/hive/dsti_2023_fallbda_1.db/${USER}_nyc_drivers` (should be empty)

   hdfs dfs -ls /warehouse/tablespace/managed/hive/dsti_spoc.db/c_triana_dsti_nyc_drivers
The path exist so it has being creted, now we need to upload hte data. 

### Load data from the CSV table to the ORC table

Now we want to populate our ORC table from our CSV table. Using the [Hive Data Manipulation Language](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DML):

1. Write a statement to insert data to the ORC table by applying 2 transformations (check the available [HiveQL string functions](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+UDF#LanguageManualUDF-StringFunctions)):
   - Split `name` into `first_name` and `last_name`
   - Transform `certified` from `STRING` to `BOOLEAN`
   - Rename `location` to `address`
  
     INSERT INTO TABLE dsti_spoc.c_triana_dsti_nyc_drivers
SELECT
  driver_id,
  split(name, ' ')[0] AS first_name,
  split(name, ' ')[1] AS last_name,
  location AS address,
  (certified = 'yes') AS certified,
  wage_plan
FROM
  dsti_spoc.c_triana_dsti_nyc_drivers_ext;

2. Execute your query
3. Check what the data looks like in the managed table using the HDFS CLI at
   SELECT * FROM dsti_spoc.c_triana_dsti_nyc_drivers LIMIT 10;


you cant really do that 
`/warehouse/tablespace/managed/hive/dsti_2023_fallbda_1.db/${username}_nyc_drivers`
