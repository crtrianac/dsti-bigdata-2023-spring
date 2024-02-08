# Lab 3


1. Import /education/dsti_spoc/dsti-bigdata-2023-spring/04.data-warehousing-with-hive/lab-resources/science_federal_giving_samp.csv into your hdfs home directory

hdfs dfs -put dsti-bigdata-2023-spring/04.data-warehousing-with-hive/lab-resources/science_federal_giving_samp.csv /user/c.triana-dsti/

3. Create external hive table that wrap this file.

CREATE EXTERNAL TABLE IF NOT EXISTS  dsti_spoc.c_triana_dsti_science_federal_giving (
    cmte_nm STRING,
    cand_name STRING,
    cand_pty_affiliation STRING,
    cand_office_st STRING,
    cand_office STRING,
    contributor_name STRING,
    city STRING,
    state STRING,
    employer STRING,
    occupation STRING,
    transaction_dt DATE, 
    transaction_amt FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/c.triana-dsti/';

4. Create Optimized ORC Table with same schema. Copy data from previous table.
Compare size on corresponding hdfs directory


##Create Optimized ORC Table with same schema

CREATE TABLE dsti_spoc.c_triana_dsti_science_federal_giving_orc (
    cmte_nm STRING,
    cand_name STRING,
    cand_pty_affiliation STRING,
    cand_office_st STRING,
    cand_office STRING,
    contributor_name STRING,
    city STRING,
    state STRING,
    employer STRING,
    occupation STRING,
    transaction_dt DATE,
    transaction_amt FLOAT
)
STORED AS ORC;


##Copy data from previous table

INSERT INTO TABLE dsti_spoc.c_triana_dsti_science_federal_giving_orc 
SELECT * FROM dsti_spoc.c_triana_dsti_science_federal_giving;


to find the location you can run the following command : 

DESCRIBE FORMATTED dsti_spoc.c_triana_dsti_science_federal_giving_orc;
| Location:                     | hdfs://au/warehouse/tablespace/managed/hive/dsti_spoc.db/c_triana_dsti_science_federal_giving_orc;


DESCRIBE FORMATTED dsti_spoc.c_triana_dsti_science_federal_giving;
| Location:  hdfs://au/user/c.triana-dsti

##compare the size of hte 2 tables

[c.triana-dsti@edge-1 ~]$ hdfs dfs -du -h hdfs://au/warehouse/tablespace/managed/hive/dsti_spoc.db/c_triana_dsti_science_federal_giving_orc

409.0 K  818.1 K  hdfs://au/warehouse/tablespace/managed/hive/dsti_spoc.db/c_triana_dsti_science_federal_giving_orc/delta_0000001_0000001_0000
[c.triana-dsti@edge-1 ~]$
[c.triana-dsti@edge-1 ~]$ hdfs dfs -du -h hdfs://au/user/c.triana-dsti

39.6 M  79.2 M  hdfs://au/user/c.triana-dsti/.hiveJars
0       0       hdfs://au/user/c.triana-dsti/.staging
2.6 M   5.3 M   hdfs://au/user/c.triana-dsti/science_federal_giving_samp.csv

hdfs dfs -ls /user/c.triana-dsti/
hdfs dfs -du -h /path/to/csv/table


6. Create ORC table but partition by city

   CREATE TABLE dsti_spoc.c_triana_dsti_science_federal_giving_partitioned (
    cmte_nm STRING,
    cand_name STRING,
    cand_pty_affiliation STRING,
    cand_office_st STRING,
    cand_office STRING,
    contributor_name STRING,
    state STRING,
    employer STRING,
    occupation STRING,
    transaction_dt DATE,
    transaction_amt FLOAT
)
PARTITIONED BY (city STRING)
STORED AS ORC
LOCATION 'hdfs://au/warehouse/tablespace/managed/hive/dsti_spoc.db/c_triana_dsti_science_federal_giving_partitioned';



CONTTINUE FORM HERE 

8. Observe disk structure. Describe how this structure will impact request.
   Find 2 SQL requests that will directly be optimised
   What will be impact if data is not well distributed per city ? What will be impact on small data ?

9. Create ORC table with 10 buckets. 

10. Observe disk structure. Describe how this structure will impact request.
   Find a scenario for bucketing.
   What will be impact if data is not well distributed per city ? What will be impact on small data ?

5. Create ORC table and partitionned by city with 10 buckets
   Describe how it will behave in comparison to partionned-only and bucketed-only
