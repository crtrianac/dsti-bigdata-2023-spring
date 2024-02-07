SET hivevar:clusterUsername=f.lastname-cs;
-- DO NOT USE '.' NOR '-' IN HIVEUSERNAME
SET hivevar:hiveUsername=f_lastname_cs;

CREATE EXTERNAL TABLE IF NOT EXISTS dsti_2023_fallbda_1.${hiveUsername}_nyc_drivers_ext (
  driver_id INT,
  name STRING,
  ssn INT,
  location STRING,
  certified STRING,
  wage_plan STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE
LOCATION '/education/dsti_2023_fallbda_1/${clusterUsername}/lab4/nyc_drivers'
TBLPROPERTIES ('skip.header.line.count'='1');


###MY version 


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

