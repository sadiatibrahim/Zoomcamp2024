CREATE OR REPLACE EXTERNAL TABLE `virtual-aileron-412616.ny_taxi.external_green_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://sadiat-etl-bucket/green-taxi-2022/green_taxi_01.parquet',
  'gs://sadiat-etl-bucket/green-taxi-2022/green_taxi_02.parquet', 
  'gs://sadiat-etl-bucket/green-taxi-2022/green_taxi_03.parquet',
  'gs://sadiat-etl-bucket/green-taxi-2022/green_taxi_04.parquet',
  'gs://sadiat-etl-bucket/green-taxi-2022/green_taxi_05.parquet',
  'gs://sadiat-etl-bucket/green-taxi-2022/green_taxi_06.parquet',
  'gs://sadiat-etl-bucket/green-taxi-2022/green_taxi_07.parquet',
  'gs://sadiat-etl-bucket/green-taxi-2022/green_taxi_08.parquet',
  'gs://sadiat-etl-bucket/green-taxi-2022/green_taxi_09.parquet',
  'gs://sadiat-etl-bucket/green-taxi-2022/green_taxi_10.parquet',
  'gs://sadiat-etl-bucket/green-taxi-2022/green_taxi_11.parquet',
  'gs://sadiat-etl-bucket/green-taxi-2022/green_taxi_12.parquet'])

-- create table
CREATE OR REPLACE TABLE ny_taxi.green_tripdata_non_partitioned AS
SELECT * FROM ny_taxi.external_green_tripdata

--Question1
SELECT COUNT(*) FROM ny_taxi.external_green_tripdata;

--Question2

select distinct count(PULocationID) from ny_taxi.green_tripdata_non_partitioned
select distinct count(PULocationID) from ny_taxi.external_green_tripdata

--Question 3
select count(*) from  ny_taxi.external_green_tripdata
where fare_amount = 0

--Question 4
CREATE OR REPLACE TABLE ny_taxi.green_tripdata_partitioned
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PULocationID AS
SELECT * FROM ny_taxi.external_green_tripdata


--Question 5

SELECT DISTINCT PULocationID
FROM `ny_taxi.green_tripdata_non_partitioned`
where DATE(lpep_pickup_datetime) BETWEEN PARSE_DATE('%m/%d/%Y', '06/01/2021') AND PARSE_DATE('%m/%d/%Y', '06/30/2022')

SELECT DISTINCT PULocationID
FROM `ny_taxi.green_tripdata_partitioned`
where DATE(lpep_pickup_datetime) BETWEEN PARSE_DATE('%m/%d/%Y', '06/01/2021') AND PARSE_DATE('%m/%d/%Y', '06/30/2022')
