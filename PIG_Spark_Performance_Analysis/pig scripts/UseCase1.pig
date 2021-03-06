-- File: UseCase1.pig
-- Description:

-- For Running On Local System With Sample Data On Local File System
-- records = LOAD '/home/cloudera/workspace/Spark_And_PIG_Performance_POC/data/sample data/trip_yellow_taxi.data' USING PigStorage(',') AS (VendorID, TPEP_PickUp_DateTime, TPEP_DropOff_DateTime, Passenger_Count, Trip_Distance, RateCodeID, Store_And_Forward_Flag, PULocationID, DOLocationID, Payment_Type, Fare_Amount, Extra, MTA_Tax, Tip_Amount, Tolls_Amount, Improvement_Surcharge, Total_Amount);

-- For Running On Local System With Big Data On Local File System
-- records = LOAD '/home/cloudera/workspace/Spark_And_PIG_Performance_POC/data/big data/yellow_tripdata_*' USING PigStorage(',') AS (VendorID, TPEP_PickUp_DateTime, TPEP_DropOff_DateTime, Passenger_Count, Trip_Distance, RateCodeID, Store_And_Forward_Flag, PULocationID, DOLocationID, Payment_Type, Fare_Amount, Extra, MTA_Tax, Tip_Amount, Tolls_Amount, Improvement_Surcharge, Total_Amount);

-- For Running On AWS EMR With Sample Data On S3
records = LOAD 's3://pgpbdepigdata/input/trip_yellow_taxi.data' USING PigStorage(',') AS (VendorID, TPEP_PickUp_DateTime, TPEP_DropOff_DateTime, Passenger_Count, Trip_Distance, RateCodeID, Store_And_Forward_Flag, PULocationID, DOLocationID, Payment_Type, Fare_Amount, Extra, MTA_Tax, Tip_Amount, Tolls_Amount, Improvement_Surcharge, Total_Amount);

filtered_records = FILTER records BY NOT (VendorID == 'VendorID');

final_records = FILTER filtered_records BY (VendorID == '2' AND TPEP_PickUp_DateTime == '2017-10-01 00:15:30' AND TPEP_DropOff_DateTime == '2017-10-01 00:25:11' AND Passenger_Count == '1' AND Trip_Distance == '2.17');

-- For Running On Local System
-- STORE final_records INTO '/home/cloudera/workspace/Spark_And_PIG_Performance_POC/output/pig/usecase1.output';

-- For Running On AWS EMR With Sample Data On S3
STORE final_records INTO 's3://pgpbdepigdata/output/usecase1.output';
