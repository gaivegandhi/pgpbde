set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE hive_db.parking_violation_data PARTITION(month) 
SELECT *, CONCAT(SUBSTR(issue_date,7,4), SUBSTR(issue_date,1,2)) AS MONTH 
FROM hive_db.parking_violation_data_temp 
WHERE (issue_date like '%2017');
