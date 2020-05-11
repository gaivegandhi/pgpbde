SELECT HOUR(FROM_UNIXTIME(UNIX_TIMESTAMP(CONCAT(issue_date,' ',violation_time,'M'), 'MM/dd/yyyy hhmma'))) AS Time_of_day, COUNT(*) 
FROM hive_db.parking_violation_data 
GROUP BY HOUR(FROM_UNIXTIME(UNIX_TIMESTAMP(CONCAT(issue_date,' ',violation_time,'M'), 'MM/dd/yyyy hhmma'))) 
ORDER BY Time_of_day;
