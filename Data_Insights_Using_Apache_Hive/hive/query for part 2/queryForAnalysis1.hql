SELECT violation_code, COUNT(violation_code) AS Violation_Frequency 
FROM hive_db.parking_violation_data 
GROUP BY violation_code 
ORDER BY Violation_Frequency DESC LIMIT 5;
