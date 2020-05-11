SELECT vehicle_body_type, COUNT(vehicle_body_type) AS Violation_Frequency 
FROM hive_db.parking_violation_data 
GROUP BY vehicle_body_type 
ORDER BY Violation_Frequency DESC LIMIT 5;
