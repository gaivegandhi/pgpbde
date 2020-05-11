SELECT vehicle_make, COUNT(vehicle_make) AS Violation_Frequency 
FROM hive_db.parking_violation_data 
WHERE vehicle_make IS NOT NULL 
GROUP BY vehicle_make 
ORDER BY Violation_Frequency DESC LIMIT 5;
