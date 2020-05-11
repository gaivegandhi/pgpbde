SELECT COUNT(summons_number) 
FROM hive_db.parking_violation_data 
WHERE street_code_1 = 0 OR street_code_2 = 0 OR street_code_3 = 0;
