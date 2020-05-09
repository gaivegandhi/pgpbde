SELECT
tmp_table.State_Name,
tmp_table.State_district_name,
tmp_table.Population_Per_House_Hold
FROM (
SELECT
State_Name,
State_district_name,
(AA_Population_Total/AA_Households_Total) AS Population_Per_House_Hold,
RANK() OVER 
(PARTITION BY State_Name ORDER BY
(AA_Population_Total/AA_Households_Total) DESC) AS Rank
FROM India_Annual_Health_Survey_2012_13_DB.iahs_2012_13_orc
) tmp_table WHERE Rank < 3;
