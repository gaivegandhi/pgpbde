SELECT
tmp_table.State_Name,
tmp_table.State_district_name,
tmp_table.CC_Sex_Ratio_All_Ages_Total
FROM (
SELECT
State_Name,
State_district_name,
CC_Sex_Ratio_All_Ages_Total,
RANK() OVER 
(PARTITION BY State_Name ORDER BY
CC_Sex_Ratio_All_Ages_Total ASC) AS Rank
FROM India_Annual_Health_Survey_2012_13_DB.iahs_2012_13_orc
) tmp_table WHERE Rank < 3;
