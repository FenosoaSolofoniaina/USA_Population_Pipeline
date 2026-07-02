--MAIN QUERY
SELECT
    `State_ID`,
    `State`,
    `Year`,
    `Population`,
    `Extracted_at`
FROM `data_engineer`.`silver`.`usa_data_population`
LIMIT 100;