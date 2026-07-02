SELECT
    `Extracted_at`,
    COUNT(`State_ID`) as `total_rows`
FROM `data_engineer`.`bronze`.`usa_data_population`
GROUP BY `Extracted_at`
ORDER BY `Extracted_at` DESC
LIMIT 100;