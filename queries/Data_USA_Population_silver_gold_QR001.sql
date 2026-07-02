CREATE OR REPLACE TABLE `data_engineer`.`gold`.`USA_total_population_per_year` AS (
    SELECT
        `Year`,
        SUM(`Population`) AS `total_Population`
    FROM `data_engineer`.`silver`.`usa_data_population`
    GROUP BY `Year`
);

SELECT
    *
FROM `data_engineer`.`gold`.`USA_total_population_per_year`
LIMIT 100;