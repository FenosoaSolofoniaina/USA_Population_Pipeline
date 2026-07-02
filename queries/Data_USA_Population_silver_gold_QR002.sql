CREATE OR REPLACE TABLE `data_engineer`.`gold`.`USA_rank_state_by_population_count` AS (
    SELECT
        `State_ID`,
        `State`,
        `Year`,
        `Population`,
        RANK() OVER (PARTITION BY `Year` ORDER BY `Population` DESC) AS `top_rank`
    FROM `data_engineer`.`silver`.`usa_data_population`
);

SELECT
    *
FROM `data_engineer`.`gold`.`USA_rank_state_by_population_count`
LIMIT 100;