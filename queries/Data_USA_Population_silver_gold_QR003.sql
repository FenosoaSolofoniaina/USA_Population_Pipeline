CREATE OR REPLACE TABLE `data_engineer`.`gold`.`USA_evolution_population_per_state` AS (
    SELECT
        `State_ID`,
        `State`,
        `Year`,
        `evolution_rate`
    FROM (
        SELECT
            *,
            COALESCE((`Population` - `previous_year_population`) * 100 / `previous_year_population`, 0) AS `evolution_rate`
        FROM (
            SELECT
                `State_ID`,
                `State`,
                `Year`,
                `Population`,
                LAG(`Year`) OVER (PARTITION BY `State_ID` ORDER BY `Year`) AS previous_year,
                LAG(`Population`) OVER (PARTITION BY `State_ID` ORDER BY `Year`) AS previous_year_population
            FROM `data_engineer`.`silver`.`usa_data_population`
        )
    )
    WHERE `evolution_rate` > 0
);


SELECT
    *
FROM `data_engineer`.`gold`.`USA_evolution_population_per_state`
LIMIT 100;
