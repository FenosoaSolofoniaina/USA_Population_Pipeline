import os
from dotenv import load_dotenv

from scripts.utils import init_spark_session


load_dotenv('.databricks.env')
spark_session = init_spark_session()
CLEAN_DATA_TABLE = os.getenv('SILVER_TABLE_NAME')
ANALYTICS_SCHEMA_NAME = os.getenv('GOLD_SCHEMA_NAME')

spark_session.sql(f"""
    CREATE OR REPLACE TABLE {ANALYTICS_SCHEMA_NAME}.`USA_total_population_per_year` AS (
        SELECT
            `Year`,
            SUM(`Population`) AS `total_Population`
        FROM {CLEAN_DATA_TABLE}
        GROUP BY `Year`
    );
""")
spark_session.sql(f"""
    CREATE OR REPLACE TABLE {ANALYTICS_SCHEMA_NAME}.`USA_rank_state_by_population_count` AS (
        SELECT
            `State_ID`,
            `State`,
            `Year`,
            `Population`,
            RANK() OVER (PARTITION BY `Year` ORDER BY `Population` DESC) AS `top_rank`
        FROM {CLEAN_DATA_TABLE}
    );
""")
spark_session.sql(f"""
    CREATE OR REPLACE TABLE {ANALYTICS_SCHEMA_NAME}.`USA_evolution_population_per_state` AS (
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
                FROM {CLEAN_DATA_TABLE}
            )
        )
        WHERE `evolution_rate` > 0
    );
""")