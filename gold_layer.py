import os
from dotenv import load_dotenv

from scripts.utils import init_spark_session, define_logger


# Initialize Logger and constants
load_dotenv('.databricks.env')
CLEAN_DATA_TABLE = os.getenv('SILVER_TABLE_NAME')
ANALYTICS_SCHEMA_NAME = os.getenv('GOLD_SCHEMA_NAME')
gold_logger = define_logger(os.path.join(os.getenv('PROJECT_DIR'), 'logs/gold.log'))

# =================== HERE WE GO =================== #
gold_logger.info('==== PROGRAM ON GOLD LAYER STARTED =====')
SPARK_SESSION = init_spark_session()
gold_logger.info('Spark Session initialized')

# Create table 'TOTAL_POPULATION_TABLE'
TOTAL_POPULATION_TABLE = f'{ANALYTICS_SCHEMA_NAME}.`USA_total_population_per_year`'
SPARK_SESSION.sql(f"""
    CREATE OR REPLACE TABLE {TOTAL_POPULATION_TABLE} AS (
        SELECT
            `Year`,
            SUM(`Population`) AS `total_Population`
        FROM {CLEAN_DATA_TABLE}
        GROUP BY `Year`
    );
""")
gold_logger.info(f"Table created at {TOTAL_POPULATION_TABLE}")

# Create table 'RANK_POPULATION_TABLE'
RANK_POPULATION_TABLE = f'{ANALYTICS_SCHEMA_NAME}.`USA_rank_state_by_population_count`'
SPARK_SESSION.sql(f"""
    CREATE OR REPLACE TABLE {RANK_POPULATION_TABLE} AS (
        SELECT
            `State_ID`,
            `State`,
            `Year`,
            `Population`,
            RANK() OVER (PARTITION BY `Year` ORDER BY `Population` DESC) AS `top_rank`
        FROM {CLEAN_DATA_TABLE}
    );
""")
gold_logger.info(f"Table created at {RANK_POPULATION_TABLE}")

# Create table 'EVOLUTION_POPULATION_TABLE'
EVOLUTION_POPULATION_TABLE = f'{ANALYTICS_SCHEMA_NAME}.`USA_evolution_population_per_state`'
SPARK_SESSION.sql(f"""
    CREATE OR REPLACE TABLE {EVOLUTION_POPULATION_TABLE} AS (
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
gold_logger.info(f"Table created at {EVOLUTION_POPULATION_TABLE}")