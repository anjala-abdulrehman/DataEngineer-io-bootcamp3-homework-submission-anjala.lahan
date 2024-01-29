from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


def query_2() -> str:
    return """
    SELECT
        player_name AS subject_identifier,
        'player' AS subject_type,
        CAST(team_id AS STRING) AS object_identifier,
        'team' AS object_type,
        'plays_on' AS edge_type,
        MAP(
            'number_of_games', CAST(COUNT(1) AS STRING),
            'number_of_points', CAST(SUM(pts) AS STRING)
    ) AS properties
FROM
    nba_game_details
GROUP BY
    player_name,
    team_id
    """


def job_2(spark_session: SparkSession, dataframe) -> Optional[DataFrame]:
    dataframe.createOrReplaceTempView("nba_game_details")
    return spark_session.sql(query_2())


def main():
    output_table_name: str = "nba_player_edges"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_2")
        .getOrCreate()
    )
    output_df = job_2(spark_session, spark_session.table("nba_player_edges"))
    output_df.write.mode("overwrite").insertInto(output_table_name)


if __name__ == '__main__':
    main()
