from collections import namedtuple
from chispa import assert_df_equality
from pyspark.sql.types import StructType, StructField, StringType, LongType

from infrastructure.data_quality.src.job_1 import job_1

PlayerDetails = namedtuple("PlayerDetails", "player_name team_id pts")  # represents the input
PlayerPoints = namedtuple("PlayerPoints", "player_name team_id number_of_games number_of_points")  # represents the o/p


def test_dataframe_job1(spark_session):

    source_data = [
        PlayerDetails("A BC", '112233', 14),
        PlayerDetails("A BC", '112233', 21),
        PlayerDetails("X YZ", '223344', 8),
        PlayerDetails("X YZ", '223344', 22)
    ]
    source_df = spark_session.createDataFrame(source_data)

    actual_df = job_1(spark_session, source_df)
    expected_data = [
        PlayerPoints("A BC", '112233', 2, 35),
        PlayerPoints("X YZ", '223344', 2, 30),
    ]
    schema1 = StructType([
        StructField('player_name', StringType(), True),
        StructField('team_id', StringType(), True),
        StructField('number_of_games', LongType(), False),
        StructField('number_of_points', LongType(), True),
    ])
    expected_df = spark_session.createDataFrame(expected_data, schema=schema1)
    assert_df_equality(actual_df, expected_df)
