from collections import namedtuple
from chispa import assert_df_equality
from pyspark.sql.types import StructType, StructField, StringType, MapType

from infrastructure.data_quality.src.job_2 import job_2

PlayerDetails = namedtuple("PlayerDetails", "player_name team_id pts")  # represents the input
PlayerProperties = namedtuple("PlayerProperties",  # represents the output
                              "subject_identifier subject_type object_identifier object_type edge_type properties")


def test_dataframe_job2(spark_session):
    source_data = [
        PlayerDetails("A BC", '112233', 14),
        PlayerDetails("A BC", '112233', 21),
        PlayerDetails("X YZ", '223344', 8),
        PlayerDetails("X YZ", '223344', 22)
    ] # dummy input data
    source_df = spark_session.createDataFrame(source_data)
    actual_df = job_2(spark_session, source_df)

    schema = StructType([
        StructField('subject_identifier', StringType(), True),
        StructField('subject_type', StringType(), False),
        StructField('object_identifier', StringType(), True),
        StructField('object_type', StringType(), False),
        StructField('edge_type', StringType(), False),
        StructField('properties', MapType(StringType(), StringType(), True), False)
    ])
    # expected output for dummy output data
    expected_data = [
        PlayerProperties("A BC", 'player', '112233', 'team', 'plays_on', {"number_of_games": "2", "number_of_points": "35"}),
        PlayerProperties("X YZ", 'player', '223344', 'team', 'plays_on', {"number_of_games": "2", "number_of_points": "30"}),
    ]

    expected_df = spark_session.createDataFrame(expected_data, schema=schema)
    assert_df_equality(actual_df, expected_df)
