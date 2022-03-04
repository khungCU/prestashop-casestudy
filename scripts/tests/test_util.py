from scripts.utils import explode_nested_type
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import SparkSession
import pytest


@pytest.fixture
def spark_session():
    return SparkSession.builder \
                    .master('local[*]') \
                    .appName('test_spark_cutomized_func') \
                    .getOrCreate()


@pytest.fixture
def dataframe_with_array(spark_session):
    return spark_session.createDataFrame(
    [
        (1, "storeA" , ["banana", "apple", "orange"]),  # create your data here, be consistent in the types.
        (2, "storeB" , ["smart phone", "computer", "television"]),
    ],
    ["id", "store", "inventory"]  # add your column names here
)

@pytest.fixture
def dataframe_with_struct(spark_session):
    schema = StructType([
                StructField("user_id", IntegerType(), True),
                StructField("user_name", StringType(), True),
                StructField("orders", StructType(
                    [
                        StructField("order_id", IntegerType(), True),
                        StructField("order_price", IntegerType(), True),
                        StructField("order_userid", IntegerType(), True),
                    ])),
             ])
    
    return  spark_session.createDataFrame(
                    [
                        (1, "UserA" , ({"id":1,"price": 500,"userid":1})), 
                        (2, "UserB" , ({"id":3,"price": 600,"userid":2}))
                    ],
                    schema
            )



def test_explode_dataframe_with_array_type(dataframe_with_array):
    
    assert dataframe_with_array.count() == 2, "Init total data count should equal to 2 !!"
    assert len(dataframe_with_array.columns) == 3, "Init total column should equal to 3 !!"
    
    test_df = explode_nested_type(dataframe_with_array)
    
    assert test_df.count() == 6, "After explode The total row count not equal to 6 !!"
    assert len(test_df.columns) == 3, "After explode with ararry data set total column should stay the same"


def test_explode_dataframe_with_struct_type(dataframe_with_struct):
    
    assert dataframe_with_struct.count() == 2, "Init total data count should equal to 2 !!"
    assert len(dataframe_with_struct.columns) == 3, "Init total column should equal to 3 !!"
    
    test_df = explode_nested_type(dataframe_with_struct, "struct")
    
    assert test_df.count() == 2, "After explode with struct data set total data count should stay the same"
    assert len(test_df.columns) == 5 , "After explode with struct data set total column count should equal to 5"

    
