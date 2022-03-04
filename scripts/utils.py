from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def explode_nested_type(df: DataFrame, data_type = "array"):
    
    normal_column = []
    to_be_explode_column = []
    explode = []

    for column in df.dtypes:
        if column[1].startswith(data_type):
            to_be_explode_column.append(column)
        else:
            normal_column.append(column[0])

    # explode the column
    for column in to_be_explode_column:
        
        if data_type == "array":
            explode.append(F.explode(df[column[0]]).alias(column[0]))
        elif data_type == "struct":
            explode.append(F.col(f"{column[0]}.*"))

    # put all back together
    df = df.select(
        *normal_column,
        *explode
    )
    
    return df