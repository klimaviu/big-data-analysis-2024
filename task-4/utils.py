import math
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

def clean_name(col):
    """
    Cleans a column name (in string format) by:
      1. changing them to lowercase
      2. removing unwanted symbols
      3. replacing spaces with the "_" symbol
    """
    return col.lower().replace("# ", "").replace(" ", "_")

def clean_names(df):
    """
    Cleans the column names of a PySpark dataframe by:
      1. changing them to lowercase
      2. removing unwanted symbols
      3. replacing spaces with the "_" symbol
    """
    return df.toDF(*[clean_name(c) for c in df.columns])

def haversine(lon1, lat1, lon2, lat2):
    """
    Takes two sets of coordinates and returns the distance between them in kilometers, using the Harvesine formula.
    """
    # Convert decimal degrees to radians (as trigonometry functions in Python use radians, not degrees)
    lon1, lat1, lon2, lat2 = map(math.radians, [lon1, lat1, lon2, lat2])

    # Haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    earth_radius = 6371  # Radius of the Earth in kilometers
    return c * earth_radius

haversine_udf = udf(haversine, DoubleType())