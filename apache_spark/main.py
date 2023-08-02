import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, explode, udf, array, collect_list
from pyspark.sql.types import StringType, ArrayType, FloatType

from time import time
from os import listdir

spaceDeleteUDF = udf(lambda s: s.replace(" ", ""), StringType())


def outer_function(a):
    return 2*a
    #return np.outer(a, a)


outerUDF = udf(outer_function, ArrayType(FloatType()))

if __name__ == "__main__":
    """
        Usage: 1.word counts on documents
               2.adj matrix creation
    """
    spark = SparkSession \
        .builder \
        .master("local") \
        .appName("Word_Count") \
        .getOrCreate()

    sc = spark.sparkContext

    files = listdir("data/Dummy_docs")
    start = time()
    for filename in files:
        df = spark.read.text(f"data/Dummy_docs/{filename}")
        df = df.withColumn('Word', spaceDeleteUDF('value'))
        df_count = (df.withColumn('word', explode(split(col('Word'), ' ')))
                    .groupby('word')
                    .count()
                    .withColumn('count2',col('count'))
                    )
        #df_count.show()
        col1 = df_count.select('word','count')
        #col1.show()
        col2 = df_count.select('count2')
        #col2.show()
        matrix = col1.crossJoin(col2)
        #matrix.show()
        matrix_to_df = matrix.withColumn('crossprod', col('count')*col('count2')).groupby('word').agg(collect_list('crossprod'))
        #matrix_to_df.show()

    spark.stop()
    end = time()
    print(f"***----->Execution time :{end - start}")
