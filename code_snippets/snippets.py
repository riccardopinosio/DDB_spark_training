import pandas as pd

df = pd.DataFrame(
    {'a': [1,2,3],
    'b': [4,5,6]}
    )
df['a'] = [7,8,9]



df = df.drop('colname')
import pyspark.sql.functions as f
df = df.withColumn('constant', f.lit(0))


(
    df.filter(f.col('country') == 'US')
    .select(my_function(f.col('temperature')).alias('temp_transformed'))
)

(
    df.select(my_function(f.col('temperature')).alias('temp_transformed'))
    .filter(f.col('country') == 'US')
)
