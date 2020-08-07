# Databricks notebook source
# MAGIC %md
# MAGIC # IoT databricks ETL example
# MAGIC 
# MAGIC In this notebook we are going to carry out some transformations and analysis on the IoT data provided by databricks, to get a feel for spark dataframes, which are essentially resilient distributed datasets (rdds) with an additional structured API on top, which resembles the pandas/R dataframe API.
# MAGIC 
# MAGIC This IoT data is stored as a collection of .json files, and contains information on telemetry recorded from multiple devices in different countries. Although this dataset is static, in a real application, there would be a continuous generation of this type of data by the devices (e.g. every second), and this data would be recorded by e.g. azure IoT hub and then exported to e.g. azure data lake. We can then envisage a spark job running every night, that would aggregate this data for downstream consumption and write it back to the data lake, or analyze the data directly (e.g. score a MLlib machine learning model on the new data, save accuracy metrics to monitor data drift, and retrain the model). E.g., in the following a spark job could run every night and aggregate the IoT data for downstream consumption:
# MAGIC 
# MAGIC <img src ='/files/shared_uploads/images/architecture_spark_ETL.png'>

# COMMAND ----------

# MAGIC %md
# MAGIC ## basic dbfs operations

# COMMAND ----------

# MAGIC %md
# MAGIC Each databricks cluster comes with the *dbfs*, or databricks file system. It is a distributed file system that makes it easier to navigate folders and read/write data (the dbfs is a databricks feature, not a spark feature). Advantages of dbfs are:
# MAGIC 
# MAGIC -  is a file system associated with the cluster; so data saved to it is persisted when the cluster is terminated (but not when the cluster is erased)
# MAGIC - one can mount external storage on it (eg S3 bucket, or google cloud storage bucket, or azure blob storage/datalake) and interact with the files there using file semantics rather than URIs
# MAGIC - databricks offers various magic commands to navigate the file system
# MAGIC 
# MAGIC See https://docs.databricks.com/data/databricks-file-system.html#dbfs-dbutils for the command list

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

dbutils.fs.ls('.')

# COMMAND ----------

# MAGIC %fs ls databricks-datasets

# COMMAND ----------

# MAGIC %fs ls databricks-datasets/iot

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading and checking out data (with schemas)

# COMMAND ----------

iot_df = spark.read.json('/databricks-datasets/iot/iot_devices.json')

# COMMAND ----------

# this will trigger an action
iot_df.take(3) # iot_df.head(3) does the same thing

# COMMAND ----------

# MAGIC %md
# MAGIC Note that a dataframe is, from Spark's perspective, just a Dataset of Type "ROW" (an array of elements of type ROW).

# COMMAND ----------

# nicer output, but can be slower than .take or .head as it retrieves more rows
display(iot_df)

# COMMAND ----------

# check the schema
iot_df.schema

# COMMAND ----------

# MAGIC %md
# MAGIC Spark has determined the schema of our dataframe itself, i.e., it performed schema-on-read. Spark does this by glimpsing at a few values for each column and inferring the schema, similarly to what pd.read_csv and read_csv in R would do. This is fine for interactive analysis, but for ETL it is better to specify the schema of the data manually (you SHOULD always do it when the source file is csv or json. If the source file is parquet, then you can be more lenient as the types are already encoded in the filetype). Let's try that:

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, LongType, DoubleType, TimestampType
# pyspark.sql is the main module for pyspark dataframe operations. You can check out the API for it at https://spark.apache.org/docs/latest/api/python/pyspark.sql.html

IoTSchema = StructType([
  StructField('battery_level', LongType(), True),
  StructField('c02_level', LongType(), True),
  StructField('cca2', StringType(), True),
  StructField('cca3', StringType(), True),
  StructField('cn', StringType(), True),
  StructField('device_id', LongType(), True),
  StructField('device_name', StringType(), True),
  StructField('humidity', LongType(), True),
  StructField('ip', StringType(), True),
  StructField('latitude', DoubleType(), True),
  StructField('lcd', StringType(), True),
  StructField('longitude', DoubleType(), True),
  StructField('scale', StringType(), True),
  StructField('temp', LongType(), True),
  StructField('timestamp', LongType(), True)]
)

# don't forget the () in the column types

iot_df = spark.read.schema(IoTSchema).json('/databricks-datasets/iot/iot_devices.json')

# COMMAND ----------

display(iot_df)

# COMMAND ----------

# MAGIC %md
# MAGIC We now have a distributed dataframe we can work with. Always keep in mind the distributed nature of the dataframe: it might look like an R or Pandas dataframe, but it is a wildly different beast under the cover. When we operate on it we might use transformations familiar from R or pandas, but those transformations are then converted by Catalyst into Spark code running on Rdds, which are again very different animals than pandas or R dataframe operations.

# COMMAND ----------

# MAGIC %md
# MAGIC # Transforming data
# MAGIC 
# MAGIC Although this data is already quite clean, in principle we now want to perform some cleaning operations on it, before performing e.g. any aggregation/statistics. For instance, we will want to transform the Unix timestamp into something which we can read and from which we can extract dates for grouping, maybe drop some columns, maybe remove some outliers/fill na values.
# MAGIC When developing an application in spark (whether for ETL or machine learning/analytics), it is useful to divide the process into the phases of exploration and productionization:
# MAGIC 
# MAGIC - in the exploration stage we just write notebooks with 'free floating' code inside chunks. We are focusing on exploring the data to identify the transformations we eventually want to apply. At this stage, a specification  of how the data will be used downstream is paramount
# MAGIC - in the productionization stage, we take the result of the exploration stage and we abstract them into reusable pieces of code. These can be included in libraries, so they can be more easily maintained, extended and used with different data

# COMMAND ----------

from pyspark.sql import functions as f
from pyspark.sql import DataFrame

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exploration of the data:

# COMMAND ----------

iot_df.head()

# COMMAND ----------

iot_df.count() # n of observations

# COMMAND ----------

iot_df[['scale']].distinct().count()

# COMMAND ----------

# MAGIC %md
# MAGIC There's only one unit of measurement ('scale'). We can drop it. Also we need only keep one identifier for the country (say cca3, and we drop cca2 and cn). Let's check some stats:

# COMMAND ----------

display(iot_df.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC Let's check some nulls:

# COMMAND ----------

iot_df.filter(f.col('cca3').isNull() | f.col('device_id').isNull()).show() # no nulls for country or id

# COMMAND ----------

iot_df[['device_id']].distinct().count() # devices are the same as the number of rows, so we only have one observation per device

# COMMAND ----------

# MAGIC %md
# MAGIC Note the many different ways in which we can create the timestamp column. Although they look different, their essence is the same, namely, they specify an expression. **A column in Spark is just the specification of an expression for a computation to be performed for each row when an action is triggered**.

# COMMAND ----------

# for instance, the following expression specifies a column
f.to_utc_timestamp(f.from_unixtime(iot_df['timestamp']/1000, 'yyy-MM-dd hh:mm:ss'), 'Europe/Amsterdam')

# COMMAND ----------

iot_df = iot_df.withColumn('time',f.to_utc_timestamp(f.from_unixtime(f.col('timestamp')/1000, 'yyy-MM-dd hh:mm:ss'), 'Europe/Amsterdam'))
  
  # these also work, and are wholly equivalent:
  # iot_df.select(f.expr('*'), f.to_utc_timestamp(f.from_unixtime(iot_df['timestamp']/1000, 'yyy-MM-dd hh:mm:ss'), 'Europe/Amsterdam').alias('time')
  # iot_df.select(f.expr('*'), f.expr("to_utc_timestamp(from_unixtime(timestamp/1000, 'yyy-MM-dd hh:mm:ss'), 'Europe/Amsterdam') AS time"))
  # iot_df.selectExpr('*', "to_utc_timestamp(from_unixtime(timestamp/1000, 'yyy-MM-dd hh:mm:ss'), 'Europe/Amsterdam') AS time")
  
  # HOWEVER, this does not work, as dataframes are immutable. While the above specify a (lazy) transformation,
  # the command below tries to make an assignment:
  # iot_df['timestamp'] = f.to_utc_timestamp(f.from_unixtime(iot_df['timestamp']/1000, 'yyy-MM-dd hh:mm:ss'), 'Europe/Amsterdam')
  # the right hand side of the assignment is fine; it's the assignment that's the problem

iot_df = iot_df.drop(*['cca2', 'cn', 'timestamp', 'scale'])

iot_df = (iot_df.withColumn('Date', f.to_date('time'))
            .withColumn('Month', f.month('Date'))
            .withColumn('Year', f.year('Date'))
           )

# COMMAND ----------

iot_df[['Date']].distinct().show() # we also have data from a single date in the sample

# COMMAND ----------

# we have multiple observations per country on this date
observations = (iot_df.groupby(['cca3'])
.count()
.withColumnRenamed('count', 'n_observations')
.sort(f.col('count').desc())
)

# COMMAND ----------

observations.show()

# COMMAND ----------

display(observations[['cca3', 'n_observations']])

# COMMAND ----------

# MAGIC %md
# MAGIC Let's keep only observations from countries which have at least 1000 observations. The following transformations are equivalent:

# COMMAND ----------

# filterin with a list. This is useful only if the list is smallish (in the thousands)
countries_threshold = observations.filter(f.col('n_observations') > 1000).select('cca3').toPandas()['cca3'].tolist()

iot_df_above_threshold = (
  iot_df.filter(f.col('cca3').isin(countries_threshold))
)

# if the list is larger, then an inner join with possible broadcast is better
iot_df_above_threshold = (
  iot_df.join(f.broadcast(observations.filter(f.col('n_observations') > 1000)),
              on=['cca3'],
              how='inner'
  )
)

# or, using a window function to calculate the number of observations and
# filter on that
from pyspark.sql.window import Window
counter_window = Window.partitionBy('cca3')

iot_df_above_threshold = (
  iot_df.withColumn('n_observations', f.count('device_id').over(counter_window))
  .filter(f.col('n_observations') > 1000)
  .drop('n_observations')
)

# COMMAND ----------

iot_df_above_threshold.show()

# COMMAND ----------

# means
iot_df_means = iot_df_above_threshold.groupby('cca3').agg(
  f.mean('humidity').alias('mean_humidity'),
  f.expr('AVG(temp) AS mean_temp'),
  f.mean('c02_level').alias('mean_CO2')
)

# COMMAND ----------

# not much variations in temperature
display(iot_df_means[['cca3', 'mean_temp']])

# COMMAND ----------

display(iot_df_means[['cca3', 'mean_CO2']])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Productionization of the code:

# COMMAND ----------

# MAGIC %md
# MAGIC We can wrap the transformations of the data into functions. This is a first layer of abstraction that greatly enhances reusability and maintenability. Note that the docstrings must be better than those I have here (I am lazy)

# COMMAND ----------

def clean_IOT_data(iot_df: DataFrame) -> DataFrame:
  '''Clean the IOT data
  '''
  # convert unix timestamp to timestamp
  iot_df = iot_df.withColumn('time', 
                             f.to_utc_timestamp(f.from_unixtime(f.col('timestamp')/1000, 'yyy-MM-dd hh:mm:ss'), 'Europe/Amsterdam')
                            )
  iot_df = iot_df.drop(*['cca2', 'cn', 'timestamp', 'scale'])
  iot_df = (iot_df.withColumn('Date', f.to_date('time'))
            .withColumn('Month', f.month('Date'))
            .withColumn('Year', f.year('Date'))
           )
  return iot_df


def calculate_means(iot_df: DataFrame, threshold: int) -> DataFrame:
  '''Filter the dataframe using the threshold and calculate the means
  '''
  countries_threshold = (iot_df.groupby('cca3')
                         .count()
                         .filter(f.col('count') > threshold)
                         .select(f.col('cca3'))
                         .toPandas()['cca3'].tolist()
                        )
  
  iot_df_means = (iot_df.filter(f.col('cca3').isin(countries_threshold))
                  .groupby('cca3').agg(
                    f.mean('humidity').alias('mean_humidity'),
                    f.expr('AVG(temp) AS mean_temp'),
                    f.mean('c02_level').alias('mean_CO2')
                  )
  return iot_df_means

# COMMAND ----------

# MAGIC %md
# MAGIC It can also be useful to wrap transformations (especially when there are multiple complex transformations that belong together logically) into **transformer classes**, which can be used
# MAGIC stand-alone, or with the pipeline API (similar to pandas pipelines) to combine multiple transformations.
# MAGIC 
# MAGIC While transformers are mainly used in the context of machine learning (to build featurization pipelines that lead to model fitting), I find they can be equally useful in a pure ETL context.
# MAGIC Here they can be assembled into stateless aggregation pipelines, or they can be used in functions, or as components of larger classes (using e.g. class composition).
# MAGIC 
# MAGIC Transformers can be custom, or  one can use the transformers from the pyspark API. Most of the pyspark API transformers pertain to machine learning related transformations
# MAGIC (e.g. imputation, normalization, feature creation, ...). However, the SQL transformer can be used to implement arbitrary aggregation logic using SQL.
# MAGIC 
# MAGIC Note that custom transformers cannot be easily saved/loaded (serialized) to disk unless you add some more stuff depending on the internal spark API, see:
# MAGIC - https://stackoverflow.com/questions/41399399/serialize-a-custom-transformer-using-python-to-be-used-within-a-pyspark-ml-pipel
# MAGIC - https://stackoverflow.com/questions/32331848/create-a-custom-transformer-in-pyspark-ml
# MAGIC 
# MAGIC So if the transformers/function/classes/pipelines need to be saved to disk, then using custom transformers requires the above adaptations. An example of when you might want
# MAGIC to serialize a pipeline to disk is when you fit a model pipeline, estimating its parameters, and want to save it to score data. ETL transformations are generally stateless
# MAGIC so this is less of an issue there.
# MAGIC 
# MAGIC Examples of these patterns follow.

# COMMAND ----------

from pyspark.ml import Transformer
from pyspark.sql import DataFrame 

# CUSTOM TRANSFORMER ----------------------------------------------------------------
# Works standalone and in a pipeline but cannot be serialized without extra effort
# useful to combine together transformation logic for code understandability, and to apply
# the same transformations on new data, as these classes can be put into libraries.
class CleanerTransformerCustom(Transformer):
    """
    Cleaner transformer, it drops some columns and does some conversions. (please write better docstrings than me ;))
    """

    def __init__(self):
        super(CleanerTransformerCustom, self).__init__()
        
    def _add_times(self, iot_df: DataFrame) -> DataFrame:
        iot_df = iot_df.withColumn('time', 
                             f.to_utc_timestamp(f.from_unixtime(f.col('timestamp')/1000, 'yyy-MM-dd hh:mm:ss'), 'Europe/Amsterdam')
                            )
        iot_df = (iot_df.withColumn('Date', f.to_date('time'))
            .withColumn('Month', f.month('Date'))
            .withColumn('Year', f.year('Date'))
           )
        return iot_df
      
    def _transform(self, iot_df: DataFrame) -> DataFrame:
      iot_df = self._add_times(iot_df)
      iot_df = iot_df.drop(*['cca2', 'cn', 'timestamp'])
      return iot_df
    
class MeansTransformerCustom(Transformer):
  """Calculate the means
  """
  def __init__(self, threshold: int):
    super(MeansTransformerCustom, self).__init__()
    self.threshold = threshold
    
  def _transform(self, iot_df: DataFrame) -> DataFrame:
      countries_threshold = (iot_df.groupby('cca3')
                         .count()
                         .filter(f.col('count') > self.threshold)
                         .select(f.col('cca3'))
                         .toPandas()['cca3'].tolist()
                        )
      
      iot_df_means = (iot_df.filter(f.col('cca3').isin(countries_threshold))
                  .groupby('cca3').agg(
                    f.mean('humidity').alias('mean_humidity'),
                    f.expr('AVG(temp) AS mean_temp'),
                    f.mean('c02_level').alias('mean_CO2')
                  ))
      return iot_df_means

# COMMAND ----------

# BUILTIN spark SQL transformer. These do the same thing as above, it's largely based on SQL (so more verbose) but can be serialized easily 
#as part of a pipeline.
# Check out the available transformers here: https://spark.apache.org/docs/latest/ml-features#sqltransformer
from pyspark.ml.feature import SQLTransformer

cleaner_transformer = SQLTransformer(
  statement="""SELECT battery_level, c02_level, cca3, device_id, device_name, humidity, ip, latitude, longitude, lcd, scale, temp,
  to_utc_timestamp(from_unixtime(timestamp/1000, 'yyy-MM-dd hh:mm:ss'), 'Europe/Amsterdam') AS time,
  to_date(to_utc_timestamp(from_unixtime(timestamp/1000, 'yyy-MM-dd hh:mm:ss'), 'Europe/Amsterdam')) AS Date,
  month(to_date(to_utc_timestamp(from_unixtime(timestamp/1000, 'yyy-MM-dd hh:mm:ss'), 'Europe/Amsterdam'))) AS Month,
  year(to_utc_timestamp(from_unixtime(timestamp/1000, 'yyy-MM-dd hh:mm:ss'), 'Europe/Amsterdam')) AS Year
  FROM __THIS__
  """
)

def means_transformer(threshold: int):
  return SQLTransformer(
    statement="""SELECT __THIS__.cca3, AVG(humidity) AS mean_humidity,
    AVG(temp) AS mean_temp,
    AVG(c02_level) AS mean_CO2
    FROM __THIS__ INNER JOIN (
    SELECT cca3 FROM (
    SELECT cca3, COUNT(device_id) AS count
    FROM __THIS__
    GROUP BY cca3
    )
    WHERE count > {}
    )
    GROUP BY __THIS__.cca3
    """.format(threshold)
)

# COMMAND ----------

# means using the sql transformer
means_transformer(1000).transform(iot_df).show()

# COMMAND ----------

# means using the custom transformer
means_transformer_custom = MeansTransformerCustom(threshold=1000) # using the custom transformers
means_transformer_custom.transform(iot_df).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Transformers can be chained together to produce pipelines.
# MAGIC The idea here is that we chain together multiple transformations into a larger transformation (we compose transformations).
# MAGIC The last trasformation in the chain can be a trasformer, or an Estimator (which fits a model on data and produces a transformer for prediction).
# MAGIC Here, the second transformer (mean calculation) does not depend on the previous transformer (adding the time parts) as we are
# MAGIC not aggregating on the time parts, but you can understand why chaining transformations is useful in general.

# COMMAND ----------

from pyspark.ml import Pipeline

# we define a pipeline with different stages.
# the stages are our transformers.
pipeline = Pipeline(stages=[CleanerTransformerCustom(), MeansTransformerCustom(threshold=1000)])

# now that we have a pipeline we can use it to transform our data.
# the process is this: first we fit the pipeline to the data. This will pass the data through to each
# transformer, in sequence, and apply the transformations; since there is no estimator step at the end, there
# is no model to be trained. The result is then a PipelineModel object. If we call the transform method
# of this pipeline and pass in the same data, we obtain the transformed data.

pipeline.fit(iot_df).transform(iot_df).show()