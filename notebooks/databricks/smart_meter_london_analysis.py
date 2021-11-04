# Databricks notebook source
# MAGIC %md
# MAGIC # Smart meter London data: an analysis
# MAGIC 
# MAGIC In this notebook we are going to:
# MAGIC 
# MAGIC - read a largish dataset (>10GB) of smart meter sensor readings, provided by the city of London, from an azure data lake;
# MAGIC - clean the data and create aggregations of the dataset on coarser time periods, and save them back to the data lake for downstream consumption;
# MAGIC - explore these aggregations to get a feel on what we can model with the data;
# MAGIC - use the created aggregations to construct a dataset that can be used to fit a household energy consumption forecasting model
# MAGIC 
# MAGIC We will use this dataset with the main aim of exploring further the spark API, and discussing more advanced aggregation tools (e.g. pandas UDF, complex types)
# MAGIC 
# MAGIC The smart meter sensor data can be downloaded from here:
# MAGIC 
# MAGIC - https://data.london.gov.uk/dataset/smartmeter-energy-use-data-in-london-households
# MAGIC 
# MAGIC Read also the data description on the page.
# MAGIC 
# MAGIC Please download locally the version named: low-carbon-london-data-168-files. This is a version where the dataset is split into 168 csvs, each containing
# MAGIC around 1 million rows. After you unzip the file, you will have a folder called Power-Networks-LCL-June2015(withAcornGps).csv_Pieces
# MAGIC with the data.
# MAGIC 
# MAGIC We now need to upload the data to an azure data lake. Please follow the steps outlined in the slides (in the appendix) to create your azure data lake, within
# MAGIC the resource group where databricks lives, and to upload the data.
# MAGIC 
# MAGIC After you have uploaded the data, we need to make it accessible from databricks to operate on it. Please follow the steps outlined in the slides (in the appendix) to make your data
# MAGIC lake accessible from within your cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC # Read the raw data

# COMMAND ----------

# connecting to the azure data lake. Here scope and key need to be replaced with the scope and keys you created when you setup the
# databricks secrets
spark.conf.set("fs.azure.account.key.adsliot.dfs.core.windows.net", dbutils.secrets.get(scope="dataconnections", key="connection"))
# list the files in the data lake
dbutils.fs.ls("abfss://iotdata@adsliot.dfs.core.windows.net")

# COMMAND ----------

# set the number of shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", 80)

# COMMAND ----------

import os
import pandas as pd
BASE_FOLDER = "abfss://iotdata@adsliot.dfs.core.windows.net"
RAW_FOLDER = os.path.join(BASE_FOLDER, "Raw")
CLEAN_FOLDER = os.path.join(BASE_FOLDER, "Clean")
display(dbutils.fs.ls(RAW_FOLDER))

# COMMAND ----------

from pyspark.sql import functions as f
from pyspark.sql.types import StructField, StructType, TimestampType, StringType, DoubleType

# COMMAND ----------

# we read the data in parallel into a dataframe.
# the following is a lazy transformation (and not an action)
schema_raw = StructType([
  StructField('LCLid', StringType(), True),
  StructField('stdorTOU', StringType(), True),
  StructField('DateTime', TimestampType(), True),
  StructField('KWH_half_hour', DoubleType(), True),
  StructField('Acorn', StringType(), True),
  StructField('Acorn_grouped', StringType(), True),
])

raw_df = spark.read.schema(schema_raw).option("header", True).csv(RAW_FOLDER)

# COMMAND ----------

# this is an action and it triggers the read
display(raw_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Data checks, complex types, and udfs

# COMMAND ----------

# let's split the meter reading from the household information and revert to a star schema (fact vs dimensions)
household_dims = ['LCLid', 'stdorToU', 'Acorn', 'Acorn_grouped']
household_dims_df = raw_df.selectExpr(*household_dims, "DateTime").cache()
consumption_df = (
  raw_df.select(raw_df.columns[0:1] + raw_df.columns[2:4])
)
display(household_dims_df)

# COMMAND ----------

# let's check the acorn groups
# DISTINCT is a rather expensive operation, and in this case it would be faster on a single node
# also here databricks can start resizing the cluster (acquiring more nodes), 
# which can take quite some time for interactive work (but it's good for automated pipelines)
household_dims_df.select(['Acorn', 'Acorn_grouped']).distinct().show()

# COMMAND ----------

household_dims_df.groupby(['Acorn', 'Acorn_grouped']).count().show()

# COMMAND ----------

(household_dims_df.groupby(['Acorn', 'Acorn_grouped']).count()).explain()

# COMMAND ----------

household_dims_df.select(['Acorn', 'Acorn_grouped']).distinct().explain()

# COMMAND ----------

household_dims_df.groupby('Acorn').count().show()

# COMMAND ----------

household_dims_df.select('Acorn_grouped').distinct().count()

# COMMAND ----------

# MAGIC %md
# MAGIC 19 ACORN groups; the caci website (https://acorn.caci.co.uk/what-is-acorn) reports 18 groups, however ACORN- here most likely represent an NA (no acorn recorded).
# MAGIC 5 ACORN_grouped, which should correspond to the CACI ACORN categories (currently 6). Let's create a feature with just the letter code of the acorn group.

# COMMAND ----------

household_dims_df = (household_dims_df.select(f.expr('*'), f.split(f.col('Acorn'), '-').alias('Acorn_splitted'))
.selectExpr('*', 'Acorn_splitted[1] AS Acorn_letter')
.select(f.expr('*'), f.size(f.col('Acorn_splitted')).alias('size'))
).cache() # caching the dataframe to memory

display(household_dims_df) # triggering the computation

# note that this doesn't work:
# (household_dims_df.select(f.expr('*'), f.split(f.col('Acorn'), '-').alias('Acorn_splitted'), 'Acorn_splitted[1]')
#).show()

# COMMAND ----------

# the null values
household_dims_df.filter(f.col('Acorn_letter') == '').show()

# COMMAND ----------

# MAGIC %md
# MAGIC It would be better to have NA as the acorn_letter rather than the empty '' representing NA.
# MAGIC We can do this in two ways. Using custom udfs or using builtin pyspark operators.
# MAGIC 
# MAGIC Here, **udf** stands for **user defined function**. Udfs are a way for the programmer to specify
# MAGIC custom (i.e., not composed of spark API commands) computations in native python over spark dataframes.
# MAGIC In pyspark, we can broadly distinguish three classes of udfs:
# MAGIC 
# MAGIC - python udfs (row by row udfs)
# MAGIC - pandas udfs (i.e., vectorized udfs)
# MAGIC - pandas UDAF (user defined aggregate functions), which operate on grouped dataframes
# MAGIC 
# MAGIC The last two types have various different subtypes, depending on the types of the input columns and the output type.
# MAGIC See the slides for an overview of these udfs.
# MAGIC Let's do the NA replacement using udfs, pandas vectorized udfs, and native code.

# COMMAND ----------

# replace null acorn with NA - PLAIN UDF

# note: we are using type hints to specify the output type here
# in Spark < 3.0.0, one does @udf(returnType=StringType())
@udf
def replace_empty_str_udf(val: StringType()) -> StringType():
  if val == '':
    return None
  else:
    return val

# or, more succintly
#replace_empty_str = f.udf(lambda val: None if val == '' else val, StringType())

(household_dims_df.withColumn('Acorn_letter', replace_empty_str_udf('Acorn_letter')).filter(f.col('Acorn_letter').isNull()).show())

# COMMAND ----------

# replace null acorn with NA - PANDAS UDF

import pandas as pd # we need pandas and pyarrow
from pyspark.sql.functions import pandas_udf

# define a series to series pandas udf

@pandas_udf(StringType()) # return type of the series
def replace_empty_str_pudf(s: pd.Series) -> pd.Series: # using type hints to specify input/output shape
  s[s == ''] = None
  return s

# NOTE: for pyspark < 3.0.0 one needed to specify the function type
# this will be deprecated
#from pyspark.sql.functions import PandasUDFType
#from pyspark.sql.types import IntegerType
#@pandas_udf(StringType(), PandasUDFType.SCALAR)
#def replace_empty_str(s: pd.Series) -> pd.Series: # using type hints to specify input/output shape
#  s[s == ''] = None
#  return s

# check that the function works for a normal pandas df
(
  household_dims_df.withColumn('Acorn_letter', replace_empty_str_pudf('Acorn_letter'))
  .filter(f.col('Acorn_letter').isNull())
  .show()
)

# COMMAND ----------

# MAGIC %md
# MAGIC Generally speaking, **pandas UDF** will be faster than **normal UDF**, since the operation is vectorized.
# MAGIC What's happening under the hood is that spark splits the input column into multiple chunks, then converts
# MAGIC the columns to a pandas series using **apache arrow**, then applies the function, and concatenates the output back.
# MAGIC Apache Arrow is an in-memory columnar data format that is used in Spark to efficiently transfer data between JVM 
# MAGIC and Python processes, so it is important that **pyarrow** is installed to be able to pass data from the JVM to pandas
# MAGIC and viceversa.
# MAGIC **NOTE**, however, that pandas udfs **require the whole data to be loaded in memory on the executor node**. In the case above, for instance,
# MAGIC the column is split into multiple chunks and these are **loaded in memory** locally. This can cause issues if the data does not fit on the executor
# MAGIC memory (read: crashes).
# MAGIC 
# MAGIC Pyspark offers a variety of different pandas udfs, which are distinguished by their input/output types; see https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.functions.pandas_udf.
# MAGIC For instance, we can write a udf that takes an iterator of series and returns a series:

# COMMAND ----------

# MAGIC %md
# MAGIC The difference with the previous pandas udf is that this udf take as input a **batch** of pd.Series, and it returns a batch of pd.Series. This is particularly useful if some state needs to be initialized that the computation depends on (e.g. loading a model):

# COMMAND ----------

@pandas_udf(StringType())
def calculate(iterator: Iterator[pd.Series]) -> Iterator[pd.Series]:
    # initialize the state
    state = 1
    for x in iterator:
      yield x + '__' + str(state)
      # update the state
      state = state + 1

tracked_df = (household_dims_df.withColumn('tracker', f.lit('T'))
 .select(calculate(f.col('tracker')))
)

tracked_df.head(10)

# COMMAND ----------

tracked_df.tail(10)

# COMMAND ----------

# MAGIC %md
# MAGIC This is particularly useful if e.g. we need to score the column against an ML model, as we can load the model in the udf and then score each batch. The previous udf would have required us to reload the model every time the function was called.
# MAGIC Nevertheless, if there is a solution to the problem that uses the basic spark API, and doesn't rely on udfs/pandas udfs, that will be faster. E.g., for our problem of replacing empty strings with NA, we can use .when to perform conditional replacement. The following is orders of magnitudes faster:

# COMMAND ----------

household_dims_df = household_dims_df.withColumn('Acorn_letter', f.when(f.col('Acorn_letter') == '', None).otherwise(f.col('Acorn_letter')))

# COMMAND ----------

# MAGIC %md
# MAGIC So, to summarise, the order in which you should try things is:
# MAGIC built-in API > pandas UDF > UDF. If you are in a situation where you cannot express the computation using the API, but the pandas UDF are too slow, recurring to scala udf might be a solution (as these are faster than python/pandas udfs).

# COMMAND ----------

household_dims_df = household_dims_df.drop(*['Acorn_splitted', 'Acorn', 'size'])
household_dims_df.show()

# COMMAND ----------

household_dims_df

(household_dims_df.groupby(['LCLid'])
.agg(
  (f.countDistinct('stdorToU') + f.countDistinct('Acorn_grouped') + f.countDistinct('Acorn_letter')).alias('dist')
).filter(f.col('dist') > 3)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Since there are no changes in status, for this dataset we can just store the distinct combinations.

# COMMAND ----------

# coalesce the data to ten partitions and save to disk
(household_dims_df.drop_duplicates(['LCLid', 'stdorToU', 'Acorn_grouped', 'Acorn_letter'])
 .coalesce(10)
 .write.mode('overwrite').parquet(os.path.join(CLEAN_FOLDER, 'household_metrics.parquet'))
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Exercise :)**
# MAGIC On kaggle, you can find the detailed breakdown of the metrics defining the acorn groups for this dataset:
# MAGIC 
# MAGIC - https://www.kaggle.com/jeanmidev/smart-meters-in-london/data (dataset name: acorn_details.csv)
# MAGIC 
# MAGIC Download this csv, upload it to the data lake, and join the POPULATION, FAMILY and ECONOMY metrics with the household_dims_df
# MAGIC computed above. (hint: pyspark has a pivot function).

# COMMAND ----------

# MAGIC %md
# MAGIC # Aggregations and explorations

# COMMAND ----------

# MAGIC %md
# MAGIC Let's now perform some **aggregations** and **explorations** on the energy consumption dataset. The goal is to:
# MAGIC 
# MAGIC - Identify transformations/aggregations of the data that might be useful for scheduled ETL processes (for the data to be used downstream)
# MAGIC - Explore the data to see whether we can use it to produce energy consumption forecasts, and if yes, write code to prepare the data to fit a model on
# MAGIC 
# MAGIC We can start by calculating energy consumption at daily and monthly levels.
# MAGIC We can do that by doing separate aggregations and save the datasets separately. Alternatively, we can do roll up aggregations, where we calculate hiearchical subtotals
# MAGIC (from left to right).

# COMMAND ----------

consumption_df = (consumption_df.selectExpr('to_date(DateTime) AS Date', '*')
                 .selectExpr('year(date) AS Year', 'month(Date) as Month', '*')
                 )

# COMMAND ----------

consumption_rollup_df = (consumption_df.rollup(['Year', 'Month', 'Date', 'LCLid']) # pay attention to the ordering of cols, as aggregations goes left-to-right
                      .agg(f.sum('KWH_half_hour').alias('KWH'),
                          f.countDistinct('LCLid').alias('n_households')
                          )
                      .orderBy(['Year', 'Month', 'Date', 'LCLid'])
).cache()

# COMMAND ----------

# triggering the computation
display(consumption_rollup_df)

# COMMAND ----------

# we cached the object, so this will be instantaneous:
display(consumption_rollup_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Alternatively, we might be interested in calculating the aggregated function on *all possible combinations* for a set of columns (rather than just follow a left-to-right hierarchy).
# MAGIC We can do that with the *cube* operator:

# COMMAND ----------

# The following will have e.g. rows for the total consumption of a household ID (over the whole time span)
cube_consumption = (
  consumption_df.cube(['Year', 'Month', 'Date', 'LCLid'])
  .agg(f.sum('KWH_half_hour').alias('KWH'))
)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's take a look at some plots for this data. Note that the following will run only if you have plotnine installed on the cluster. To install python packages on the cluster, navigate to workspace/your_user_name and from the dropdown menu (the downward facing arrow next to your username) select create > library. Then choose PyPy and put 'plotnine' in the package name box. Then select your cluster from the menu and click install.

# COMMAND ----------

# REMEMBER TO IMPORT PANDAS otherwise you will get very weird and nondesscript errors
dates_consumption_pdf = (consumption_rollup_df.filter(f.col('LCLid').isNull())
 .filter(f.col('Date').isNotNull())
 .select(f.col('Date'), f.col('KWH'), f.col('n_households'))).toPandas()

# COMMAND ----------

dates_consumption_pdf

# COMMAND ----------

from plotnine import ggplot, aes, geom_point, scale_x_date, theme, element_text, xlab, ylab

# warning: next function uses eval(). TREAT WITH CARE! E.g., you should never do
# something like eval(input()), since the user my give as input something like os.system('rm -rf *') (oops!)

def plot_metrics_dates(data, y, *args, breaks='3 month'):
  additional_coms = ' + \n'.join(args)
  plot = '''ggplot({}, aes(x='Date', y='{}')) +
          geom_point() +
          scale_x_date(date_breaks='{}') +
          theme(axis_text_x=element_text(rotation=90, hjust=1))'''.format(data, y, breaks)
  
  if len(additional_coms) > 0:
    plot = plot + ' +\n {}'.format(additional_coms)
    
  plot = '(' + plot + ')'
  return eval(plot)

# COMMAND ----------

plot_metrics_dates('dates_consumption_pdf', 'n_households', "ylab('N Households')")

# COMMAND ----------

plot_metrics_dates('dates_consumption_pdf', 'KWH')

# COMMAND ----------

# MAGIC %md
# MAGIC Let's take a random sample of dates across the different years and plot the cross-section across all houses

# COMMAND ----------

household_daily_consumption = (consumption_rollup_df.filter(f.col('LCLid').isNotNull()))

# COMMAND ----------

# MAGIC %md
# MAGIC Let's first try with a Pandas UDAF, so we also discuss those.
# MAGIC As we saw above, we can use Pandas UDF, which are vectorized UDFs, to perform computations on dataframe columns. But what if we want to perform some computation *by group*, i.e., on a pyspark.sql.group.GroupedData object? Well, before Spark 3.0.0 Spark offered a different type of vectorized pandas UDF: the PandasUDFType.GROUPED_MAP. This Pandas udf could then be passed to the *apply* method of the grouped object, and the function would be applied to the group (which is passed to the function as a pandas dataframe), like so:

# COMMAND ----------

from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import IntegerType, DateType

return_schema = StructType([StructField('Year', IntegerType(), True), StructField('Date', DateType(), True)]) # This is the schema defining the return dataframe

@pandas_udf(return_schema, PandasUDFType.GROUPED_MAP)
def sample_dates_udaf(pdf):
  pdf = pdf.drop_duplicates()
  pdf = pdf.sample(n=3, random_state=1234)
  return pdf

# COMMAND ----------

# calculate the sample
sampled_dates = household_daily_consumption.selectExpr('Year', 'Date').groupby(['Year']).apply(sample_dates_udaf)
sampled_dates_pd = sampled_dates.toPandas()['Date'].tolist()
sampled_dates_pd

# COMMAND ----------

# MAGIC  %md
# MAGIC Since spark 3.0.0, there is a better way: just define a native python function, and pass it to the applyInPandas method of the grouped object:

# COMMAND ----------

def sample_dates_udaf(pdf):
  pdf = pdf.drop_duplicates()
  pdf = pdf.sample(n=3, random_state=1234)
  return pdf

household_daily_consumption.selectExpr('Year', 'Date').groupby(['Year']).applyInPandas(
  sample_dates_udaf, schema=return_schema
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Note also that you can pass the grouping key to the pandas udaf, alongside the pandas dataframe. This can be useful if you have a pandas udaf that can operate on different dataframes, grouped by different keys, and you need to do some computation with those keys. The keys are passed to the pandas udf as a tuple of numpy types:

# COMMAND ----------

return_schema_keys = StructType([StructField('Year', IntegerType(), True), StructField('Date', DateType(), True),
                           StructField('keys', StringType(), True)]) # This is the schema defining the return dataframe

def sample_dates_udaf_keys(keys, pdf):
  pdf = pdf.drop_duplicates()
  pdf = pdf.sample(n=3, random_state=1234)
  pdf['keys'] = str(keys)
  return pdf

household_daily_consumption.selectExpr('Year', 'Date').groupby(['Year']).applyInPandas(
  sample_dates_udaf_keys, schema=return_schema_keys
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC As nice as pandas udaf are, keep in mind that spark has to load the entire group in memory on the executor nodes. That means that if your groups are highly skewed (i.e., one group contains a high percentage of the rows) and you apply the pandas udaf, then the rows are shuffled across the executor nodes and then loaded into memory. Thus, the large group might be larger than the executor memory, which can crash the application. This is harder to control here than for the previous pandas udf, as those do not depend on grouping (the series are just chunked independently) and moreover for those you can always resort to the iterator udfs. So keep this in mind.

# COMMAND ----------

# MAGIC %md
# MAGIC As usual, however, it would be best to use the built-in methods. Like so:

# COMMAND ----------

from pyspark.sql.window import Window

household_daily_consumption_dates = household_daily_consumption.select(['Year', 'Date']).distinct()
window_sampling = Window.partitionBy(household_daily_consumption_dates['Year']).orderBy(f.rand(seed=1234)) 
#f.rand() Generates a random column with independent and identically distributed (i.i.d.) samples uniformly distributed in [0.0, 1.0). We can use that to randomly rank the
#date column and then pick the top three dates

(household_daily_consumption_dates.select('Year','Date', f.rank().over(window_sampling).alias('rank')).filter(f.col('rank') <= 3).drop('rank')
).show()

# COMMAND ----------

sampled_dates_pdf = (household_daily_consumption.filter(f.col('Date').isin(sampled_dates_pd))
.selectExpr('Date', 'LCLid', 'KWH')
.toPandas())

# COMMAND ----------

sampled_dates_pdf

# COMMAND ----------

from plotnine import geom_histogram

(ggplot(sampled_dates_pdf, aes(x='KWH')) +
 geom_histogram(bins=200)
)

# COMMAND ----------

# MAGIC %md
# MAGIC The distribution looks rather right skewed. But it could be fixable with a Box-Cox transform, e.g. a log

# COMMAND ----------

import numpy as np
sampled_dates_pdf['log_KWH'] = np.log(sampled_dates_pdf['KWH'])

(ggplot(sampled_dates_pdf, aes(x='log_KWH')) +
 geom_histogram(bins=200)
)

# COMMAND ----------

# MAGIC %md
# MAGIC Looks rather normal, with however some outliers on the far left of the distribution.

# COMMAND ----------

sampled_dates_pdf[sampled_dates_pdf['log_KWH'] < -1]

# COMMAND ----------

# MAGIC %md
# MAGIC there are some values with zero, which send the log to infinity, and some other values that are quite small. Moreover, we need to keep in mind that the timeseries for a household could have gaps for some days, e.g. because the household had very low energy consumption for that day and so the consumption wasn't recorded. When modelling this kind of data, one needs to make a decision on how to deal with such issues, and it usually depends on what you are trying to model and on the methods you want to employ. 
# MAGIC 
# MAGIC For instance, if the goal is to forecast the energy consumption of the city of London 1 day ahead with a statistical time series model (e.g. arima), one might want to impute the zero values/gaps with very small values and forecast e.g. the mean of the log transforms of the data and its standard deviation, so as to keep the daily distributions normal. This would allow the calculation of a projected future normal distribution, from which the toal energy consumption of the city can be estimated (note that this is likely to yield worse results than if we had the timeseries for the total consumption of the city available). For this, one might also want to exlude the period until 2012-10-01, as after that date the number of households in the sample remains more or less constant, which would give more stability to the forecasts.
# MAGIC 
# MAGIC Alternatively, if the goal is to provide energy usage forecasts to the households in the program, we could train a machine learning model (e.g. a random forest) to forecast the energy consumption of a household, encoded as a vector of features (acorn group, consumption of the previous 7 days, average consumption of the previous 7 days, and so forth). So the model would look like this:
# MAGIC 
# MAGIC $$EC_{T+1}\mid EC_{T}, \text{acorn}, mean(EC(T),..., EC(T-7)), \dots$$
# MAGIC 
# MAGIC We could then use the model to forecast the energy usage of a household 1 day ahead, provided that we have the necessary covariates on past energy usage.

# COMMAND ----------

# let's check the daily percentage of small values for KWH
display(household_daily_consumption.withColumn('is_small', f.when(f.col('KWH') <= 0.5, 1).otherwise(0))
 .groupby('Date')
 .agg(
   (f.sum('is_small')/f.count('*')).alias('perc_small')
 )
)

# COMMAND ----------

# MAGIC %md
# MAGIC There are some dates where the percentage of small KWH household is higher than usual. The last data point is clearly an outlier, as we saw above, and needs to be excluded. The other blips could be investigated further, but they also look like outliers, which might need to be imputed or excluded.
# MAGIC Let's now take a quick look at the possible gaps in the time series:

# COMMAND ----------

from pyspark.sql.window import Window

window_date_diff =  Window.partitionBy('LCLid').orderBy('Date')

(household_daily_consumption.withColumn('date_diff', 
                                        f.datediff(f.lead(f.col('Date'), 1).over(window_date_diff), f.col('Date'))
                                       )
 .filter(f.col('date_diff') > 1)
 .orderBy('date_diff', ascending=False)
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC As you can see, some time series have large gaps, while others have just a few days gaps.

# COMMAND ----------

mac_df = household_daily_consumption.filter(f.col('LCLid').isin(['MAC005561', 'MAC001381', 'MAC001763', 'MAC003345'])).toPandas()

# COMMAND ----------

from plotnine import facet_wrap
display(plot_metrics_dates('mac_df', 'KWH', "facet_wrap('LCLid')").draw())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC We have now explored quite a bit this dataset. We can then save to the clean folder:
# MAGIC 
# MAGIC - the aggregated consumption, which can be used later downstream for futher analysis
# MAGIC - a processed version of the data that can be used to fit a model for a 1 day ahead forecast of household energy consumption

# COMMAND ----------

consumption_rollup_df.write.mode('overwrite').parquet(os.path.join(CLEAN_FOLDER, 'rollup_agg.parquet'))

# COMMAND ----------

# MAGIC %md
# MAGIC Let's also save a daily aggregate of consumptions, and shard it by day. That can be more useful when we have ETL jobs running that aggregate new data that comes in daily, and need to keep up to date various datasets, as we can compute our aggregations only on the new data.

# COMMAND ----------

# this will take some time as it needs to repartition the data
(consumption_df.groupby(['Date', 'LCLid'])
.agg(f.sum('KWH_half_hour').alias('KWH'))
.repartition('Date') # move all the data with the same date to the same partition
.write.partitionBy('Date').mode('overwrite').parquet(os.path.join(CLEAN_FOLDER, 'daily_aggregate'))
)

# Note that if you run the above with new data that has come in, then the whole hierarchy will be overwritten.
# if you just want to update the hierarchy with the partitions for the new data, set spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
# before running the above

# COMMAND ----------

# MAGIC %md
# MAGIC Let's now use the insights from above to compute a dataset that we can use to fit a forecasting model that would predict, for a given household at a given date, its 1 day ahead forecast.
# MAGIC Let's do this as an excercise:

# COMMAND ----------

# MAGIC %md
# MAGIC **Exercise**
# MAGIC Write a preprocessing pipeline (using functions or transformers) that:
# MAGIC 
# MAGIC - Joins the household_daily_consumption data back with the household_features data using broadcasting
# MAGIC - Impute the zero values for KWH with the average KWH for the household acorn group, for that day
# MAGIC - Impute the null values for KWH with the average KWH for the household acorn group, for that day
# MAGIC - filters out data before 2012-07-01
# MAGIC - filters out the last date in the dataset
# MAGIC - for every household, imputes the gaps in the time series using the 'last value carried forward' method, or a 7-day moving average
# MAGIC 
# MAGIC The last part of the exercise is the most laborious one. Remember that you can use the window functions (see above) and the pandas udf to achieve your goal.
# MAGIC Also:
# MAGIC - hint (1): split the problem into two, first add to the df the missing dates with Null KWH, and then do the forward fill
# MAGIC - hint (2): this blogpost: https://johnpaton.net/posts/forward-fill-spark/ has a nice solution to the forward-fill problem. **PLEASE try yourself for a bit before looking at it**.

# COMMAND ----------

# MAGIC %md
# MAGIC **Exercise**
# MAGIC 
# MAGIC Extend the previous preprocessing pipeline to calculate, for every household, for every row (representing a measurement of the household at date T):
# MAGIC 
# MAGIC - the value of the last 7 days of KWH
# MAGIC - the mean energy consumed in the last 3 days by the household (including the present row)
# MAGIC - the mean energy consumed in the last 7 days by the household (including the present row)
# MAGIC - the value of the energy consumed at time T+1 (this will be our forecasting target)
# MAGIC 
# MAGIC So, your dataset should end up looking like this:
# MAGIC 
# MAGIC Date | LCLid | Acorn_letter | Acorn_group | ... | KWH | KWH_-1 | KWH_-2 | KWH_-3 | ... | KWH_mean_3_days | KWH_mean_7_days

# COMMAND ----------

# MAGIC %md
# MAGIC **Exercise**
# MAGIC Go back to https://www.kaggle.com/jeanmidev/smart-meters-in-london and:
# MAGIC - download the weather data (weahter_daily_darksky)
# MAGIC - upload the weather data to the azure data lake in the Raw folder
# MAGIC - read the data into databricks and join it (using broadcasting) with the dataset you obtained from the previous exercise

# COMMAND ----------

# MAGIC %md
# MAGIC You're done! In the next session we will look at the dataset we have constructed and fit a forecasting model.
