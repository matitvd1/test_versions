# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ####Calendar Dimension####

# COMMAND ----------

# Read Buy Box
df_buy_box = \
    spark.table("raw.cxh_buy_box_summary")\
        .withColumn("Date", to_date(col("Date"), "MM/dd/yyyy HH:mm:ss"))\
        .filter(col('Date') >= '2022-11-01')

# COMMAND ----------

date_dimension = \
    df_buy_box\
        .select('Date')\
        .distinct()

date_dimension.createOrReplaceTempView('date_table')

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   year(Date) * 10000 + month(Date) * 100 + day(Date) as dateInt,
# MAGIC   Date,
# MAGIC   year(Date) AS CalendarYear,
# MAGIC   date_format(Date, 'MMMM') as CalendarMonth,
# MAGIC   month(Date) as MonthOfYear,
# MAGIC   date_format(Date, 'EEEE') as CalendarDay,
# MAGIC   dayofweek(Date) AS DayOfWeek,
# MAGIC   weekday(Date) + 1 as DayOfWeekStartMonday,
# MAGIC   case
# MAGIC     when weekday(Date) < 5 then 'Y'
# MAGIC     else 'N'
# MAGIC   end as IsWeekDay,
# MAGIC   dayofmonth(Date) as DayOfMonth,
# MAGIC   case
# MAGIC     when Date = last_day(Date) then 'Y'
# MAGIC     else 'N'
# MAGIC   end as IsLastDayOfMonth,
# MAGIC   dayofyear(Date) as DayOfYear,
# MAGIC   weekofyear(Date) as WeekOfYearIso,
# MAGIC   quarter(Date) as QuarterOfYear,
# MAGIC   case
# MAGIC     when month(Date) >= 7 then year(Date) + 1
# MAGIC     else year(Date)
# MAGIC   end as FiscalYearJulToJun,
# MAGIC   (month(Date) + 5) % 12 + 1 AS FiscalMonthJulToJun,
# MAGIC   case
# MAGIC     when (month(Date) + 5) % 12 + 1 between 1 and 3 then "Fiscal Qtr 1"
# MAGIC     when (month(Date) + 5) % 12 + 1 between 4 and 6 then "Fiscal Qtr 2"
# MAGIC     when (month(Date) + 5) % 12 + 1 between 7 and 9 then "Fiscal Qtr 3"
# MAGIC     when (month(Date) + 5) % 12 + 1 between 10 and 12 then "Fiscal Qtr 4"
# MAGIC   end as FiscalQtr
# MAGIC from
# MAGIC   date_table
# MAGIC order by
# MAGIC   Date;

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

adafaww q