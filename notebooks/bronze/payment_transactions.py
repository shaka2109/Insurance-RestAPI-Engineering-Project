# Databricks notebook source
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType, TimestampType 

myschema = StructType([
                        StructField('Customer_id', LongType(), True),
                        StructField('policy_id', StringType(), True),
                        StructField('payment_date', TimestampType(), True),
                        StructField('payment_amount', DoubleType(), True),
                        StructField('payment_frequency', StringType(), True),
                        StructField('payment_mode', StringType(), True),
                        StructField('payment_status', StringType(), True),
                        StructField('transaction_id', StringType(), True)])

# COMMAND ----------

payments_df = (spark.read.format('csv')
                    .option('header', False)
                    .schema(myschema)
                    .load('/Volumes/insureallbi/file/payments/Payments.csv'))


# COMMAND ----------

payments_df.count()

# COMMAND ----------

column_names = [
    "customer_id",
    "policy_id",
    "payment_date",
    "payment_amount",
    "payment_frequency",
    "payment_mode",
    "payment_status",
    "transaction_id"
]

payments_df = payments_df.toDF(*column_names)

# COMMAND ----------

(payments_df.write.format('delta')
                    .mode('overwrite')
                    .saveAsTable('insureallbi.bronze.payment_transactions'))

# COMMAND ----------

# %sql
# SELECT * 
# FROM insureallbi.bronze.payment_transactions
# LIMIT 10
