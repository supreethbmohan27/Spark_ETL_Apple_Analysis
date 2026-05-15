# Databricks notebook source
# MAGIC %run "/Users/supreethbmohan@icloud.com/reader_factory"
# MAGIC

# COMMAND ----------

# MAGIC %run "/AppleAnalysis/transform"
# MAGIC

# COMMAND ----------

# MAGIC %run "/AppleAnalysis/extractor"

# COMMAND ----------

# MAGIC %run "/AppleAnalysis/loader"

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("appleanalysisproject").getOrCreate()
# input_df = spark.read.format("csv").option("header","true").load("/Workspace/default/transaction_updated")
input_df = spark.table("transaction_updated")
input_df.show()

# COMMAND ----------

dbutils.fs.ls("/")

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("appleanalysisproject").getOrCreate()
input_df = spark.read.format("csv").option("header","true").load("/Volumes/workspace/appledata/appleanalysis/Transaction_Updated.csv")
input_df.show()

# COMMAND ----------

# DBTITLE 1,Cell 5
class WorkFlow:
    def __init__(self):
        pass

    def runner(self):
        transactionInputDF = get_data_source(
            data_type = 'csv',
            file_path = '/Volumes/workspace/appledata/appleanalysis/Transaction_Updated.csv'
        ).get_data_frame()

        transactionInputDF.orderBy("customer_id","transaction_date").show()

        
        customerInputDF = spark.table("default.customer_delta_table")
        # customerInputDF.show()

        inputDFs = {
            "transactionInputDF": transactionInputDF,
            "customerInputDF": customerInputDF
        }
        firstTransform = FirstTransformer().transform(inputDFs) 



workFlow = WorkFlow().runner()

# COMMAND ----------

# DBTITLE 1,Cell 7
file_location = '/Volumes/Workspace/appledata/appleanalysis/Customer_Updated.csv'
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header)\
  .option("sep", delimiter) \
  .load(file_location)
temp_table_name = 'customer_delta_table'
df.createOrReplaceTempView(temp_table_name)



# COMMAND ----------




# -- # df.write.format('delta').saveAsTable('Cusotmer_Delta_Table')

# -- df.write.format("delta") \
# --     .mode("overwrite") \
# --     .save("/Volumes/Workspace/appledata/appleanalysis/Customer_Delta_Table")
df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("default.customer_delta_table")


# COMMAND ----------

class WorkFlow:
    def __init__(self):
        pass

    def runner(self):
        #Step 1: Extract all the required data from different source
        inputDFs = AirpodsAfterIphoneExtractor().extractor()

        #step 2: Implement the Transformation 
        firstTransformedDF = FirstTransformer().transform(inputDFs)

        #step3: Implement the Loading in all different sink
        # AirpodsAfterIphoneLoader(firstTransformedDF).sink()
        loader = AirpodsAfterIphoneLoader(firstTransformedDF)
        print("Loader output")
        loader.transformedDF.show()
        loader.sink()
workflow = WorkFlow().runner()

# COMMAND ----------

class SecondWorkFlow:
    
    """
    ETL pipeline to generate the data for all the customers who have bought Airpods just after buing iPhone
    """

    def __init__(self): 
        pass

    def runner(self):
        #Step 1: Extract all the required data from different source
        inputDFs = AirpodsAfterIphoneExtractor().extractor()

        #step 2: Implement the Transformation 
        OnlyAirpodsAndIphoneDF = OnlyAirpodsAndIphone().transform(inputDFs)

        #step3: Implement the Loading in all different sink
        # AirpodsAfterIphoneLoader(firstTransformedDF).sink()
        loader = OnlyAirpodsAndIphoneLoader(OnlyAirpodsAndIphoneDF)
        # print("Loader output")
        # loader.transformedDF.show()
        loader.sink()


# secondworkflow = SecondWorkFlow().runner()

# COMMAND ----------

class WorkFlowRunner:
    def __init__(self, name):
        self.name = name

    def runner(self):
        if self.name == "workflow":
            return WorkFlow().runner()
        elif self.name == "secondworkflow":
            return SecondWorkFlow().runner()
        else:
            return "Invalid workflow"

name = "secondworkflow"
workflowRunner = WorkFlowRunner(name).runner()


# COMMAND ----------

dbutils.fs.rm(
    "/Volumes/workspace/appledata/appleanalysis/output/onlyairpodsandiphone",
    True
)