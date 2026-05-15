# Databricks notebook source
from pyspark.sql.window import Window
from pyspark.sql.functions import lead,col, broadcast, collect_set, array_contains, size

class Transformer:
    def __init__(self):
        pass
    def transform(self, input):
        pass


class FirstTransformer(Transformer):
    def transform(self, inputDFs):

        """
        Customers who have bought Airpods after buying the iphone
        """

        transactionInputDF = inputDFs.get("transactionInputDF")
        print("transactionInputDF in transform")
        transactionInputDF.show()

        windowSpec = Window.partitionBy("customer_id").orderBy("transaction_date")
        transformedDF = transactionInputDF.withColumn("next_product", lead("product_name", 1).over(windowSpec))
        
        print("Airpods after buying Iphone")
        # transformedDF.orderBy("customer_id","transaction_date","product_name").show()
        
        try:
            transformedDF.orderBy(
                "customer_id",
                "transaction_date",
                "product_name"
            ).show()
        except Exception as e:
            print(e)
        
        
        filteredDF = transformedDF.filter((col("product_name") == 'iPhone') & (col("next_product")== "AirPods"))
        
        print("next_product")
        filteredDF.orderBy("customer_id","transaction_date","product_name").show()

        customerInputDF = inputDFs.get("customerInputDF")
        customerInputDF.show()
        
        
        filteredDF = filteredDF.withColumn(
            "customer_id",
            col("customer_id").cast("int")
        )

        print("CUSTOMER DF SCHEMA")
        customerInputDF.printSchema()

        print("FILTERED DF SCHEMA")
        filteredDF.printSchema()

        joinDF = customerInputDF.join( broadcast(filteredDF),'customer_id')

        print('JOINED DF')
        joinDF.show()

        return joinDF.select("customer_id","customer_name","location")
    
class OnlyAirpodsAndIphone(Transformer):
    def transform(self, inputDFs):
        """
        Cusotmers who have bought only Iphone and Airpods nothing else
        """
        transactionInputDF = inputDFs.get("transactionInputDF")
        
        print("transactionInputDF in transform")
        
        
        groupedDF = transactionInputDF.groupBy("customer_id").agg(
            collect_set("product_name").alias("products")
        )
        
        print("groupedDF")
        groupedDF.show()
        
        filteredDF = groupedDF.filter(
            (array_contains(col("products"), "iPhone")) & 
            (array_contains(col("products"), "AirPods")) &
            (size(col("products")) == 2)
        )

        filteredDF.orderBy("customer_id").show(truncate=False)
        
        customerInputDF = inputDFs.get("customerInputDF")
        
        customerInputDF.show()

        joinDF = customerInputDF.join( broadcast(filteredDF),'customer_id')

        print('JOINED DF')
        joinDF.show()

        return joinDF.select("customer_id","customer_name","location")
            