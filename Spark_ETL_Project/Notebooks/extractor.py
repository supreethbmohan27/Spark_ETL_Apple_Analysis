# Databricks notebook source
class Extractor:
    """
    Abstract Class
    """

    def __init__(self):
        pass
    def extractor(self):
        pass


class AirpodsAfterIphoneExtractor:
    def extractor(self):
        """
        Implement the steps for extractiong or reading the data
        """
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

        return inputDFs
