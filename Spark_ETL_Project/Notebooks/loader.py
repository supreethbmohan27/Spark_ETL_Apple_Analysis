# Databricks notebook source
# MAGIC %run "/Workspace/Users/supreethbmohan@icloud.com/loader_factory"

# COMMAND ----------

class AbstractLoader:
    def __init__(self, transformedDF):
        self.transformedDF = transformedDF
    def sink(self):
        pass

class AirpodsAfterIphoneLoader(AbstractLoader):
    # def sink(self):
    #     get_sink_source(
    #         sink_type = "volumes",
    #         df = self.transformedDF,
    #         path = "/Volumes/workspace/appledata/appleanalysis/output",
    #         method="overwrite"
    #     ).load_data_frame

    def sink(self):

        sink = get_sink_source(
            sink_type="volume",
            df=self.transformedDF,
            path="/Volumes/workspace/appledata/appleanalysis/output",
            method="overwrite"
        )

        sink.load_data_frame()


class OnlyAirpodsAndIphoneLoader(AbstractLoader):

    def sink(self):
        params = {
            "partitionByColumns": ["location"]
        }
        
        sink = get_sink_source(
            sink_type="volume_partition",
            df=self.transformedDF,
            path="/Volumes/workspace/appledata/appleanalysis/output/onlyairpodsandiphone",
            method="overwrite",
            params = params
        )

        sink = get_sink_source(
            sink_type="delta",
            df=self.transformedDF,
            path="workspace.appledata.appleanalysis.onlyairpodsandiphone",
            method="overwrite",
            params = params
        )

        sink.load_data_frame()
