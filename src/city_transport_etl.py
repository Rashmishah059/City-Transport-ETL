import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import max, avg
import pandas as pd

class CityTransportETLJob:
    def __init__(self):
        
        self.current_dir = os.path.dirname(os.path.abspath(__file__))
        self.input_path = os.path.join(self.current_dir, "../data/city-transport-data.csv")
        self.output_path = os.path.join(self.current_dir, "../city-transport-analysis")

        os.makedirs(self.output_path, exist_ok=True)

        self.spark_session = (
            SparkSession.builder
            .appName("CityTransportETLJob")
            .master("local[*]")
            .config("spark.hadoop.io.native.lib.available", "false")
            .getOrCreate()
        )

    def extract(self):
        df = self.spark_session.read.csv(self.input_path, header=True, inferSchema=True)
        df.show(5)
        return df

    def transform(self, df):
        agg_df = df.groupBy("transport_id").agg(
            avg("passenger_count").alias("avg_passengers"),
            max("passenger_count").alias("max_passengers")
        )
        return agg_df

    def load(self, agg_df):
        os.makedirs(self.output_path, exist_ok=True)
        output_file = os.path.join(self.output_path, "city-transport-analysis.csv")
        agg_df.coalesce(1).toPandas().to_csv(output_file, index=False)
        print(f"Data successfully written to {output_file}")


    def run(self):
        self.load(self.transform(self.extract()))

etl_job = CityTransportETLJob()
etl_job.run()
