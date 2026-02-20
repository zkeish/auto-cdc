import os
from datetime import datetime, date, timedelta
import glob
from typing import Any

try:
    if 'DATABRICKS_RUNTIME_VERSION' in os.environ:
        from databricks.sdk.runtime import *
        from pyspark.sql.dataframe import DataFrame
        from pyspark.sql.types import *
    else:
        DataFrame = Any
except ImportError as e:
    print(f"Error importing module: {e}")


class CDC:

    @staticmethod
    def write_to_cdc_feed(spark_dataframe: DataFrame, folder_path: str, keys: list, source_timestamp: datetime, exclude_columns_from_tracking: list=None, vacuum_days: int=7, recurse: bool=True, batch: bool=True) -> None:
        spark.conf.set('spark.sql.caseSensitive', True)
        spark.conf.set('spark.databricks.delta.properties.defaults.enableChangeDataFeed', True)
        
        if isinstance(source_timestamp, date):
            source_timestamp = datetime.combine(source_timestamp, datetime.min.time())

        cdc_utils = HelperFunctions(spark_dataframe, folder_path, keys, source_timestamp, exclude_columns_from_tracking, vacuum_days, recurse)

        if spark_dataframe.isEmpty():
            cdc_utils.handle_empty_dataframe()
        elif not spark.catalog.tableExists(cdc_utils.target_table_name):
            spark_dataframe.write.format("delta").mode("overwrite").saveAsTable(cdc_utils.target_table_name)
            cdc_utils.final_cdc_push(batch=batch)
        else:
            versions_df = cdc_utils.get_cdc_versions(date(1900, 1, 1))
            last_source_timestamp = cdc_utils.get_max_source_timestamp(versions_df)
            
            if source_timestamp < last_source_timestamp and recurse:
                cdc_utils.rewrite_cdc_feed()
            else:
                if cdc_utils.is_schema_changed():
                    cdc_utils.merge_data()
                else:
                    condition = cdc_utils.create_condition()
                    cdc_utils.merge_data(condition)
                cdc_utils.final_cdc_push(batch=batch)
    
    @staticmethod
    def delete_cdc_feed(folder_path: str) -> None:
        cat_schema = folder_path.strip('/').split('/')[1:3]
        table_name = folder_path.strip('/').split('/')[3:]
        table_name = '_'.join(table_name)
        cat_schema = '.'.join(cat_schema)
        target_table_name = f'{cat_schema}.{table_name}'.strip('')
        spark.sql(f"DROP TABLE IF EXISTS {target_table_name}")
        spark.sql(f"DROP TABLE IF EXISTS {target_table_name}_helper")
        dbutils.fs.rm(folder_path, recurse=True)

    @staticmethod
    def cdc_tracker_table_name(folder_path: str) -> str:
        cat_schema = folder_path.strip('/').split('/')[1:3]
        table_name = folder_path.strip('/').split('/')[3:]
        table_name = '_'.join(table_name)
        cat_schema = '.'.join(cat_schema)
        table_name = f'{cat_schema}.{table_name}'.strip('')
        return table_name


class HelperFunctions:

    def __init__(self, spark_dataframe: DataFrame, folder_path: str, keys: list, source_timestamp: datetime, exclude_columns_from_tracking: list, vacuum_days: int, recurse: bool):
        self.source_timestamp = source_timestamp
        self.folder_path = folder_path.rstrip('/')
        self.keys = keys
        self.spark_dataframe = spark_dataframe
        self.target_table_name = self.create_table_name_from_folder_path()
        self.merge_keys = self.convert_keys()
        self.exclude_columns_from_tracking = exclude_columns_from_tracking
        self.vacuum_days = vacuum_days
        self.recurse = recurse

    def create_table_name_from_folder_path(self) -> str:
        cat_schema = self.folder_path.strip('/').split('/')[1:3]
        table_name = self.folder_path.strip('/').split('/')[3:]
        table_name = '_'.join(table_name)
        cat_schema = '.'.join(cat_schema)
        return f'{cat_schema}.{table_name}'.strip('')
    
    def convert_keys(self) -> str:
        merge_keys = " and ".join([f"existing.{col} = updates.{col}" for col in self.keys])
        return merge_keys
    
    def create_condition(self) -> str:
        source_fields = self.spark_dataframe.schema.names
        if self.exclude_columns_from_tracking is not None:
            source_fields = [field for field in source_fields if field not in self.exclude_columns_from_tracking]
        condition = " or ".join([f"existing.{col} != updates.{col}" for col in source_fields])
        return condition
    
    def get_target_fields(self) -> list:
        if spark.catalog.tableExists(self.target_table_name):
            columns = spark.read.table(self.target_table_name).schema.names
        else:
            columns = []
        return columns
       
    def is_schema_changed(self) -> bool:
        source_fields = self.spark_dataframe.schema.names
        target_fields = self.get_target_fields()
        if all(item in target_fields for item in source_fields):
            return False
        else:
            return True
        
    def vacuum_data(self) -> None:
        from delta.tables import DeltaTable
        delta_table = DeltaTable.forName(spark, self.target_table_name)
        last_vacuum_timestamp = spark.sql(f"""
                    SELECT MAX(timestamp) AS max_timestamp
                    FROM (
                        SELECT timestamp
                        FROM (DESCRIBE HISTORY {self.target_table_name})
                        WHERE operation LIKE 'VACUUM%'
                        )
                    """).collect()[0][0]
        if last_vacuum_timestamp:
            if last_vacuum_timestamp < (datetime.now() - timedelta(days=self.vacuum_days)):
                delta_table.vacuum((self.vacuum_days * 24))
        else:
            delta_table.vacuum((self.vacuum_days * 24))

    def revert_delta_table(self, version: int) -> None:
        from delta.tables import DeltaTable
        delta_table = DeltaTable.forName(spark, self.target_table_name)
        delta_table.restoreToVersion(version)
    
    def get_cdc_versions(self, source_timestamp: datetime) -> DataFrame:
        source_timestamp = source_timestamp.strftime('%Y-%m-%d')
        df = spark.sql(f"""
                        with cte_group_timestamp as (
                            select 
                                  source_timestamp
                                , max(version) as version
                                , max(created_on) as created_on
                            from {self.target_table_name}_helper
                            where version is not null
                            group by source_timestamp
                        )
                        select distinct * from cte_group_timestamp
                        qualify ifnull(lead(source_timestamp) over (order by source_timestamp), source_timestamp) >= '{source_timestamp}'
                        order by version
                        """)
        return df.cache()
    
    def get_version_of_df(self, version: int) -> DataFrame:
        return (spark.read.option("versionAsOf", version).table(self.target_table_name))
    
    @staticmethod
    def get_min_version(versions_df: DataFrame) -> int:
        return int(versions_df.orderBy("version").limit(1).collect()[0]['version'])
    
    @staticmethod
    def get_min_timestamp(versions_df: DataFrame) -> int:
        return versions_df.orderBy("created_on").limit(1).collect()[0]['created_on']
    
    @staticmethod
    def get_max_source_timestamp(versions_df: DataFrame) -> datetime:
        return versions_df.orderBy("source_timestamp", ascending=False).limit(1).collect()[0]['source_timestamp']
    
    @staticmethod
    def get_min_source_timestamp(versions_df: DataFrame) -> datetime:
        return versions_df.orderBy("source_timestamp").limit(1).collect()[0]['source_timestamp']
    
    def get_latest_cdc_version(self) -> int:
        from delta.tables import DeltaTable
        delta_table = DeltaTable.forName(spark, self.target_table_name)
        return int(delta_table.history().orderBy("version", ascending=False).limit(1).collect()[0]['version'])
    
    def get_latest_cdc(self, starting_version: int) -> DataFrame:
        df = (spark.read
                .option("readChangeFeed", "true")
                .option("startingVersion", starting_version)
                .table(self.target_table_name)
                )
        return df.filter(df["_change_type"].isin("update_postimage", "insert", "delete"))
        
    def handle_empty_dataframe(self):
        print(f'No data in dataframe {self.source_timestamp}')
        if not spark.catalog.tableExists(self.target_table_name):
            version = None
        else:
            version = self.get_latest_cdc_version()
        self.create_cdc_helper_table(version)

    def create_cdc_helper_table(self, version: int) -> None:
        helper_table = f'{self.target_table_name}_helper'
        data = [(self.source_timestamp, version, datetime.now())]
        schema = StructType([
                    StructField('source_timestamp', TimestampType(), True),
                    StructField('version', LongType(), True),
                    StructField('created_on', TimestampType(), True)
                ])
        spark_dataframe = spark.createDataFrame(data, schema)
        if not spark.catalog.tableExists(helper_table):
            spark_dataframe.write.format("delta").mode("overwrite").saveAsTable(helper_table)
        else:
            spark_dataframe.write.format("delta").mode("append").saveAsTable(helper_table)
        
    def merge_data(self, condition: str=None) -> None:
        from delta.tables import DeltaTable
        delta_table = DeltaTable.forName(spark, self.target_table_name)
        delta_table.alias("existing") \
            .merge(self.spark_dataframe.alias("updates"), self.merge_keys) \
            .withSchemaEvolution() \
            .whenNotMatchedInsertAll() \
            .whenMatchedUpdateAll(
                condition=condition) \
            .whenNotMatchedBySourceDelete() \
            .execute()
        
    def rewrite_cdc_feed(self) -> None:
        versions_df = self.get_cdc_versions(self.source_timestamp)
        if self.get_min_timestamp(versions_df) < (datetime.now() - timedelta(days=self.vacuum_days)):
            raise Exception(f'File is older than {self.vacuum_days} days old. Files are set to be vacuumed every {self.vacuum_days} days')
        if self.source_timestamp < self.get_min_source_timestamp(versions_df):
            raise Exception(f'File is older than the first file loaded in change data capture feed. File not processed {self.source_timestamp}')
        min_version = self.get_min_version(versions_df)
        versions = versions_df.filter(versions_df.version != min_version).collect()
        self.revert_delta_table(min_version)
        CDC.write_to_cdc_feed(self.spark_dataframe, self.folder_path, self.keys, self.source_timestamp, self.exclude_columns_from_tracking, recurse=False)
        for row in versions:
            source_timestamp = row['source_timestamp']
            version = row['version']
            spark_dataframe = self.get_version_of_df(version)
            CDC.write_to_cdc_feed(spark_dataframe, self.folder_path, self.keys, source_timestamp, self.exclude_columns_from_tracking, recurse=False)
        
    def final_cdc_push(self, batch: bool) -> None:
        latest_version = self.get_latest_cdc_version()
        change_data_df = self.get_latest_cdc(latest_version)
        file_name = Utils.format_file_datetime('cdc', self.source_timestamp, 'parquet')
        output_path = os.path.join(self.folder_path, file_name)
        if not change_data_df.isEmpty():
            if self.recurse:
                files_to_remove = glob.glob(f"{self.folder_path}/*{file_name}")
                for file in files_to_remove:
                    dbutils.fs.rm(file)
            Utils.push_to_volume(change_data_df, output_path, batch=batch)
            print(f'Pushed: {output_path}')
        else:
            print(f'No changes pushed {self.source_timestamp}')
        self.create_cdc_helper_table(latest_version)
        self.vacuum_data()
