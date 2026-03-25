"""
Core CDC (Change Data Capture) module for Databricks Delta Lake.
"""

import os
import glob
from datetime import datetime, date, timedelta
from typing import Any, Optional, List

from .utils import Utils
from .exceptions import CDCVersionError, CDCSchemaError, CDCException

try:
    if 'DATABRICKS_RUNTIME_VERSION' in os.environ:
        from databricks.sdk.runtime import spark, dbutils
        from pyspark.sql.dataframe import DataFrame
        from pyspark.sql.types import (
            StructType, StructField, TimestampType, LongType
        )
    else:
        DataFrame = Any
        spark = None
        dbutils = None
except ImportError as e:
    print(f"Error importing Databricks module: {e}")
    DataFrame = Any
    spark = None
    dbutils = None


class CDC:
    """
    Change Data Capture (CDC) framework for Databricks Delta Lake.

    Provides methods to write, manage, and track changes in Delta tables.
    """

    @staticmethod
    def write_to_cdc_feed(
        spark_dataframe: DataFrame,
        folder_path: str,
        keys: List[str],
        source_timestamp: datetime,
        exclude_columns_from_tracking: Optional[List[str]] = None,
        vacuum_days: int = 7,
        recurse: bool = True,
        batch: bool = True,
    ) -> None:
        """
        Write data to CDC feed with change tracking.

        Args:
            spark_dataframe: Input Spark dataframe
            folder_path: Target folder path for CDC data
            keys: List of key columns for merge operations
            source_timestamp: Timestamp of data source
            exclude_columns_from_tracking: Columns to exclude from change detection
            vacuum_days: Number of days to retain before vacuuming
            recurse: Whether to handle historical rewrites
            batch: Whether to use batch mode for writes

        Raises:
            CDCException: If operation fails
        """
        if spark is None:
            raise CDCException(
                "Spark context not available. This function requires a Databricks environment."
            )

        spark.conf.set('spark.sql.caseSensitive', True)
        spark.conf.set('spark.databricks.delta.properties.defaults.enableChangeDataFeed', True)

        if isinstance(source_timestamp, date) and not isinstance(source_timestamp, datetime):
            source_timestamp = datetime.combine(source_timestamp, datetime.min.time())

        cdc_utils = HelperFunctions(
            spark_dataframe,
            folder_path,
            keys,
            source_timestamp,
            exclude_columns_from_tracking,
            vacuum_days,
            recurse,
        )

        if spark_dataframe.isEmpty():
            cdc_utils.handle_empty_dataframe()
        elif not spark.catalog.tableExists(cdc_utils.target_table_name):
            spark_dataframe.write.format("delta").mode("overwrite").saveAsTable(
                cdc_utils.target_table_name
            )
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
        """
        Delete a CDC feed and associated tables.

        Args:
            folder_path: Path to the CDC feed to delete

        Raises:
            CDCException: If operation fails
        """
        if spark is None or dbutils is None:
            raise CDCException(
                "Spark/dbutils context not available. This function requires a Databricks environment."
            )

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
        """
        Generate the CDC tracker table name from a folder path.

        Args:
            folder_path: Path to derive table name from

        Returns:
            Fully qualified CDC tracker table name
        """
        cat_schema = folder_path.strip('/').split('/')[1:3]
        table_name = folder_path.strip('/').split('/')[3:]
        table_name = '_'.join(table_name)
        cat_schema = '.'.join(cat_schema)
        return f'{cat_schema}.{table_name}'.strip('')


class HelperFunctions:
    """
    Helper functions for CDC operations.

    Handles merge operations, versioning, schema evolution, and data management.
    """

    def __init__(
        self,
        spark_dataframe: DataFrame,
        folder_path: str,
        keys: List[str],
        source_timestamp: datetime,
        exclude_columns_from_tracking: Optional[List[str]],
        vacuum_days: int,
        recurse: bool,
    ):
        """Initialize helper functions for CDC operations."""
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
        """Generate table name from folder path."""
        cat_schema = self.folder_path.strip('/').split('/')[1:3]
        table_name = self.folder_path.strip('/').split('/')[3:]
        table_name = '_'.join(table_name)
        cat_schema = '.'.join(cat_schema)
        return f'{cat_schema}.{table_name}'.strip('')

    def convert_keys(self) -> str:
        """Convert key columns to merge condition string."""
        merge_keys = " and ".join([f"existing.{col} = updates.{col}" for col in self.keys])
        return merge_keys

    def create_condition(self) -> str:
        """Create condition for detecting changed columns."""
        source_fields = self.spark_dataframe.schema.names
        if self.exclude_columns_from_tracking is not None:
            source_fields = [
                field for field in source_fields
                if field not in self.exclude_columns_from_tracking
            ]
        condition = " or ".join([f"existing.{col} != updates.{col}" for col in source_fields])
        return condition

    def get_target_fields(self) -> List[str]:
        """Get column names from target table."""
        if spark.catalog.tableExists(self.target_table_name):
            columns = spark.read.table(self.target_table_name).schema.names
        else:
            columns = []
        return columns

    def is_schema_changed(self) -> bool:
        """Check if source schema differs from target table schema."""
        source_fields = self.spark_dataframe.schema.names
        target_fields = self.get_target_fields()
        return not all(item in target_fields for item in source_fields)

    def vacuum_data(self) -> None:
        """Vacuum the Delta table to remove old versions."""
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
        """Revert Delta table to a specific version."""
        from delta.tables import DeltaTable

        delta_table = DeltaTable.forName(spark, self.target_table_name)
        delta_table.restoreToVersion(version)

    def get_cdc_versions(self, source_timestamp: datetime) -> DataFrame:
        """Get CDC versions from helper table."""
        source_timestamp = source_timestamp.strftime('%Y-%m-%d')
        df = spark.sql(f"""
            with cte_group_timestamp as (
                select
                    source_timestamp,
                    max(version) as version,
                    max(created_on) as created_on
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
        """Read a specific version of the Delta table."""
        return spark.read.option("versionAsOf", version).table(self.target_table_name)

    @staticmethod
    def get_min_version(versions_df: DataFrame) -> int:
        """Get minimum version from versions dataframe."""
        return int(versions_df.orderBy("version").limit(1).collect()[0]['version'])

    @staticmethod
    def get_min_timestamp(versions_df: DataFrame) -> datetime:
        """Get minimum created timestamp from versions dataframe."""
        return versions_df.orderBy("created_on").limit(1).collect()[0]['created_on']

    @staticmethod
    def get_max_source_timestamp(versions_df: DataFrame) -> datetime:
        """Get maximum source timestamp from versions dataframe."""
        return versions_df.orderBy("source_timestamp", ascending=False).limit(1).collect()[0]['source_timestamp']

    @staticmethod
    def get_min_source_timestamp(versions_df: DataFrame) -> datetime:
        """Get minimum source timestamp from versions dataframe."""
        return versions_df.orderBy("source_timestamp").limit(1).collect()[0]['source_timestamp']

    def get_latest_cdc_version(self) -> int:
        """Get the latest CDC version number."""
        from delta.tables import DeltaTable

        delta_table = DeltaTable.forName(spark, self.target_table_name)
        return int(delta_table.history().orderBy("version", ascending=False).limit(1).collect()[0]['version'])

    def get_latest_cdc(self, starting_version: int) -> DataFrame:
        """Get latest changes from a starting version."""
        df = (spark.read
              .option("readChangeFeed", "true")
              .option("startingVersion", starting_version)
              .table(self.target_table_name))
        return df.filter(df["_change_type"].isin("update_postimage", "insert", "delete"))

    def handle_empty_dataframe(self) -> None:
        """Handle case where input dataframe is empty."""
        print(f'No data in dataframe {self.source_timestamp}')
        if not spark.catalog.tableExists(self.target_table_name):
            version = None
        else:
            version = self.get_latest_cdc_version()
        self.create_cdc_helper_table(version)

    def create_cdc_helper_table(self, version: Optional[int]) -> None:
        """Create or append to CDC helper tracking table."""
        from pyspark.sql.types import StructType, StructField, TimestampType, LongType

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

    def merge_data(self, condition: Optional[str] = None) -> None:
        """Merge data into target table with optional change detection condition."""
        from delta.tables import DeltaTable

        delta_table = DeltaTable.forName(spark, self.target_table_name)
        delta_table.alias("existing") \
            .merge(self.spark_dataframe.alias("updates"), self.merge_keys) \
            .withSchemaEvolution() \
            .whenNotMatchedInsertAll() \
            .whenMatchedUpdateAll(condition=condition) \
            .whenNotMatchedBySourceDelete() \
            .execute()

    def rewrite_cdc_feed(self) -> None:
        """Rewrite CDC feed when timestamped inserts arrive out of order."""
        versions_df = self.get_cdc_versions(self.source_timestamp)

        if self.get_min_timestamp(versions_df) < (datetime.now() - timedelta(days=self.vacuum_days)):
            raise CDCVersionError(
                f'File is older than {self.vacuum_days} days old. '
                f'Files are set to be vacuumed every {self.vacuum_days} days'
            )
        if self.source_timestamp < self.get_min_source_timestamp(versions_df):
            raise CDCVersionError(
                f'File is older than the first file loaded in change data capture feed. '
                f'File not processed {self.source_timestamp}'
            )

        min_version = self.get_min_version(versions_df)
        versions = versions_df.filter(versions_df.version != min_version).collect()

        self.revert_delta_table(min_version)

        CDC.write_to_cdc_feed(
            self.spark_dataframe,
            self.folder_path,
            self.keys,
            self.source_timestamp,
            self.exclude_columns_from_tracking,
            recurse=False,
        )

        for row in versions:
            source_timestamp = row['source_timestamp']
            version = row['version']
            spark_dataframe = self.get_version_of_df(version)
            CDC.write_to_cdc_feed(
                spark_dataframe,
                self.folder_path,
                self.keys,
                source_timestamp,
                self.exclude_columns_from_tracking,
                recurse=False,
            )

    def final_cdc_push(self, batch: bool) -> None:
        """Push final CDC changes to output volume."""
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
