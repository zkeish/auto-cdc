from datetime import datetime, timezone
import shutil
import os

class AppFunctions:

    @staticmethod
    def get_spark(app_name: str='spark_app'):
        from pyspark.sql import SparkSession
        try:
            return SparkSession.builder.getOrCreate()
        except Exception as exc:
            return SparkSession.builder \
                .appName(app_name) \
                .getOrCreate()

    @staticmethod
    def format_file_now(prefix: str, ext: str):
        """
        Formats the current date and time into a specific file name format and returns the formatted file name.
        """
        now = datetime.now()
        year = now.strftime('%Y')
        month = now.strftime('%m')
        day = now.strftime('%d')
        hour = now.strftime('%H')
        minute = now.strftime('%M')
        second = now.strftime('%S')
        file_name = f'{prefix}_{year}_{month}_{day}_{hour}_{minute}_{second}.{ext}'
        return file_name
    
    @staticmethod
    def format_file_datetime(prefix: str, elt_timestamp: datetime, ext: str):
        """
        Formats the current date and time using the etl_timestamp input into a specific file name format and returns the formatted file name.
        """
        now = elt_timestamp
        year = now.strftime('%Y')
        month = now.strftime('%m')
        day = now.strftime('%d')
        hour = now.strftime('%H')
        minute = now.strftime('%M')
        second = now.strftime('%S')
        file_name = f'{prefix}_{year}_{month}_{day}_{hour}_{minute}_{second}.{ext}'
        return file_name
    
    @staticmethod
    def spark_write(spark_dataframe, batch_size: int, temp_path: str, file_ext):
        if file_ext == 'parquet':
            spark_dataframe.repartition(batch_size).write.mode("overwrite").parquet(temp_path)
        elif file_ext == 'csv':
            spark_dataframe.repartition(batch_size).write.mode("overwrite").option("header", "true").csv(temp_path)

    @staticmethod
    def save_spark_table(spark_dataframe, file_path: str, write_to_memory: bool=False, batch_size: int=1, lexical_prefix: bool=False) -> list[str] | None:
        """
        Writes a Spark DataFrame as a single Parquet file to a specified path.\n
        1. Ensures proper path formatting and creates necessary directories.\n
        2. Writes DataFrame to a temporary Parquet file.\n
        3. Moves the Parquet file to the target location and cleans up temporary files.
        4. Writing to memory still needs a temporary file location to parse the spark output
        """

        base_file_path, file_name = os.path.split(file_path)
        if not base_file_path or not file_name:
            raise ValueError(f"Invalid file path: {file_path}")

        _, ext = os.path.splitext(file_path)
        ext = ext.lower()
        if ext not in {".csv", ".parquet"}:
            raise ValueError(f"Unsupported file extension: {file_path}")

        file_ext = "csv" if ext == ".csv" else "parquet"
        
        temp_path = f'{base_file_path}/temp_output'
        Utils.mkdirs(base_file_path)

        AppFunctions.spark_write(spark_dataframe, batch_size, temp_path, file_ext)
        
        names_lst = []
        bytes_lst = []
        for tmp_file in Utils.ls(temp_path):
            _, tmp_file = os.path.split(file_path)
            if tmp_file.endswith('.parquet'):
                names_lst.append(tmp_file)
        
        if len(names_lst) == 0:
            raise Exception(f'{file_ext} file not found')
        else:
            max_value = len(names_lst)
            width = len(str(max_value))

        if lexical_prefix:
            unix_timestamp = int(datetime.now(timezone.utc).timestamp())
            unix_timestamp = f'{unix_timestamp:011d}_'
        else:
            unix_timestamp = ""

        for index, name in enumerate(names_lst):
            if write_to_memory:
                with open(f'{temp_path}/{name}', "rb") as f:
                    bytes_lst.append(f.read())
            else:
                if len(names_lst) > 1:
                    Utils.cp(f'{temp_path}/{name}', f'{base_file_path}/{unix_timestamp}{index:0{width}d}_{file_name}')
                else:
                    Utils.cp(f'{temp_path}/{name}', f'{base_file_path}/{unix_timestamp}{file_name}')
        
        Utils.rm(temp_path, recurse=True)
        if write_to_memory:
            return bytes_lst

    @staticmethod
    def save_data_to_file(data, file_path: str, zip: bool=False, batch_size: int=1, lexical_prefix: bool=False):
        """
        Saves data to a specified file type (parquet, csv, xml, json) in a directory, 
        optionally compressing it into a zip file.\n
        1. Determines file type and writes data accordingly.\n
        2. If 'zip' is True, compresses the file into a zip archive.
        """
        import json
        try:
            base_file_path = file_path.rsplit('/',1)[0]
            file = file_path.rsplit('/',1)[1]
            file_name = file.rsplit('.',1)[0]
            file_type = file.rsplit('.',1)[1]
        except Exception as e:
            raise Exception(f'Invalid file path {e}')

        Utils.mkdirs(base_file_path)
        if zip == False:
            if file_type == 'parquet':
                AppFunctions.save_spark_table(data, file_path, batch_size=batch_size, lexical_prefix=lexical_prefix)
            elif file_type == 'csv':
                AppFunctions.save_spark_table(data, file_path, batch_size=batch_size, lexical_prefix=lexical_prefix)
            elif file_type == 'xml':
                with open(file_path, 'w') as f:
                    f.write(data)
            elif file_type == 'json':
                with open(file_path, 'w') as f:
                    json.dump(data, f)
            else:
                raise Exception("Not an accepted file type")
        
        else:
            from io import BytesIO
            import zipfile
            import zlib
            content = BytesIO()
            zip_name = f"{file_name}.zip"
            data_lst = None
            with zipfile.ZipFile(content, 'w', compression=zlib.DEFLATED, compresslevel=9) as zip_file:
                if file_type == 'json':
                    data = json.dumps(data).encode()
                    zip_file.writestr(file, data)
                elif file_type == 'parquet' and 'DataFrame' in str(type(data)):
                    data_lst = AppFunctions.save_spark_table(data, file_path, write_to_memory=True, batch_size=batch_size, lexical_prefix=lexical_prefix)
                elif file_type == 'csv' and 'DataFrame' in str(type(data)):
                    data_lst = AppFunctions.save_spark_table(data, file_path, write_to_memory=True, batch_size=batch_size, lexical_prefix=lexical_prefix)
                else:
                    zip_file.writestr(file, data)

                if data_lst:
                    for index, data in enumerate(data_lst):
                            zip_file.writestr(f'{index}_{file}', data)

                zip_file.close()
            
            with open(f'{base_file_path}/{zip_name}', 'wb') as zip_file2:
                zip_file2.write(content.getvalue())

    @staticmethod
    def read_raw_file(file_path: str, xml_root: str | None=None, xml_tag: str | None=None, skip_rows: int=0, multi_line: bool=False, quote_char: str='"', escape_char: str='\\'):
        """
        Reads raw data from a specified file (json, csv, xml, parquet) and converts all data types to strings.\n
        1. Infers the schema and reads the file based on its type.\n
        2. Replaces the inferred data types with StringType for consistency.\n
        3. Supports skipping rows for CSV and custom XML root/tag parsing.
        """
        file_type = file_path.rsplit('.', 1)[1]
        spark = AppFunctions.get_spark()

        def convert_data_types(schema):
            # string replace schema
            from pyspark.sql.types import (
                StringType, IntegerType, LongType, FloatType, DoubleType, ShortType, ByteType,
                BooleanType, DateType, TimestampType, BinaryType, DecimalType, ArrayType, 
                MapType, StructType, StructField, NullType
            )
            schema = str(schema)
            for data_type in ['IntegerType()', 'DoubleType()', 'LongType()', 'BooleanType()', 'DateType()', 'TimestampType()', 'ShortType()', 'FloatType()', 'DecimalType()']:
                schema = schema.replace(data_type, 'StringType()')
            return eval(schema)

        if file_type == 'json':
            df = spark.read.option('inferSchema','true').option("dropFieldIfAllNull","true").json(file_path)
            new_schema = convert_data_types(df.schema)
            df = spark.read.schema(new_schema).option('inferSchema','true').json(file_path)
        elif file_type == 'csv':
            df = spark.read.option('inferSchema','True').option("header", "true").option("skipRows", skip_rows).option("multiLine", multi_line).csv(file_path)
            new_schema = convert_data_types(df.schema)
            df = spark.read.schema(new_schema).option('inferSchema','True').option("header", "true").option("skipRows", skip_rows).option("multiLine", multi_line).option("quote",quote_char).option("escape", escape_char).csv(file_path)
        elif file_type == 'xml':
            df = spark.read.format("xml").option("rootTag", xml_root).option("rowTag", xml_tag).option("excludeAttribute", "true").load(file_path)
            new_schema = convert_data_types(df.schema)
            df = spark.read.schema(new_schema).format("xml").option("rootTag", xml_root).option("rowTag", xml_tag).load(file_path)
        elif file_type == 'parquet':
            df = spark.read.option('inferSchema','true').parquet(file_path)
        else:
            raise Exception('File type not accepted')
        
        return df

class Utils:

    @staticmethod
    def cp(source, dest, recurse: bool=False) -> None:

        if os.path.exists(source):
            raise FileNotFoundError(source)
        if recurse and not os.path.isdir(source):
            raise ValueError("Source must be a directory when recurse=True")
        if not recurse and not os.path.isfile(source):
            raise ValueError("Source must be a file when recurse=False")
            
        if recurse:
            for root, dirs, files in os.walk(source):
                rel_path = os.path.relpath(root, source)
                target_dir = os.path.join(dest, rel_path)
                os.makedirs(target_dir, exist_ok=True)
                for file in files:
                    shutil.copy2(os.path.join(root, file), os.path.join(target_dir, file))
        else:
            shutil.copy2(source, dest)
        
    @staticmethod
    def mkdirs(dir) -> None:
        os.makedirs(dir)

    @staticmethod
    def ls(dir, recurse: bool=False) -> list[str]:

        if not os.path.exists(dir):
            raise FileNotFoundError(dir)
        if not os.path.isdir(dir):
            raise ValueError("Must be a directory")
            
        if recurse:
            fnl_file_lst = list()
            for root, dirs, files in os.walk(dir):
                for f in files:
                    fnl_file_lst.append(os.path.join(root, f))
            return fnl_file_lst
        else:
            return [f for f in os.listdir(dir)]

    @staticmethod
    def mv(source, dest, recurse: bool=False) -> None:

        if os.path.exists(source):
            raise FileNotFoundError(source)
        if recurse and not os.path.isdir(source):
            raise ValueError("Source must be a directory when recurse=True")
        if not recurse and not os.path.isfile(source):
            raise ValueError("Source must be a file when recurse=False")
        
        if recurse:
            for root, dirs, files in os.walk(source):
                rel_path = os.path.relpath(root, source)
                target_dir = os.path.join(dest, rel_path)
                os.makedirs(target_dir, exist_ok=True)
                for file in files:
                    shutil.move(os.path.join(root, file), os.path.join(target_dir, file))
        else:
            shutil.move(source, dest)

    @staticmethod
    def rm(dir, recurse: bool=False) -> None:

        if os.path.exists(dir):
            raise FileNotFoundError(dir)
        if recurse and not os.path.isdir(dir):
            raise ValueError("Path must be a directory when recurse=True")
        if not recurse and not os.path.isfile(dir):
            raise ValueError("Path must be a file when recurse=False")
        
        if recurse:
            shutil.rmtree(dir)
        else:
            os.remove(dir)