"""
Spark-related functionality.
"""

import logging
import pgtoolkit.pgpass
import pyspark.sql
import re
import shutil
from collections.abc import Callable, Container, Iterable, Mapping
from functools import partial
from glob import glob
from pathlib import Path
from pyspark.sql import functions as F
from tempfile import TemporaryDirectory
from typing import Any, Optional
from .database_functions import get_pg_config
from .utils import cast_chromosome_to_int, cfg, zopen

DataFrame = pyspark.sql.dataframe.DataFrame
Row = pyspark.sql.Row
SparkSession = pyspark.sql.session.SparkSession
stypes = pyspark.sql.types
logger = logging.getLogger(__name__)


def setup_spark(
    cores: int | str = cfg["spark"]["cores"],
    memory: str = cfg["spark"]["memory"],
    use_db: bool = False,
    use_excel: bool = False,
    use_glow: bool = False,
    use_xml: bool = False,
    show_console_progress: bool = True,
    extra_options: Optional[Iterable[tuple[str, str]]] = None,
    extra_packages: Optional[Iterable[str]] = None,
    postgresql_jdbc: str = cfg["spark"]["jdbc"],
    excel_package: str = cfg["spark"]["packages"]["excel"],
    glow_codec: str = cfg["spark"]["codecs"]["glow"],
    glow_package: str = cfg["spark"]["packages"]["glow"],
    xml_package: str = cfg["spark"]["packages"]["xml"],
    spark_logging_level: int | str = logging.ERROR,
) -> SparkSession:
    """
    Configures and creates a PySpark session according to the supplied arguments

    :param cores: The number of cores to configure PySpark with
    :param memory: The amount of memory to configure PySpark with
    :param use_db: Configure PySpark to be able to query a database via JDBC
    :param use_excel: Configure PySpark to be able to parse Excel
        spreadsheets
    :param use_glow: Configure PySpark to use glow (e.g. to parse a VCF)
    :param use_xml: Configure PySpark to be able to parse XML files
    :param show_console_progress: Configure PySpark to show console progress
    :param extra_options: Any additional options to configure PySpark with
    :param extra_packages: Any additional packages for PySpark to load
    :param postgresql_jdbc: The path to the PostgreSQL JDBC jar for use if
        use_db is specified
    :param excel_package: The name of the package PySpark needs to parse
        Excel spreadsheets
    :param glow_codec: The name of the codec PySpark needs to load glow
    :param glow_package: The name of the package PySpark needs to load glow
    :param xml_package: The name of the package PySpark needs to parse XML files
    :param spark_logging_level: The logging level to configure py4j and pyspark with
    :return: The configured PySpark session
    """
    logging.getLogger("py4j").setLevel(spark_logging_level)
    logging.getLogger("pyspark").setLevel(spark_logging_level)
    spark = SparkSession.builder.config("spark.driver.memory", memory)
    if not show_console_progress:
        spark = spark.config("spark.ui.showConsolePorgress", "false")
    if extra_options:
        for option, value in extra_options:
            spark = spark.config(option, value)
    if use_db:
        spark = spark.config("spark.jars", postgresql_jdbc)
    if use_excel:
        spark = spark.config("spark.jars.packages", excel_package)
    if use_glow:
        spark = spark.config("spark.jars.packages", glow_package).config(
            "spark.hadoop.io.compression.codecs", glow_codec
        )
    if use_xml:
        spark = spark.config("spark.jars.packages", xml_package)
    if extra_packages:
        for extra_package in extra_packages:
            spark = spark.config("spark.jars.packages", extra_package)
    spark = spark.master(f"local[{cores}]").getOrCreate()
    if use_glow:
        # glow can take a while to load so isn't done until it's actually requested
        import glow

        spark = glow.register(spark)
    return spark


def load_dataframe(
    self: SparkSession,
    path: Path | str,
    format: str = "parquet",
    load_config_options: Optional[Iterable[tuple[str, str]]] = None,
    **kwargs,
) -> DataFrame:
    r"""
    Load and return the specified data source using PySpark

    :param self: The PySpark session to use
    :param path: The path to the data source to load
    :param format: The format of the data source
    :param load_config_options: Any additonal config options to load data
    :params \**kwargs: Any additional named arguments
    :return: The dataframe requested
    """
    load_func = self.read.format(format)
    if load_config_options:
        for option, value in load_config_options:
            logger.debug(f"Setting {option} to {value}.")
            load_func = load_func.option(option, value)
    logger.info(f"Loading {path} with format {format}.")
    return load_func.load(str(path), **kwargs)


SparkSession.load_dataframe = load_dataframe


def load_xml_to_dataframe(
    self: SparkSession,
    xml_fn: Path | str,
    row_tag: str,
    schema: Optional[str] = None,
    value_tag: Optional[str] = None,
    load_config_options: Optional[Iterable[tuple[str, str]]] = None,
    **kwargs,
) -> DataFrame:
    r"""
    Load and return the specified XML file with PySpark

    :param self: The PySpark session to use
    :param xml_fn: The path to the data source to load
    :param row_tag: The XML tag that delimits records
    :param schema: The path to an XSD schema to validate records
    :param value_tag: Passed as valueTag, suggested to use ``"_tag_value"``;
        can help with schema parsing and name conflict issues
    :param load_config_options: Any additonal config options to load data
    :param \**kwargs: Any additional named arguments
    :return: The dataframe requested
    """
    if load_config_options is None:
        load_config_options = []
    load_config_options = list(load_config_options)
    load_config_options.append(("rowTag", row_tag))
    if schema:
        load_config_options.append(("rowValidationPath", schema))
    if value_tag:
        load_config_options.append(("valueTag", value_tag))
    return self.load_dataframe(
        xml_fn,
        format="xml",
        load_config_options=load_config_options,
        **kwargs,
    )


SparkSession.load_xml_to_dataframe = load_xml_to_dataframe


def convert_xml_to_parquet(
    self: SparkSession,
    xml_fn: Path | str,
    output_directory: Path | str,
    row_tag: str,
    schema: Optional[str] = None,
    value_tag: Optional[str] = None,
    load_config_options: Optional[Iterable[tuple[str, str]]] = None,
    return_dataframe: bool = False,
    **kwargs,
) -> Optional[DataFrame]:
    r"""
    Convert the specified XML to parquet with PySpark.
    N.B. it can be substantially more efficient to query parquet data than XML.

    :param self: The PySpark session to use
    :param xml_fn: The path to the data source to load
    :param output_directory: The path to write parquet to
    :param row_tag: The XML tag that delimits records
    :param schema: The path to an XSD schema to validate records
    :param value_tag: Passed as valueTag, suggested to use ``"_tag_value"``;
        can help with schema parsing and name conflict issues
    :param load_config_options: Any additional config options to load data
    :param return_dataframe: Whether to load and return the new parquet data
    :param \**kwargs: Any additional named arguments
    :return: The dataframe requested if ``return_dataframe == True``
    """
    xml_df = self.load_xml_to_dataframe(
        xml_fn=xml_fn,
        row_tag=row_tag,
        schema=schema,
        value_tag=value_tag,
        load_config_options=load_config_options,
        **kwargs,
    )
    xml_df.write.mode("overwrite").parquet(str(output_directory))
    if return_dataframe:
        return self.load_dataframe(output_directory)


SparkSession.convert_xml_to_parquet = convert_xml_to_parquet


def load_db_to_dataframe(
    self: SparkSession,
    pgpass_record: pgtoolkit.pgpass.PassEntry = None,
    relation: Optional[str] = None,
    query: Optional[str] = None,
    load_config_options: Optional[Iterable[tuple[str, str]]] = None,
    service: str | None = None,
    **kwargs,
) -> DataFrame:
    r"""
    Return a PySpark dataframe from either a relation or query

    :param self: The PySpark session to use
    :param pgpass_record: PostgreSQL login credentials
    :param relation: The database relation to load
    :param query: The database query to load
    :param load_config_options: Any additonal config options to load data
    :param \**kwargs: Any additional named arguments
    :return: The dataframe requested
    """
    if relation is None == query is None:
        raise ValueError(f"Specify relation or query and not both.")
    load_func = self.read.format("jdbc")
    if not pgpass_record:
        pgpass_record = get_pg_config(service=service)
    load_func = (
        load_func.option(
            "url",
            f"jdbc:postgresql://{pgpass_record.hostname}:{pgpass_record.port}/{pgpass_record.database}",
        )
        .option("user", pgpass_record.username)
        .option("password", pgpass_record.password)
        .option("driver", "org.postgresql.Driver")
    )
    if relation:
        load_func = load_func.option("dbtable", relation)
    else:
        load_func = load_func.option("query", query)
    return load_func.load(**kwargs)


SparkSession.load_db_to_dataframe = load_db_to_dataframe


def load_excel_to_dataframe(
    self: SparkSession,
    xl_fn: str,
    header: bool = True,
    load_config_options: Optional[Iterable[tuple[str, str]]] = None,
    **kwargs,
) -> DataFrame:
    r"""
    Load and return the specified Excel spreadsheet with PySpark

    :param self: The PySpark session to use
    :param xl_fn: The path to the data source to load
    :param header: Whether the data source has a header or not
    :param load_config_options: Any additonal config options to load data
    :param \**kwargs: Any additional named arguments
    :return: The dataframe requested
    """
    if load_config_options is None:
        load_config_options = []
    load_config_options.append(("header", "true" if header else "false"))
    return self.load_dataframe(
        xl_fn,
        format="com.crealytics.spark.excel",
        load_config_options=load_config_options,
        **kwargs,
    )


SparkSession.load_excel_to_dataframe = load_excel_to_dataframe


def load_dataframe_with_preprocessing(
    self: SparkSession,
    path: Path | str,
    func: Callable[[str], bool],
    process_line_func: Optional[Callable[[str], list[str]]] = None,
    sep: str = ",",
) -> DataFrame:
    """
    Load the specified dataset and remove rows by calling the
    provided ``func`` which takes a line as input and returns whether
    it should be removed

    :param self: The PySpark session to use
    :param path: The path to the data source to load
    :param func: A callable object that takes a line of input as an argument
        and returns whether the line should be removed
    :param process_line_func: A callable object that takes a line of input and
        parses it to a list of fields. Defaults to splitting on ``sep`` if not
        provided.
    :param sep: The column separator to use
    :return: The dataframe requested
    """
    if not process_line_func:
        process_line_func = lambda x: x.split(sep)
    rdd = self.sparkContext.textFile(str(path)).filter(func).map(process_line_func)
    cols = rdd.first()
    return rdd.filter(lambda x: x != cols).toDF(cols)


SparkSession.load_dataframe_with_preprocessing = load_dataframe_with_preprocessing


def split_dataframe_to_csv_by_column_value(
    self: DataFrame,
    column_to_split_on: str,
    output_directory: str,
    filename_format: str = "{column_value}",
    include_header: bool = True,
    overwrite: bool = True,
    include_split_column_in_new_files: bool = False,
) -> None:
    """
    Split a dataframe with PySpark to a set of gzipped CSVs, e.g. if a dataframe
    has data:
    col1,col2,col3
    1,1,1
    1,2,3
    2,1,1

    and we split on col1, we will produce two files, the first file (col1=1) containing
    col2,col3
    1,1
    2,3

    and the second (col1=2) containing
    col2,col3
    1,1

    :param self: The dataframe to use
    :param column_to_split_on: The column name to split on
    :param output_directory: The directory to output to
    :param filename_format: Filename convention to use; use ``{column_value}``
        to refer to the split column's value
    :param include_header: Whether to include a header in each output file
    :param overwrite: Overwrite existing directory it if already exists
    :param include_split_column_in_new_files: Whether to include the column
        that was used to split in the resulting files
    """
    if include_header:
        cols = self.columns
        cols.remove(column_to_split_on)
        if include_split_column_in_new_files:
            cols.insert(0, column_to_split_on)
        header = ",".join(cols) + "\n"
    regex = re.compile(rf"^{column_to_split_on}=(.+)$")
    output_directory = Path(output_directory)
    if output_directory.exists():
        if overwrite:
            shutil.rmtree(output_directory)
    if not output_directory.is_dir():
        output_directory.mkdir(parents=True)
    with TemporaryDirectory() as d:
        dir_name = Path(d, "partitions")
        self.write.partitionBy(column_to_split_on).csv(dir_name)
        partitions = sorted(
            [p.stem for p in Path(dir_name).glob(f"{column_to_split_on}=*")]
        )
        logger.info("Found the following partitions:\n" + "\n".join(partitions))
        for partition in partitions:
            column_value = regex.match(partition).group(1)
            output_filename = Path(
                output_directory,
                filename_format.format(column_value=column_value) + ".csv.gz",
            )
            logger.debug(
                f"Writing split {column_to_split_on}={column_value} to {output_filename}."
            )
            with zopen(f"!gzip -9 > {output_filename}", "w") as out_fh:
                if include_header:
                    out_fh.write(header)
                for in_fn in sorted(Path(dir_name, partition).glob("part-*.csv")):
                    with open(in_fn) as in_fh:
                        for line in in_fh:
                            if include_split_column_in_new_files:
                                line = column_value + "," + line
                            out_fh.write(line)


DataFrame.split_dataframe_to_csv_by_column_value = (
    split_dataframe_to_csv_by_column_value
)


# for use with PySpark, casts string representations of human chromosomes to
# integers
cast_chromosome = F.udf(cast_chromosome_to_int, stypes.IntegerType())


def convert_chromosome(
    self: DataFrame, current_column_name: str, new_column_name: Optional[str] = None
) -> DataFrame:
    """
    Return a PySpark dataframe with ``current_column_name`` (containing human
    chromosomes) with a new column, ``new_column_name`` (defaulting to
    overwriting the original), with the chromosome cast as an integer.

    :param self: The dataframe to use
    :param current_column_name: The column name to cast
    :param new_column_name: The new column name to use
    :return: The processed dataframe
    """
    if not new_column_name:
        new_column_name = current_column_name
    return self.withColumn(new_column_name, cast_chromosome(current_column_name))


DataFrame.convert_chromosome = convert_chromosome


def count_nulls(
    self: DataFrame,
) -> DataFrame:
    """
    Return a PySpark dataframe with the number of null values in each column of
    a dataframe

    :param self: The dataframe to summarize
    :return: The new dataframe with null counts per column
    """
    return self.select(
        [F.count(F.when(F.isnull(col), col)).alias(col) for col in self.columns]
    )


DataFrame.count_nulls = count_nulls


def count_columns_with_string(self: DataFrame, string: str = "|") -> DataFrame:
    """
    Return a PySpark dataframe with the number of times a given string occurs
    in each string column in a dataframe

    :param self: The dataframe to summarize
    :param string: The string to search for
    :return: The new dataframe with counts per column
    """
    return self.select(
        [
            F.count(F.when(F.col(col.name).contains(s), 1)).alias(col.name)
            for col in self.schema
            if type(col.dataType) is stypes.StringType
        ]
    )


DataFrame.count_columns_with_string = count_columns_with_string


def count_distinct_values(
    self: DataFrame,
    columns_to_ignore: Container[str] = set(),
    approximate: bool = False,
) -> DataFrame:
    """
    Return a new PySpark dataframe with the number of distinct values in each
    column.  Uses :func:`pyspark.sql.functions.count_distinct` by default and
    :func:`pyspark.sql.functions.approx_count_distinct` if
    ``approximate == True``

    :param self: The dataframe to summarize
    :param columns_to_ignore: An optional set of columns to not summarize
    :param bool approximate: Get approximate counts instead of exact (faster)
    :return: The new dataframe with counts of distinct values per column
    """
    func = F.approx_count_distinct if approximate else F.count_distinct
    return self.agg(
        *[
            func(F.col(col)).alias(col)
            for col in self.columns
            if col not in columns_to_ignore
        ]
    )


DataFrame.count_distinct_values = count_distinct_values


def convert_dicts_to_dataframe(
    self: SparkSession,
    dict_list: Optional[Iterable[Mapping[str, Any]]] = None,
    iter_func: Optional[Callable[[], Iterable[Mapping[str, Any]]]] = None,
    coerce_to_lists_if_needed: bool = True,
) -> DataFrame:
    """
    Converts either a list of dicts (``dict_list``) or a function that returns
    an iterator of dicts (``iter_func``) to a PySpark dataframe

    :param self: The SparkSession to use
    :param dict_list: A list of dicts representing rows
    :param iter_func: A function that returns an iterator of dicts representing rows
    :param coerce_to_lists_if_needed: For any column to create, check if any
        value in a row is a list, and if so, convert any non-lists in the
        column to a list
    :return: A new dataframe built from the provided rows of dicts
    """
    if dict_list and iter_func:
        raise ValueError("Only one of dict_list or iter_func should be specified.")
    if (not dict_list) and (not iter_func):
        raise ValueError("dict_list or iter_func must be specified.")
    all_fields = set()
    if coerce_to_lists_if_needed:
        fields_with_lists = set()
    if dict_list:
        iter_func = partial(iter, dict_list)
    for record in iter_func():
        for field, value in record.items():
            all_fields.add(field)
            if coerce_to_lists_if_needed and type(value) is list:
                fields_with_lists.add(field)
    for record in iter_func():
        if coerce_to_lists_if_needed:
            for field in fields_with_lists:
                if field in record and type(record[field]) is not list:
                    # original data was inconsistent - some values are lists
                    # and this one is not so we make this one a list also
                    record[field] = [record[field]]
        for field in all_fields - record.keys():
            record[field] = None
    return self.createDataFrame(Row(**dict(sorted(x.items()))) for x in iter_func())


SparkSession.convert_dicts_to_dataframe = convert_dicts_to_dataframe


def serialize_array_field(
    self: DataFrame,
    column: str,
    new_column: str,
    dtype: pyspark.sql.types.ArrayType,
    struct_columns_to_use: Optional[Container] = None,
) -> DataFrame:
    """
    Serializes a ``pyspark.sql.types.ArrayType`` field for output.

    :param self: The dataframe to use
    :param column: The name of the column to serialize
    :param new_column: The name to give the new serialized column
    :param dtype: The column definition
    :param struct_columns_to_use: A set of struct values to use (assuming column
        is a struct)
    :raises NotImplementedError: If the type in the array is a nested struct
    :return: A new dataframe with the serialized column
    """
    subtype = type(dtype.elementType)
    if subtype is stypes.StringType:
        func = lambda x: F.concat(
            F.lit('"'),
            F.trim(
                F.regexp_replace(
                    F.regexp_replace(x, '"', r'\\\\"'), "([\t\r\n])", r"\\$1"
                )
            ),
            F.lit('"'),
        )
    elif subtype is stypes.StructType:
        # will create a function to convert each element of the struct
        funcs = {}
        if struct_columns_to_use:
            use_all_subfields = False
            subfields_to_use = set(struct_columns_to_use)
        else:
            use_all_subfields = True
            struct_columns_to_use = []
        for subfield in dtype.elementType.fields:
            if use_all_subfields or subfield.name in subfields_to_use:
                if use_all_subfields:
                    struct_columns_to_use.append(subfield.name)
                subfield_type = type(subfield.dataType)
                subfield_name = subfield.name
                if subfield_type is stypes.StringType:
                    func = lambda y, subfield: F.when(
                        y[subfield].isNull(), ""
                    ).otherwise(
                        F.concat(
                            F.lit(r'\\"'),
                            F.regexp_replace(
                                F.regexp_replace(y[subfield], '"', r'\\"'),
                                "([\t\r\n])",
                                r"\\\\$1",
                            ),
                            F.lit(r'\\"'),
                        )
                    )
                elif subfield_type is stypes.StructType:
                    raise NotImplementedError(
                        f"Nested struct {subfield_name} is not supported."
                    )
                elif subfield_type is stypes.ArrayType:
                    func = lambda y, subfield: F.when(
                        y[subfield].isNull(), ""
                    ).otherwise(
                        F.concat(
                            F.lit(r'\\"{'),
                            F.array_join(
                                F.transform(
                                    y[subfield],
                                    lambda z: F.when(z.isNull(), "").otherwise(
                                        F.regexp_replace(
                                            F.regexp_replace(z, '"', r'\\"'),
                                            "([\t\r\n])",
                                            r"\\$1",
                                        )
                                    ),
                                ),
                                ",",
                            ),
                            F.lit(r'}\\"'),
                        )
                    )
                elif subfield_type is stypes.BooleanType:
                    func = (
                        lambda y, subfield: F.when(y[subfield].isNull(), "")
                        .when(y[subfield] == True, "t")
                        .otherwise("f")
                    )
                else:
                    # should just be some type that doesn't need escaping
                    func = lambda x: x[subfield_name]
                funcs[subfield.name] = func
        func = lambda x: F.concat(
            F.lit('"('),
            F.concat_ws(
                ",",
                *[funcs[subfield](x, subfield) for subfield in struct_columns_to_use],
            ),
            F.lit(')"'),
        )
    else:
        # identity function should be sufficient
        func = lambda x: F.when(x.isNull(), "").otherwise(x)
    return self.withColumn(
        new_column,
        F.when(
            F.size(F.col(column)) > 0,
            F.concat(
                F.lit("{"),
                F.array_join(F.transform(F.col(column), func), ","),
                F.lit("}"),
            ),
        ).otherwise(r"\N"),
    )


DataFrame.serialize_array_field = serialize_array_field


def serialize_bool_field(self: DataFrame, column: str, new_column: str) -> DataFrame:
    """
    Serializes a :class:`pyspark.sql.types.BooleanType` field for output.

    :param self: The dataframe to use
    :param column: The name of the column to serialize
    :param new_column: The name to give the new serialized column
    :return: A new dataframe with the serialized column
    """
    return self.withColumn(
        new_column,
        F.when(F.col(column).isNull(), r"\N")
        .when(F.col(column) == True, "t")
        .otherwise("f"),
    )


DataFrame.serialize_bool_field = serialize_bool_field


def serialize_string_field(self: DataFrame, column: str, new_column: str) -> DataFrame:
    """
    Serializes a :class:`pyspark.sql.types.StringType` field for output.

    :param self: The dataframe to use
    :param column: The name of the column to serialize
    :param new_column: The name to give the new serialized column
    :return: A new dataframe with the serialized column
    """
    # escape \t, \n, and \r
    return self.withColumn(
        new_column,
        F.when(F.col(column).isNull(), r"\N").otherwise(
            F.trim(F.regexp_replace(column, "([\t\r\n])", r"\\$1"))
        ),
    )


DataFrame.serialize_string_field = serialize_string_field


def serialize_struct_field(
    self: DataFrame,
    column: str,
    new_column: str,
    dtype: stypes.StructType,
    struct_columns_to_use: Optional[Container] = None,
) -> DataFrame:
    """
    Serializes a :class:`pyspark.sql.types.StructType` field for output.

    :param self: The dataframe to use
    :param column: The name of the column to serialize
    :param new_column: The name to give the new serialized column
    :param dtype: The column definition
    :param struct_columns_to_use: A set of struct values to use (assuming column
        is a struct)
    :raises NotImplementedError: If the type in the array is an array or struct
    :return: A new dataframe with the serialized column
    """
    # will create a function to convert each element of the struct
    funcs = {}
    if struct_columns_to_use:
        use_all_subfields = False
        subfields_to_use = set(struct_columns_to_use)
    else:
        use_all_subfields = True
        struct_columns_to_use = []
    for subfield in dtype.fields:
        if use_all_subfields or subfield.name in subfields_to_use:
            if use_all_subfields:
                struct_columns_to_use.append(subfield.name)
            subfield_type = type(subfield.dataType)
            subfield_name = f"{column}.{subfield.name}"
            if subfield_type is stypes.StringType:
                func = F.when(F.col(subfield_name).isNull(), "NULL").otherwise(
                    F.concat(
                        F.lit('"'),
                        F.regexp_replace(
                            F.regexp_replace(subfield_name, '"', '""'),
                            "([\t\r\n])",
                            r"\\$1",
                        ),
                        F.lit('"'),
                    )
                )
            elif subfield_type is stypes.ArrayType:
                # Handle ArrayType within the struct
                element_type = type(subfield.dataType.elementType)
                subfield_col = F.col(subfield_name)
                # Define how to serialize each element in the array
                if element_type is stypes.StringType:
                    element_func = lambda x: F.when(x.isNull(), "").otherwise(
                        F.regexp_replace(
                            F.regexp_replace(x, '"', '""'),
                            "([\t\r\n])",
                            r"\\$1",
                        )
                    )
                elif element_type is stypes.BooleanType:
                    element_func = (
                        lambda x: F.when(x.isNull(), "")
                        .when(x == True, "t")
                        .otherwise("f")
                    )
                else:
                    # For other types, no escaping needed
                    element_func = lambda x: F.when(x.isNull(), "").otherwise(x)
                func = F.when(subfield_col.isNull(), "NULL").otherwise(
                    F.concat(
                        F.lit('"'),
                        F.lit("{"),
                        F.array_join(F.transform(subfield_col, element_func), ","),
                        F.lit("}"),
                        F.lit('"'),
                    )
                )
            elif subfield_type is stypes.StructType:
                raise NotImplementedError(
                    f"Nested struct {subfield_name} is not supported."
                )
            elif subfield_type is stypes.BooleanType:
                func = (
                    F.when(F.col(subfield_name).isNull(), "NULL")
                    .when(F.col(subfield_name) == True, "t")
                    .otherwise("f")
                )
            else:
                # should just be some type that doesn't need escaping
                func = F.when(F.col(subfield_name).isNull(), "NULL").otherwise(
                    F.col(subfield_name)
                )
                if subfield_type is stypes.StringType:
                    func = F.trim(func)
            funcs[subfield.name] = func
    return self.withColumn(
        new_column,
        F.when(F.col(column).isNull(), r"\N").otherwise(
            F.concat(
                F.lit("("),
                F.concat_ws(
                    ",", *[funcs[subfield] for subfield in struct_columns_to_use]
                ),
                F.lit(")"),
            ),
        ),
    )


DataFrame.serialize_struct_field = serialize_struct_field


def serialize_field(
    self: DataFrame,
    column: str,
    new_column: Optional[str] = None,
    struct_columns_to_use: Optional[Container] = None,
) -> DataFrame:
    """
    Operates on a PySpark dataframe and converts any field of either atoms or
    structs, or any array of either of those (but not nested) to the properly
    formatted string for postgresql TEXT loading format and assigns it the
    column name new_column.
    If ``new_column`` is not specified, the original column will be overwritten.
    N.B. All string types should be :class:`pyspark.sql.types.StringType` as
    opposed to :class:`pyspark.sql.typesCharType` or
    :class:`pyspark.sql.types.VarcharType`.

    :param self: The dataframe to use
    :param column: The name of the column to serialize
    :param new_column: The name to give the new serialized column
    :param struct_columns_to_use: A set of struct values to use (assuming column
        is a struct)
    :return: A new dataframe replacing original column with a serialized one
    """
    if new_column:
        drop = ""
        tmp_column = new_column
    else:
        drop = column
        tmp_column = f"{column}_tmp"
        new_column = column
    dtype = dict(zip(self.schema.names, self.schema.fields))[column].dataType
    field_type = type(dtype)
    if field_type is stypes.ArrayType:
        df = self.serialize_array_field(
            column, tmp_column, dtype, struct_columns_to_use
        )
    elif field_type is stypes.BooleanType:
        df = self.serialize_bool_field(column, tmp_column)
    elif field_type is stypes.StringType:
        df = self.serialize_string_field(column, tmp_column)
    elif field_type is stypes.StructType:
        df = self.serialize_struct_field(
            column, tmp_column, dtype, struct_columns_to_use
        )
    else:
        # add more conversions here for other data types if needed
        df = self.withColumn(
            tmp_column, F.when(F.col(column).isNull(), r"\N").otherwise(F.col(column))
        )
    return df.drop(drop).withColumnRenamed(tmp_column, new_column)


DataFrame.serialize_field = serialize_field
