"""
Spark-related functionality.
"""
import logging
import os
import pgtoolkit.pgpass
import pyspark.sql
import re
import shutil
from collections.abc import Callable, Container, Iterable, Mapping
from functools import partial
from glob import glob
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

    :param cores: The number of cores to configure PySpark with, defaults to
        cfg["spark"]["cores"]
    :type cores: int or str or None
    :param str memory: The amount of memory to configure PySpark with, defaults
        to cfg["spark"]["memory"]
    :param bool use_db: Configure PySpark to be able to query a database via
        JDBC, defaults to False
    :param bool use_excel: Configure PySpark to be able to parse Excel
        spreadsheets, defaults to False
    :param bool use_glow: Configure PySpark to use glow (e.g. to parse a VCF),
        defaults to False
    :param bool use_xml: Configure PySpark to be able to parse XML files,
        defaults to False
    :param bool show_console_progress: Configure PySpark to show console
        progress, default to True
    :param extra_options: Any additional options to configure PySpark with,
        defaults to None
    :type extra_options: Iterable or None
    :param extra_packages: Any additional packages for PySpark to load, defaults
        to None
    :type extra_packages: Iterable or None
    :param str postgresql_jdbc: The path to the PostgreSQL JDBC jar for use if
        use_db is specified, defaults to cfg["spark"]["jdbc"]
    :param str excel_package: The name of the package PySpark needs to parse
        Excel spreadsheets, defaults to cfg["spark"]["packages"]["excel"]
    :param str glow_codec: The name of the codec PySpark needs to load glow,
        defaults to cfg["spark"]["codecs"]["glow"]
    :param str glow_package: The name of the package PySpark needs to load
        glow, defaults to cfg["spark"]["packages"]["glow"]
    :param str xml_package: The name of the package PySpark needs to parse XML
        files, defaults to cfg["spark"]["packages"]["xml"]
    :param spark_logging_level: The logging level to configure py4j and pyspark
        with, defaults to logging.ERROR
    :type spark_logging_level: int or str or None
    :return: The configured PySpark session
    :rtype: SparkSession
    """
    logging.getLogger("py4j").setLevel(spark_logging_level)
    logging.getLogger("pyspark").setLevel(spark_logging_level)
    spark = SparkSession.builder.config("spark.driver.memory", memory)
    if not show_console_progress:
        spark = spark.config("spark.ui.showConsolePorgress", "false")
    if extra_options:
        for option, value in extra_options:
            spark = spark.option(option, value)
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
    path: str,
    format: str = "parquet",
    load_config_options: Optional[Iterable[tuple[str, str]]] = None,
    **kwargs,
) -> DataFrame:
    """
    Load and return the specified data source using PySpark

    :param SparkSession self: The PySpark session to use
    :param str path: The path to the data source to load
    :param str format: The format of the data source, defaults to "parquet"
    :param load_config_options: Any additonal config options to load data,
        defaults to None
    :type load_config_options: Iterable or None
    :params **kwargs: Any additional named arguments
    :return: The dataframe requested
    :rtype: DataFrame
    """
    load_func = self.read.format(format)
    if load_config_options:
        for option, value in load_config_options:
            logger.debug(f"Setting {option} to {value}.")
            load_func = load_func.option(option, value)
    logger.info(f"Loading {path} with format {format}.")
    return load_func.load(path, **kwargs)


SparkSession.load_dataframe = load_dataframe


def load_xml_to_dataframe(
    self: SparkSession,
    xml_fn: str,
    row_tag: str,
    schema: Optional[str] = None,
    load_config_options: Optional[Iterable[tuple[str, str]]] = None,
    **kwargs,
) -> DataFrame:
    """
    Load and return the specified XML file with PySpark

    :param SparkSession self: The PySpark session to use
    :param str xml_fn: The path to the data source to load
    :param str row_tag: The XML tag that delimits records
    :param schema: The path to an optional XSD schema to validate records,
        defaults to None
    :type schema: str or None
    :param load_config_options: Any additonal config options to load data,
        defaults to None
    :type load_config_options: Iterable or None
    :param **kwargs: Any additional named arguments
    :return: The dataframe requested
    :rtype: DataFrame
    """
    if load_config_options is None:
        load_config_options = []
    load_config_options.append(("rowTag", row_tag))
    if schema:
        load_config_options.append(("rowValidationPath", schema))
    return self.load_dataframe(
        xml_fn,
        format="xml",
        load_config_options=load_config_options,
        **kwargs,
    )


SparkSession.load_xml_to_dataframe = load_xml_to_dataframe


def load_db_to_dataframe(
    self: SparkSession,
    pgpass_record: pgtoolkit.pgpass.PassEntry = None,
    relation: Optional[str] = None,
    query: Optional[str] = None,
    load_config_options: Optional[Iterable[tuple[str, str]]] = None,
    **kwargs,
) -> DataFrame:
    """
    Return a PySpark dataframe from either a relation or query

    :param SparkSession self: The PySpark session to use
    :param pgpass_record: PostgreSQL login credentials, defaults to getting the
        data from get_pg_config()
    :type pgpass_record: pgtoolkit.pgpass.PassEntry or None
    :param relation: The database relation to load, defaults to None
    :type relation: str or None
    :param query: The database query to load, defaults to None
    :type query: str or None
    :param load_config_options: Any additonal config options to load data,
        defaults to None
    :type load_config_options: Iterable or None
    :param **kwargs: Any additional named arguments
    :return: The dataframe requested
    :rtype: DataFrame
    """
    if relation is None == query is None:
        raise ValueError(f"Specify relation or query and not both.")
    load_func = self.read.format("jdbc")
    if not pgpass_record:
        pgpass_record = get_pg_config()
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
    """
    Load and return the specified Excel spreadsheet with PySpark

    :param SparkSession self: The PySpark session to use
    :param str xl_fn: The path to the data source to load
    :param bool header: Whether the data source has a header or not, defaults to
        True
    :param load_config_options: Any additonal config options to load data,
        defaults to None
    :type load_config_options: Iterable or None
    :param **kwargs: Any additional named arguments
    :return: The dataframe requested
    :rtype: DataFrame
    """
    if load_config_options is None:
        load_config_options = []
    load_config_options.append(("header", "true" if header else "false"))
    return self.load_spark_dataframe(
        xl_fn,
        format="com.crealytics.spark.excel",
        load_config_options=load_config_options,
        **kwargs,
    )


SparkSession.load_excel_to_dataframe = load_excel_to_dataframe


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

    :param DataFrame self: The dataframe to use
    :param str column_to_split_on: The column name to split on
    :param str output_directory: The directory to output to
    :param str filename_format: Filename convention to use; use {column_value} to
        refer to the split column's value. Defaults to "{column_value}"
    :param bool include_header: Whether to include a header in each output file,
        defaults to True
    :param bool overwrite: Overwrite existing directory it if already exists,
        defaults to True
    :param bool include_split_column_in_new_files: Whether to include the column
        that was used to split in the resulting files, defaults to False
    """
    if include_header:
        cols = self.columns
        cols.remove(column_to_split_on)
        if include_split_column_in_new_files:
            cols.insert(0, column_to_split_on)
        header = ",".join(cols) + "\n"
    regex = re.compile(rf"^{column_to_split_on}=(.+)$")
    if os.path.isdir(output_directory):
        if overwrite:
            shutil.rmtree(output_directory)
    if not os.path.isdir(output_directory):
        os.makedirs(output_directory)
    with TemporaryDirectory() as d:
        dir_name = os.path.join(d, "partitions")
        self.write.partitionBy(column_to_split_on).csv(dir_name)
        partitions = sorted(glob(f"{column_to_split_on}=*", root_dir=dir_name))
        logger.info("Found the following partitions:\n" + "\n".join(partitions))
        for partition in partitions:
            column_value = regex.match(partition).group(1)
            output_filename = os.path.join(
                output_directory,
                filename_format.format(column_value=column_value) + ".csv.gz",
            )
            logger.debug(
                f"Writing split {column_to_split_on}={column_value} to {output_filename}."
            )
            with zopen(f"!gzip -9 > {output_filename}", "w") as out_fh:
                if include_header:
                    out_fh.write(header)
                for in_fn in sorted(
                    glob(os.path.join(dir_name, partition, "part-*.csv"))
                ):
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
    Return a PySpark dataframe with current_column_name (containing human
    chromosomes) with a new column, new_column_name (defaulting to overwriting
    the original), with the chromosome cast as an integer.

    :param DataFrame self: The dataframe to use
    :param str current_column_name: The column name to cast
    :param new_column_name: The new column name to use
    :type new_column_name: str or None
    :return: The processed dataframe
    :rtype: DataFrame
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

    :param DataFrame self: The dataframe to summarize
    :return: The new dataframe with null counts per column
    :rtype: DataFrame
    """
    return self.select(
        [F.count(F.when(F.isnull(col), col)).alias(col) for col in self.columns]
    )


DataFrame.count_nulls = count_nulls


def count_columns_with_string(self: DataFrame, string: str = "|") -> DataFrame:
    """
    Return a PySpark dataframe with the number of times a given string occurs
    in each string column in a dataframe

    :param DataFrame self: The dataframe to summarize
    :param str string: The string to search for, defaults to "|"
    :return: The new dataframe with counts per column
    :rtype: DataFrame
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
    column.  Uses count_distinct by default and approx_count_distinct if
    approximate == True

    :param DataFrame self: The dataframe to summarize
    :param Container columns_to_ignore: An optional set of columns to not
        summarize, defaults to set()
    :param bool approximate: Get approximate counts instead of exact (faster),
        defaults to False
    :return: The new dataframe with counts of distinct values per column
    :rtype: DataFrame
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
    Converts either a list of dicts (dict_list) or a function that returns an
    iterator of dicts (iter_func) to a PySpark dataframe

    :param SparkSession self: The SparkSession to use
    :param dict_list: An list of dicts representing rows, defaults to None
    :type dict_list: Iterable or None
    :param iter_func: A function that returns an iterator of dicts representing
        rows, defaults to None
    :type iter_func: Callable or None
    :param bool coerce_to_lists_if_needed: For any column to create, check if any
        value in a row is a list, and if so, convert any non-lists in the
        column to a list, defaults to True
    :return: A new dataframe built from the provided rows of dicts
    :rtype: DataFrame
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
    Serializes an ArrayType field for output.

    :param DataFrame self: The dataframe to use
    :param str column: The name of the column to serialize
    :param str new_column: The name to give the new serialized column
    :param pyspark.sql.types.ArrayType dtype: The column definition
    :param struct_columns_to_use: A set of struct values to use (assuming column
        is a struct), defaults to None
    :type struct_columns_to_use: Container or None
    :raises NotImplementedError: If the type in the array is a nested struct
    :return: A new dataframe with the serialized column
    :rtype: DataFrame
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
                    raise NotImplementedError(
                        f"Nested array in struct {subfield_name} is not supported."
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
    Serializes a BooleanType field for output.

    :param DataFrame self: The dataframe to use
    :param str column: The name of the column to serialize
    :param new_column: The name to give the new serialized column
    :return: A new dataframe with the serialized column
    :rtype: DataFrame
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
    Serializes a StringType field for output.

    :param DataFrame self: The dataframe to use
    :param str column: The name of the column to serialize
    :param new_column: The name to give the new serialized column
    :return: A new dataframe with the serialized column
    :rtype: DataFrame
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
    Serializes a StructType field for output.

    :param DataFrame self: The dataframe to use
    :param str column: The name of the column to serialize
    :param new_column: The name to give the new serialized column
    :param pyspark.sql.types.StructType dtype: The column definition
    :param struct_columns_to_use: A set of struct values to use (assuming column
        is a struct), defaults to None
    :type struct_columns_to_use: Container or None
    :raises NotImplementedError: If the type in the array is an array or struct
    :return: A new dataframe with the serialized column
    :rtype: DataFrame
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
                raise NotImplementedError(
                    f"Nested array in struct {subfield_name} is not supported."
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
    If new_column is not specified, the original column will be overwritten.
    N.B. All string types should be StringType as opposed to CharType or
    VarcharType.

    :param DataFrame self: The dataframe to use
    :param str column: The name of the column to serialize
    :param new_column: The name to give the new serialized column, defaults to
        None
    :type new_column: str or None
    :param struct_columns_to_use: A set of struct values to use (assuming column
        is a struct), defaults to None
    :type struct_columns_to_use: Container or None
    :return: A new dataframe replacing original column with a serialized one
    :rtype: DataFrame
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
