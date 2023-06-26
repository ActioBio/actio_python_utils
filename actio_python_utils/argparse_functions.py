"""
Code for argparse-related functionality.
"""
import _io
import argparse
import inspect
import json
import logging
import os
import pgtoolkit.pgpass
import psycopg2.extensions
import pyspark.sql
from collections.abc import Iterable
from functools import partial
from typing import Optional
from . import (
    utils,
    database_functions as udbf,
    logging_functions as ulf,
    spark_functions as usf,
)

# get docstring from main class
current_frame = inspect.currentframe()
while current_frame.f_globals["__name__"] != "__main__":
    current_frame = current_frame.f_back


class CustomFormatter(
    argparse.ArgumentDefaultsHelpFormatter, argparse.RawTextHelpFormatter
):
    """
    argparse.HelpFormatter that displays argument defaults and doesn't
    change formatting of the description
    """

    pass


def key_value_pair(arg: str, sep: str = "=") -> tuple[str, str]:
    """
    Splits a string once on sep and returns the result

    :param str arg: The string to split on
    :param str sep: The separator to split the string on, defaults to "="
    :raises ValueError: If sep does not occur in arg
    :return: The string split on sep once
    :rtype: list
    """
    if "=" in arg:
        return arg.split("=", 1)
    raise ValueError("Argument must be formatted as KEY=VALUE.")


def file_exists(fn: str) -> str:
    """
    Returns the real path to file fn if it exists

    :param str fn: The file name to check
    :raises OSError: If fn doesn't exist
    :return: The real path to fn
    :rtype: str
    """
    if os.path.isfile(fn):
        return os.path.realpath(fn)
    else:
        raise OSError(f"{fn} does not exist.")


def dir_exists(dirn: str) -> str:
    """
    Returns the real path to directory dirn if it exists

    :param str dirn: The directory name to check
    :raises OSError: If dirn doesn't exist
    :return: The real path to dirn
    :rtype: str
    """
    if os.path.isdir(dirn):
        return os.path.realpath(dirn)
    else:
        raise OSError(f"{dirn} does not exist.")


class EnhancedArgumentParser(argparse.ArgumentParser):
    """
    Customized ArgumentParser that sets description automatically, uses both
    ArgumentDefaultsHelpFormatter and RawTextHelpFormatter formatters,
    optionally sets up logging, database, and Spark connections.

    :param *args: Optional positional arguments passed to
        argparse.ArgumentParser constructor
    :param str description: Passed to argparse.ArgumentParser constructor,
        defaults to current_frame.f_globals.get("__doc__", "")
    :param argparse.HelpFormatter formatter_class: The help formatter to use,
        defaults to CustomFormatter
    :param bool use_logging: Adds log level and log format arguments, then sets up
        parsing when parse_args() is called, defaults to False
    :param bool use_database: Adds a database service argument, then creates a
        connection to the specified database with the attribute name db when
        parse_args() is called, defaults to False
    :param bool use_spark: Adds spark cores, spark memory, and spark config
        arguments, then creates a PySpark session with the attribute name spark
        when parse_args() is called, defaults to False
    :param bool use_xml: Adds dependencies to PySpark to parse XML files; sets
        use_spark = True, defaults to False
    :param bool use_glow: Adds dependencies to PySpark to use glow, e.g. to parse
        VCF files; sets use_spark = True, defaults to False
    :param bool use_spark_db: Adds dependencies to PySpark to connect to a
        database; sets use_spark = True and creates an object to create a
        database connection with PySpark with the attribute name spark_db when
        parse_args() is called, defaults to False
    :param bool dont_create_db_connection: Don't create a database connection
        even if use_database = True
    :param spark_extra_packages: Adds additional Spark package dependencies to
        initialize; sets use_spark = True
    :type spark_extra_packages: Iterable or None
    :param **kwargs: Any additional named arguments
    """

    def __init__(
        self,
        *args,
        description: str = current_frame.f_globals.get("__doc__", ""),
        formatter_class: argparse.HelpFormatter = CustomFormatter,
        use_logging: bool = False,
        use_database: bool = False,
        use_spark: bool = False,
        use_xml: bool = False,
        use_glow: bool = False,
        use_spark_db: bool = False,
        dont_create_db_connection = False,
        spark_extra_packages: Optional[Iterable[tuple[str, str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(
            description=description, formatter_class=formatter_class, **kwargs
        )
        self.use_logging = use_logging
        self.use_database = use_database
        self.use_spark = any(
            [use_spark, use_xml, use_glow, use_spark_db, spark_extra_packages]
        )
        self.use_xml = use_xml
        self.use_glow = use_glow
        self.use_spark_db = use_spark_db
        self.dont_create_db_connection = dont_create_db_connection
        self.spark_extra_packages = spark_extra_packages
        if self.use_logging:
            self.add_log_level_argument()
            self.add_log_format_argument()
        if self.use_database or self.use_spark_db:
            self.add_db_service_argument()
        if self.use_spark:
            self.add_spark_cores_argument()
            self.add_spark_memory_argument()
            self.add_spark_config_argument()

    @staticmethod
    def sanitize_argument(long_arg: str) -> str:
        """
        Converts the argument name to the variable actually used

        :param str long_arg: The argument name
        :return: The reformatted argument
        :rtype: str
        """
        return long_arg.lstrip("-").replace("-", "_")

    def add_argument(
        self,
        short_arg: Optional[str] = None,
        long_arg: Optional[str] = None,
        *args,
        **kwargs,
    ) -> None:
        """
        Adds an argument while retaining metavar instead of dest in help
        message

        :param short_arg: The short argument name, defaults to None
        :type short_arg: str or None
        :param long_arg: The long argument name, defaults to None
        :type long_arg: str or None
        :param *args: Any additional positional arguments
        :param **kwargs: Any additional named arguments
        """
        call = partial(
            super().add_argument,
            *[arg for arg in [short_arg, long_arg] if arg],
            *args,
            **kwargs,
        )
        if kwargs.get("action") in ("help", "store_true"):
            call()
        else:
            call(
                metavar=EnhancedArgumentParser.sanitize_argument(
                    utils.coalesce(long_arg, short_arg, "")
                )
            )

    def parse_args(
        self,
        *args,
        db_connection_name: str = "db",
        spark_name: str = "spark",
        spark_db_name: str = "spark_db",
        **kwargs,
    ) -> argparse.Namespace:
        """
        Parses arguments while optionally setting up logging, database, and/or
        PySpark.

        :param *args: Any additional positional arguments
        :param str db_connection_name: The args attribute name to give to a
            created database connection, defaults to "db"
        :param str spark_name: The args attribute name to give to a created
            PySpark session, defaults to "spark"
        :param str spark_db_name: The args attribute name to give to PostgreSQL
            login credentials for use with PySpark, defaults to "spark_db"
        :param **kwargs: Any additional named arguments
        :return: Parsed arguments, additionally with attribute db as a
            database connection if use_database = True, with attribute spark
            if use_spark = True, and attribute spark_db if use_spark_db = True
        :rtype: argparse.Namespace
        """
        args = super().parse_args(*args, **kwargs)
        if self.use_logging:
            self.setup_logging(args)
        if self.use_database and not self.dont_create_db_connection:
            setattr(args, db_connection_name, self.setup_database(args))
        if self.use_spark:
            spark, pgpass_record = self.setup_spark(args)
            setattr(args, spark_name, spark)
            if pgpass_record:
                setattr(args, spark_db_name, pgpass_record)
        return args

    def add_log_level_argument(
        self,
        short_arg: Optional[str] = "-l",
        long_arg: Optional[str] = "--log-level",
        default: str = utils.cfg["logging"]["level"],
        **kwargs,
    ) -> None:
        """
        Adds an argument to set the logging level, converts it to the proper
        integer, and sets dest = "log_level"

        :param short_arg: Short argument name to use, defaults to "-l"
        :type short_arg: str or None
        :param long_arg: Long argument name to use, defaults to "--log-level"
        :type long_arg: str or None
        :param str default: Default logging level value, defaults to
            utils.cfg["logging"]["level"]
        :param **kwargs: Any additional named arguments
        """
        self.add_argument(
            short_arg=short_arg,
            long_arg=long_arg,
            type=utils.DictToFunc(logging._nameToLevel),
            default=default,
            dest="log_level",
            help=f"the logging level to use (choices are: {{{','.join(logging._nameToLevel)}}})",
            **kwargs,
        )

    def add_log_format_argument(
        self,
        short_arg: Optional[str] = "-f",
        long_arg: Optional[str] = "--log-format",
        default: str = utils.cfg["logging"]["format"],
        **kwargs,
    ) -> None:
        """
        Adds an argument to set the logging format and sets dest = "log_format"

        :param short_arg: Short argument name to use, defaults to "-f"
        :type short_arg: str or None
        :param long_arg: Long argument name to use, defaults to "--log-format"
        :type long_arg: str or None
        :param str default: Default logging format, defaults to
            utils.cfg["logging"]["format"]
        :param **kwargs: Any additional named arguments
        """
        self.add_argument(
            short_arg=short_arg,
            long_arg=long_arg,
            default=default,
            dest="log_format",
            help="the logging format to use",
            **kwargs,
        )

    def add_db_service_argument(
        self,
        short_arg: Optional[str] = "-s",
        long_arg: Optional[str] = "--service",
        default: str = utils.cfg["db"]["service"],
        **kwargs,
    ):
        """
        Adds an argument to set the database service name sets dest =
        "db_service"

        :param short_arg: Short argument name to use, defaults to "-s"
        :type short_arg: str or None
        :param long_arg: Long argument name to use, defaults to "--service"
        :type long_arg: str or None
        :param str default: Default service, defaults to
            utils.cfg["db"]["service"]
        :param **kwargs: Any additional named arguments
        """
        self.add_argument(
            short_arg=short_arg,
            long_arg=long_arg,
            default=default,
            dest="db_service",
            help="PostgreSQL service name to log in with",
            **kwargs,
        )

    def add_spark_cores_argument(
        self,
        short_arg: Optional[str] = "-c",
        long_arg: Optional[str] = "--spark-cores",
        default: int | str = utils.cfg["spark"]["cores"],
        **kwargs,
    ) -> None:
        """
        Adds an argument to set the number of PySpark cores to use and sets
        dest = "spark_cores"

        :param short_arg: Short argument name to use, defaults to "-c"
        :type short_arg: str or None
        :param long_arg: Long argument name to use, defaults to "--spark-cores"
        :type long_arg: str or None
        :param default: Default cores, defaults to utils.cfg["spark"]["cores"]
        :type default: int or str
        :param **kwargs: Any additional named arguments
        """
        self.add_argument(
            short_arg=short_arg,
            long_arg=long_arg,
            default=default,
            dest="spark_cores",
            help="the number of cores to provide to Spark",
            **kwargs,
        )

    def add_spark_memory_argument(
        self,
        short_arg: Optional[str] = "-m",
        long_arg: Optional[str] = "--spark-memory",
        default: str = utils.cfg["spark"]["memory"],
        **kwargs,
    ) -> None:
        """
        Adds an argument to set the amount of memory to give to PySpark and
        sets dest = "spark_memory"

        :param short_arg: Short argument name to use, defaults to "-m"
        :type short_arg: str or None
        :param long_arg: Long argument name to use, defaults to "--spark-memory"
        :type long_arg: str or None
        :param str default: Default memory to use, defaults to
            utils.cfg["spark"]["memory"]
        :param **kwargs: Any additional named arguments
        """
        self.add_argument(
            short_arg=short_arg,
            long_arg=long_arg,
            default=default,
            dest="spark_memory",
            help="the amount of memory to provide to Spark",
            **kwargs,
        )

    def add_spark_config_argument(
        self,
        short_arg: Optional[str] = None,
        long_arg: Optional[str] = "--spark-config",
        **kwargs,
    ) -> None:
        """
        Adds an argument to provide 0 or more options to initialize the PySpark
        session with and sets dest = "spark_config"

        :param short_arg: Short argument name to use, defaults to None
        :type short_arg: str or None
        :param long_arg: Long argument name to use, defaults to "--spark-config"
        :type long_arg: str or None
        :param **kwargs: Any additional named arguments
        """
        self.add_argument(
            short_arg=short_arg,
            long_arg=long_arg,
            type=key_value_pair,
            dest="spark_config",
            action="append",
            help="any additional config options to pass to spark (format is KEY=VALUE)",
            **kwargs,
        )

    def add_spark_load_config_argument(
        self,
        short_arg: Optional[str] = None,
        long_arg: Optional[str] = "--spark-load-config",
        **kwargs,
    ) -> None:
        """
        Adds an argument to provide 0 or more options to load a dataframe
        in PySpark with and sets dest = "spark_load_config"

        :param short_arg: Short argument name to use, defaults to None
        :type short_arg: str or None
        :param long_arg: Long argument name to use, defaults to
            "--spark-load-config"
        :type long_arg: str or None
        :param **kwargs: Any additional named arguments
        """
        self.add_argument(
            short_arg=short_arg,
            long_arg=long_arg,
            type=key_value_pair,
            dest="spark_load_config",
            action="append",
            help="any options required to load the the data (format is KEY=VALUE)",
            **kwargs,
        )

    def setup_logging(
        self,
        args: argparse.Namespace,
        name: str = "root",
        stream: Optional[_io.TextIOWrapper] = None,
        stream_handler_logging_level: Optional[str | int] = None,
    ) -> None:
        """
        Sets up logging with ulf.setup_logging and specified log level and
        format

        :param argparse.Namespace args: Parsed arguments from parse_args()
        :param str name: Logger name to initialize, defaults to "root"
        :param stream: Stream to log to, defaults to None
        :type stream: _io.TextIOWrapper or None
        :param stream_handler_logging_level: Logging level to use for stream,
            defaults to None
        :type stream_handler_logging_level: str or int or None
        """
        ulf.setup_logging(
            logging_level=args.log_level
            if "log_level" in args
            else utils.cfg["logging"]["level"],
            name=name,
            stream=stream,
            stream_handler_logging_level=stream_handler_logging_level,
            format_string=args.log_format
            if "log_format" in args
            else utils.cfg["logging"]["format"],
        )

    def setup_database(
        self, args: argparse.Namespace
    ) -> psycopg2.extensions.connection:
        """
        Returns a psycopg2 connection to the database specified in
        args.db_service

        :param argparse.Namespace args: Parsed arguments from parse_args()
        :return: The psycopg2 connection
        :rtype: psycopg2.extensions.connection
        """
        return udbf.connect_to_db(
            args.db_service if "db_service" in args else utils.cfg["db"]
        )

    def setup_spark(
        self, args: argparse.Namespace
    ) -> tuple[pyspark.sql.session.SparkSession, pgtoolkit.pgpass.PassEntry]:
        """
        Returns a list with a created PySpark session and optionally a
        PostgreSQL login record if use_spark_db = True

        :param argparse.Namespace args: Parsed arguments from parse_args()
        :return: A list with the created PySpark session and either a
            pgtoolkit.pgpass.PassEntry record or None
        :rtype: list
        """
        return_value = []
        return_value.append(
            usf.setup_spark(
                cores=args.spark_cores
                if "spark_cores" in args
                else utils.cfg["spark"]["cores"],
                memory=args.spark_memory
                if "spark_memory" in args
                else utils.cfg["spark"]["memory"],
                use_xml=self.use_xml,
                use_glow=self.use_glow,
                use_db=self.use_spark_db,
                extra_options=args.spark_config if "spark_config" in args else None,
                extra_packages=self.spark_extra_packages,
            )
        )
        return_value.append(
            udbf.get_pg_config(
                args.db_service if "db_service" in args else utils.cfg["db"]["service"]
            )
            if self.use_spark_db
            else None
        )
        return return_value
