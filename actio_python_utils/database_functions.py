"""
Database-related functionality.
"""
import logging
import os
import pgtoolkit.pgpass
import pgtoolkit.service
import psycopg2
from collections.abc import Callable, Hashable, Iterable, Mapping, MutableMapping
from contextlib import contextmanager, nullcontext
from functools import wraps
from psycopg2.extras import DictCursor
from types import TracebackType
from typing import Any, Optional
from .utils import cfg, get_csv_fields, rename_dict_keys, zopen

logger = logging.getLogger(__name__)


def get_pg_config(
    service: str = cfg["db"]["service"],
    service_fn: str = pgtoolkit.service.find(),
    pgpass_fn: str = os.path.join(os.path.expanduser("~"), ".pgpass"),
) -> pgtoolkit.pgpass.PassEntry:
    """
    Locates the PostgreSQL login credentials given a service name

    :param str service: The PostgreSQL service name to get, defaults to
        cfg["db"]["service"]
    :param str service_fn: The path to the file containing service definitions,
        defaults to pgtoolkit.service.find()
    :param str pgpass_fn: The path to the file containing database login data,
        defaults to os.path.join(os.path.expanduser("~"), ".pgpass")
    :return: The database login credentials corresponding to the given service
    :rtype: pgtoolkit.pgpass.PassEntry
    """
    if not os.path.isfile(service_fn):
        raise OSError(f"Service file {service_fn} does not exist.")
    if not os.path.isfile(pgpass_fn):
        raise OSError(f"pgpass file {pgass_fn} does not exist.")
    services = pgtoolkit.service.parse(service_fn)
    try:
        service_record = services[service]
    except:
        raise KeyError(f"Service {service} not found.")
    service_dict = rename_dict_keys(
        service_record,
        (
            ("dbname", "database"),
            ("host", "hostname"),
            ("name", None),
            ("user", "username"),
        ),
    )
    pgpass = pgtoolkit.pgpass.parse(pgpass_fn)
    for pgpass_record in pgpass.lines:
        if pgpass_record.matches(**service_dict):
            return pgpass_record
    else:
        raise ValueError("Didn't find the matching service.")


@contextmanager
def savepoint(cur: psycopg2.extensions.cursor, savepoint: str = "savepoint") -> None:
    """
    Creates a context manager to create a savepoint upon entering, rollback if
    an error occurs, and release upon exiting

    :param psycopg2.extensions.cursor cur: The psycopg2 cursor query to use
    :param str savepoint: The name to give to the savepoint, defaults to
        "savepoint"
    """
    cur.execute(f"SAVEPOINT {savepoint}")
    try:
        yield
    except:
        cur.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
        raise
    finally:
        cur.execute(f"RELEASE SAVEPOINT {savepoint}")


def savepoint_wrapper(
    method: Callable[[psycopg2.extensions.cursor, ...], Any]
) -> Callable[[psycopg2.extensions.cursor, ...], Any]:
    """
    Wraps a psycopg2 cursor's method to use a savepoint

    :param Callable method: The method to wrap
    :return: The wrapped method
    :rtype: Callable
    """

    @wraps(method)
    def inner(obj, *args, dry_run=False, **kwargs):
        cm = nullcontext() if dry_run else savepoint(super(type(obj), obj))
        with cm:
            return method(obj, *args, **kwargs)

    return inner


class LoggingCursor(DictCursor):
    """
    Wraps a DictCursor such that each copy_expert and execute statement is
    logged

    :param *args: Any positional arguments to pass to the DictCursor constructor
    :param log_level: The logging level to use, defaults to
        cfg["logging"]["level"]
    :type log_level: int or str
    :param str log_name: The name to give the logger, defaults to
        cfg["logging"]["names"]["db"]
    :param **kwargs: Any named arguments to pass to the DictCursor constructor
    """

    def __init__(
        self,
        *args,
        log_level: int | str = cfg["logging"]["level"],
        log_name: str = cfg["logging"]["names"]["db"],
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        if not isinstance(log_level, int):
            log_level = logging._nameToLevel[log_level]
        self.log_level = log_level
        self.logger = logging.getLogger(log_name)

    def _log_statement(self, statement: str, dry_run: bool = False) -> None:
        """
        Log the given statement with a prefix pertaining to whether it's a dry
        run or not

        :param str statement: The SQL statement to log
        :param bool dry_run: Do a dry run, defaults to False
        """
        if dry_run:
            self.logger.log(self.log_level, "Would execute statement:\n" + statement)
        else:
            self.logger.log(self.log_level, "Executing statement:\n" + statement)

    def copy_expert(self, sql: str, file: str, *args, dry_run=False, **kwargs) -> None:
        """
        Logs the sql statement and executes it if dry_run = False

        :param str sql: The SQL statement to execute
        :param str file: The path to the file to import
        :param *args: Any positional arguments
        :param bool dry_run: Do a dry run, defaults to False
        :param **kwargs: Any named arguments
        """
        self._log_statement(sql, dry_run)
        if not dry_run:
            super().copy_expert(sql, file, *args, **kwargs)

    def execute(
        self,
        query: str,
        vars: Iterable | Mapping = None,
        *args,
        dry_run: bool = False,
        **kwargs,
    ) -> None:
        """
        Logs the query and executes it if dry_run = False

        :param str query: The SQL query to execute
        :param vars: Variables to bind to the query, defaults to None
        :type vars: Iterable or Mapping or None
        :param *args: Any positional arguments
        :param bool dry_run: Do a dry run, defaults to False
        :param **kwargs: Any named arguments
        """
        self._log_statement(self.mogrify(query, vars).decode(), dry_run)
        if not dry_run:
            super().execute(query, vars, *args, **kwargs)


class SavepointCursor(LoggingCursor):
    """
    Wraps LoggingCursor methods to use a savepoint context manager
    """

    @savepoint_wrapper
    def copy_expert(self, *args, **kwargs) -> None:
        super().copy_expert(*args, **kwargs)

    @savepoint_wrapper
    def copy_from(self, *args, **kwargs) -> None:
        super().copy_from(*args, **kwargs)

    @savepoint_wrapper
    def copy_to(self, *args, **kwargs) -> None:
        super().copy_to(*args, **kwargs)

    @savepoint_wrapper
    def execute(self, *args, **kwargs) -> None:
        super().execute(*args, **kwargs)


def replace_dict_values_with_global_definition(
    mapping: MutableMapping[Hashable, Any],
    global_mapping: Mapping[Hashable, Any] = globals(),
) -> MutableMapping[Hashable, Any]:
    """
    Replaces values in the mapping with those in global_mapping when the value
    is a str and it is a key in the global mapping

    :param MutableMapping mapping: The mapping for which to replace values
    :param Mapping global_mapping: The mapping from which to get replacement
        values, defaults to globals()
    :return: Updated mapping replacing (key, value) with
        (key, global_mapping[value]) for appropriate keys
    :rtype: MutableMapping
    """
    for key, value in mapping.items():
        if isinstance(value, str) and value in global_mapping:
            mapping[key] = global_mapping[value]


# replace cursor_factory with the value defined here
replace_dict_values_with_global_definition(cfg["db"])


class DBConnection(object):
    """
    Creates a psycopg2 database connection with the specified parameters and
    acts as a context manager

    :param service: The PostgreSQL service to connect to
    :type service: str or None
    :param db_args: A mapping of database connection arguments, defaults to
        cfg["db"]
    :type db_args: Mapping or None
    :param str log_name: The name to give the logger, defaults to
        cfg["logging"]["names"]["db"]
    :param bool commit: Commit the transaction upon closing the connection if no
        errors were encountered, defaults to False
    """

    def __init__(
        self,
        service: Optional[str] = None,
        db_connection_string: Optional[str] = None,
        db_args: Optional[Mapping[str, Any]] = cfg["db"],
        log_name: str = cfg["logging"]["names"]["db"],
        commit: bool = False,
    ) -> None:
        if service:
            self.db_args = {"service": service, "cursor_factory": LoggingCursor}
        else:
            self.db_args = db_args
        self.commit = commit
        self.logger = logging.getLogger(log_name)
        self.kwargs = db_args

    def connect(self) -> None:
        """
        Connects to the database and creates attributes db and cursor for the
        connection and a cursor, respectively
        """
        self.logger.debug(f"Connecting to DB with parameters: {self.db_args}.")
        self.db = psycopg2.connect(**self.db_args)
        self.cur = self.db.cursor()

    def disconnect(self, exception: bool) -> None:
        """
        Disconnect from the database and commit if self.commit = True and no
        exception has been raised

        :param bool exception: Whether an exception has been raised or not
        """
        if not self.db.closed:
            if self.commit and not exception:
                self.logger.info("Committing changes.")
                self.db.commit()
            self.db.close()

    def __enter__(self) -> psycopg2.extensions.connection:
        """
        For use as a context manager, creates and returns a database connection

        :return: The database connection
        :rtype: psycopg2.extensions.connection
        """
        self.connect()
        return self.db

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> bool:
        """
        For use as a context manager, disconnects the database connection

        :return: Whether an exception has occurred
        :rtype: bool
        """
        self.disconnect(exc_type is not None)


def connect_to_db(
    service: Optional[str] = None,
    db_args: Optional[MutableMapping[str, str]] = cfg["db"],
    **kwargs,
) -> psycopg2.extensions.connection:
    """
    Return a connection to the specified PostgreSQL database

    :param service: The PostgreSQL service to connect to
    :type service: str or None
    :param db_args: A mapping of database connection arguments, defaults to
        cfg["db"]
    :type db_args: Mapping or None
    :return: The database connection
    :rtype: psycopg2.extensions.connection
    """
    if service:
        db_args = {"service": service, "cursor_factory": LoggingCursor}
    if not db_args:
        db_args = {}
    db_args.update(kwargs)
    logger.debug(f"Connecting to DB with parameters: {db_args}.")
    return psycopg2.connect(**db_args)


def split_schema_from_table(
    table: str, default_schema: str = "public"
) -> tuple[str, str]:
    """
    Split a possibly schema qualified table name into its schema and table names

    :param str table: The possibly schema qualified table name
    :param str schema: The default schema name, defaults to "public"
    :return: A list with the schema name and the table name
    :rtype: list
    """
    if "." in table:
        return table.split(".", 1)
    else:
        return [schema, table]


def get_table_constraint_statements(
    cur: psycopg2.extensions.cursor, table_list: Iterable[str]
) -> list[str]:
    """
    Takes a psycopg2 cursor and list of tables and returns a list of all the
    SQL definitions of constraints defined on the tables.
    The constraints are ordered by keys, then other constraints on the tables,
    and lastly foreign keys defined on other tables that reference one of the
    tables specified.
    The purpose of this is to be able to drop these constraints, load data, and
    recreate them for efficiency.

    :param psycopg2.extensions.cursor cur: The psycopg2 cursor to use
    :param Iterable table_list: The list of tables to get constraints on
    :return: The list of constraints
    :rtype: list
    """
    if not table_list:
        return []
    table_list = [split_schema_from_table(table) for table in table_list]
    # order constraints to list keys first for efficiency
    cur.execute(
        """
        (
        /*  this gets constraints defined on the tables of interest a rank given by
            primary key -> rank = 0,
            unique -> rank = 1,
            others (CHECK, FOREIGN KEY) -> rank = 3
        */
            SELECT 'ALTER TABLE '||n.nspname||'."'||c.relname||'" ADD CONSTRAINT "'
                ||con.conname||'" '||pg_get_constraintdef(con.oid) AS statement,
                CASE WHEN con.contype = 'p' THEN 0
                     WHEN con.contype = 'u' THEN 1
                     ELSE 3
                END AS rank
            FROM pg_constraint con
            JOIN pg_class c ON con.conrelid = c.oid
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE {0})
        UNION
        (
        /*  this gets non-primary key indexes (because the first subquery gets
            them already) and sets rank = 2
        */
            SELECT pg_get_indexdef(c2.oid), 2 AS rank
            FROM pg_index x
            JOIN pg_class c ON x.indrelid = c.oid
            JOIN pg_class c2 ON x.indexrelid = c2.oid
            JOIN pg_namespace n ON c.relnamespace = n.oid
            LEFT JOIN pg_constraint con ON c2.relname = con.conname
                AND c2.relnamespace = con.connamespace
            WHERE NOT x.indisprimary AND con.oid IS NULL AND ({0})
        )
        UNION
        (
        /*  this gets foreign keys defined on other tables that reference the
            tables of interest and sets rank = 4
        */
            SELECT 'ALTER TABLE '||n2.nspname||'."'||c2.relname||'" ADD CONSTRAINT "'
                ||con.conname||'" '||pg_get_constraintdef(con.oid) AS statement,
                4 AS rank
            FROM pg_constraint con
            JOIN pg_class c ON con.confrelid = c.oid
            JOIN pg_namespace n ON c.relnamespace = n.oid
            JOIN pg_class c2 ON con.conrelid = c2.oid
            JOIN pg_namespace n2 ON c2.relnamespace = n2.oid
            WHERE con.contype = 'f' AND ({0}) AND NOT ({1}))
        ORDER BY rank""".format(
            " OR ".join(
                [
                    f"(n.nspname = '{schema}' AND c.relname = '{table}')"
                    for schema, table in table_list
                ]
            ),
            " OR ".join(
                [
                    f"(n2.nspname = '{schema}' AND c2.relname = '{table}')"
                    for schema, table in table_list
                ]
            ),
        )
    )
    return [r[0] for r in cur.fetchall()]


def drop_table_constraints(
    cur: psycopg2.extensions.cursor, table_list: Iterable[str], dry_run: bool = False
) -> None:
    """
    Takes a psycopg2 cursor and list of tables and finds all constraints
    defined on them, then drops them.  Foreign key constraints are processed
    first because attempting to drop a unique key on a column that is referenced
    in a foreign key results in an error.

    :param psycopg2.extensions.cursor cur: The psycopg2 cursor to use
    :param Iterable table_list: The list of tables to get constraints on
    :param bool dry_run: Do a dry run, defaults to False
    """
    table_list = [split_schema_from_table(table) for table in table_list]
    # order constraints to list foreign keys first for dropping
    cur.execute(
        """WITH t AS (SELECT n.nspname, c.relname, con.conname, con.contype
        FROM pg_constraint con
        JOIN pg_class c ON con.conrelid = c.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE {0}
        UNION
        SELECT n2.nspname, c2.relname, con.conname, con.contype
        FROM pg_constraint con
        JOIN pg_class c ON con.confrelid = c.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        JOIN pg_class c2 ON con.conrelid = c2.oid
        JOIN pg_namespace n2 ON c2.relnamespace = n2.oid
        WHERE con.contype = 'f' AND ({0}))
        SELECT nspname, relname, conname
        FROM t
        ORDER BY CASE WHEN contype = 'f' THEN 0 ELSE 1 END""".format(
            " OR ".join(
                [
                    f"(n.nspname = '{schema}' AND c.relname = '{table}')"
                    for schema, table in table_list
                ]
            )
        )
    )
    constraints = cur.fetchall()
    for nspname, relname, conname in constraints:
        cur.execute(
            f"ALTER TABLE {nspname}.{relname} DROP CONSTRAINT {conname}",
            dry_run=dry_run,
        )


def get_db_table_columns(cur: psycopg2.extensions.cursor, table_name: str) -> list[str]:
    """
    Get the list of all non-generated column names for a PostgreSQL table.

    :param psycopg2.extensions.cursor cur: The psycopg2 cursor to use
    :param str table_name: The possibly schema qualified table name
    :return: The list of non-generated columns from the table, ordered by their
        position in the database
    :rtype: list
    """
    table, schema = split_schema_from_table(table_name)
    cur.execute(
        """SELECT column_name
        FROM information_schema.columns 
        WHERE is_generated <> 'ALWAYS' AND table_name = %s AND table_schema = %s
        ORDER BY ordinal_position""",
        (table, schema),
    )
    return [row[0] for row in cur.fetchall()]


def confirm_table_and_file_columns_match(
    cur: psycopg2.extensions.cursor,
    table_fn: str,
    table_name: str,
    sep: str = ",",
    sanitize: bool = False,
    allow_columns_subset: bool = False,
) -> list[str]:
    """
    :param psycopg2.extensions.cursor cur: The psycopg2 cursor to use
    :param str table_fn: The file name of the data source to check
    :param str table_name: The name of the database table to check
    :param str sep: The column separator to use, defaults to ","
    :param bool sanitize: Whether to sanitize column names with
        get_csv_fields(), defaults to False
    :param bool allow_columns_subset: Whether to allow loading to the table with
        only a subset of the columns, defaults to False
    :raises ValueError: If file columns do not match database table columns
    :return: The list of column names to load
    :rtype: list
    """
    fields = get_csv_fields(table_fn, sep, sanitize)
    db_table_columns = get_db_table_columns(cur, table_name)
    if set(fields) == set(db_table_columns):
        logger.debug(f"{table_fn} and {table_name} fields match.")
    elif allow_columns_subset and set(csv_fields) <= set(db_table_fields):
        logger.debug(
            f"{table_fn} and {table_name} fields do no match, but the file's "
            "are a subset of database.  Continuing."
        )
    else:
        fields = set(fields)
        db_table_columns = set(db_columns)
        extra = sorted(fields - db_table_columns)
        missing = sorted(db_table_columns - fields)
        extra_clause = f"\nExtra columns:\n{extra}" if extra else ""
        missing_clause = f"\nMissing columns:\n{missing}" if missing else ""
        raise ValueError(
            f"{table_fn} and {table_name} do not match.\n"
            f"Specification ({len(fields)} columns):\n{sorted(fields)}\n"
            f"Database ({len(db_table_columns)}):\n{sorted(db_table_columns)}"
            f"{extra_clause}{missing_clause}"
        )
    return fields


def import_table(
    cur: psycopg2.extensions.cursor,
    table_fn: str,
    table_name: str,
    csv_format: bool = False,
    sep: str = ",",
    sanitize: bool = False,
    truncate: bool = True,
    header: bool = True,
    quote: str = "'\"'",
    escape: str = "'\"'",
    allow_columns_subset: bool = False,
    fields: Optional[Iterable[str]] = None,
) -> None:
    """
    Load a PostgreSQL CSV or TEXT format file to the database

    :param psycopg2.extensions.cursor: The psycopg2 cursor to use
    :param str table_fn: The file name of the data source to load
    :param str table_name: The name of the database table to load to
    :param csv_format: Whether to use CSV format (otherwise TEXT), defaults to
        False
    :param str sep: The column separator to use, defaults to ","
    :param bool sanitize: Whether to sanitize column names with
        get_csv_fields(), defaults to False
    :param bool truncate: Whether to truncate the table before loading, defaults
        to True
    :param bool header: Whether the file has a header row, defaults to True
    :param str quote: The quote character, defaults to "'\"'"
    :param str escape: The escape character, defaults to "'\"'"
    :param bool allow_columns_subset: Whether to allow loading to the table with
        only a subset of the columns, defaults to False
    :param fields: A list of fields for the file; this value is required if
        there is no header, defaults to None
    :type fields: list or None
    :raises ValueError: If header and fields specified or if neither is
        specified
    """
    if header == bool(fields):
        raise ValueError("One of header or fields must be specified and not both.")
    if header:
        fields = confirm_table_and_file_columns_match(
            cur,
            table_fn,
            table_name,
            sep=sep if csv_format else "\t",
            sanitize=sanitize,
            allow_columns_subset=allow_columns_subset,
        )
    fields_string = '"{}"'.format('","'.join(fields))
    statement = f"COPY {table_name} ({fields_string}) FROM STDIN"
    if csv_format:
        statement += f" CSV DELIMITER '{sep}' QUOTE {quote} ESCAPE {escape}"
    if truncate:
        cur.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY")
    with zopen(table_fn, "rt") as table_fh:
        if header:
            next(table_fh)
        cur.copy_expert(statement, table_fh)
