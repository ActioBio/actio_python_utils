"""
Database-related functionality.
"""

import logging
import os
import pgtoolkit.pgpass
import pgtoolkit.service
import psycopg2
import re
from collections.abc import Callable, Hashable, Iterable, Mapping, MutableMapping
from contextlib import contextmanager, nullcontext
from functools import wraps
from psycopg2.extras import DictCursor
from types import TracebackType
from typing import Any, Optional
from .utils import cfg, CustomCSVDialect, get_csv_fields, rename_dict_keys, zopen

logger = logging.getLogger(__name__)


def pg_pass_entry_matches(self, do_regex_matching=False, **attrs: int | str) -> bool:
    """
    Enhances :class:`pgpass.PassEntry.pass` to do regex matching

    :param do_regex_matching: Enable this functionality
    :param attrs: keyword/value pairs correspond to one or more
        PassEntry attributes (ie. hostname, port, etc...)
    :return: Whether the entry matches all provided attributes
    """

    # Provided attributes should be comparable to PassEntry attributes
    expected_attributes = self.__dict__.keys()
    for k in attrs.keys():
        if k not in expected_attributes:
            raise AttributeError("%s is not a valid attribute" % k)

    is_match = (
        (lambda key, pattern: re.fullmatch(str(pattern), str(getattr(self, key))))
        if do_regex_matching
        else (lambda key, value: str(value) == str(getattr(self, key)))
    )
    for k, v in attrs.items():
        if not is_match(k, v):
            return False
    return True


pgtoolkit.pgpass.PassEntry.matches = pg_pass_entry_matches


def get_pg_service_record(
    service: Optional[str] = None, service_fn: Optional[str] = None
) -> dict:
    """
    Locates the PostgreSQL login credentials given a service name.
    Uses ``service = $PGSERVICE`` or ``cfg["db"]["service"]`` if not specified.

    :param service: The PostgreSQL service name to get
    :param service_fn: The path to the file containing service definitions.
        Will use :func:`pgtoolkit.service.find` if not specified
    :return: The dict of values found
    """
    if not service_fn:
        service_fn = pgtoolkit.service.find()
    if not os.path.isfile(service_fn):
        raise OSError(f"Service file {service_fn} does not exist.")
    if not service:
        service = os.getenv("PGSERVICE")
        if not service:
            service = cfg["db"]["service"]
    services = pgtoolkit.service.parse(service_fn)
    try:
        service_record = services[service]
    except:
        raise KeyError(f"Service {service} not found.")
    return rename_dict_keys(
        dict(service_record),
        (
            ("dbname", "database"),
            ("host", "hostname"),
            ("name", None),
            ("user", "username"),
        ),
    )


def get_pgpass_record(
    service_dict: Mapping[str, str],
    pgpass_fn: str = os.path.join(os.path.expanduser("~"), ".pgpass"),
    do_regex_matching=False,
) -> pgtoolkit.pgpass.PassEntry:
    """
    Gets the PostgreSQL login credentials given the parameters in `service_dict`.

    :param service_dict: Parameters to match pgpass records against.
    :param pgpass_fn: The path to the file containing database login data
    :param do_regex_matching: Enable regex matching
    :return: The database login credentials corresponding to the given service
    """
    pgpass_records = []
    for pgpass_record in pgtoolkit.pgpass.parse(pgpass_fn).lines:
        if pgpass_record.matches(do_regex_matching=do_regex_matching, **service_dict):
            pgpass_records.append(pgpass_record)
    if pgpass_records:
        if (nrecord := len(pgpass_records)) > 1:
            raise ValueError(f"Matched {nrecord} records.")
        else:
            return pgpass_records[0]
    else:
        raise ValueError("Didn't find the matching service.")


def get_pg_config(
    service: Optional[str] = None,
    service_fn: Optional[str] = None,
    pgpass_fn: str = os.path.join(os.path.expanduser("~"), ".pgpass"),
) -> pgtoolkit.pgpass.PassEntry:
    """
    Locates the PostgreSQL login credentials given a service name.
    Uses ``service = $PGSERVICE`` or ``cfg["db"]["service"]`` if not specified.

    :param service: The PostgreSQL service name to get
    :param service_fn: The path to the file containing service definitions.
        Will use :func:`pgtoolkit.service.find` if not specified
    :param pgpass_fn: The path to the file containing database login data
    :return: The database login credentials corresponding to the given service
    """
    if not os.path.isfile(pgpass_fn):
        raise OSError(f"pgpass file {pgass_fn} does not exist.")
    service_dict = get_pg_service_record(service, service_fn)
    pgpass = pgtoolkit.pgpass.parse(pgpass_fn)
    for pgpass_record in pgpass.lines:
        if pgpass_record.matches(**service_dict):
            return pgpass_record
    else:
        raise ValueError("Didn't find the matching service.")


def format_postgresql_uri_from_pgpass(pgpass_record: pgtoolkit.pgpass.PassEntry) -> str:
    """
    Return a PostgreSQL URI connection string

    :param pgpass_record: The record to format
    :return: The URI formatted as:
        ``postgresql://user:password@host:port/database``
    """
    return (
        f"postgresql://{pgpass_record.username}:{pgpass_record.password}"
        f"@{pgpass_record.hostname}:{pgpass_record.port}/{pgpass_record.database}"
    )


def split_schema_from_table(
    table: str, default_schema: str = "public"
) -> tuple[str, str]:
    """
    Split a possibly schema qualified table name into its schema and table names

    :param table: The possibly schema qualified table name
    :param default_schema: The default schema name
    :return: A list with the schema name and the table name
    """
    if "." in table:
        return table.split(".", 1)
    else:
        return [default_schema, table]


@contextmanager
def savepoint(cur: psycopg2.extensions.cursor, savepoint: str = "savepoint") -> None:
    """
    Creates a context manager to create a savepoint upon entering, rollback if
    an error occurs, and release upon exiting

    :param cur: The psycopg2 cursor query to use
    :param savepoint: The name to give to the savepoint
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

    :param method: The method to wrap
    :return: The wrapped method
    """

    @wraps(method)
    def inner(obj, *args, dry_run=False, dont_use_savepoint=False, **kwargs):
        cm = (
            nullcontext()
            if (dry_run or dont_use_savepoint)
            else savepoint(super(type(obj), obj))
        )
        with cm:
            return method(obj, *args, **kwargs)

    return inner


class LoggingCursor(DictCursor):
    r"""
    Wraps :class:`psycopg2.extras.DictCursor` such that each copy_expert and
    execute statement is logged

    :param \*args: Any positional arguments to pass to the DictCursor constructor
    :param log_level: The logging level to use
    :param log_name: The name to give the logger
    :param \**kwargs: Any named arguments to pass to the DictCursor constructor
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

        :param statement: The SQL statement to log
        :param dry_run: Do a dry run
        """
        if dry_run:
            self.logger.log(self.log_level, "Would execute statement:\n" + statement)
        else:
            self.logger.log(self.log_level, "Executing statement:\n" + statement)

    def copy_expert(self, sql: str, file: str, *args, dry_run=False, **kwargs) -> None:
        r"""
        Logs the sql statement and executes it if ``dry_run = False``

        :param sql: The SQL statement to execute
        :param file: The path to the file to import
        :param \*args: Any positional arguments
        :param dry_run: Do a dry run
        :param \**kwargs: Any named arguments
        """
        self._log_statement(sql, dry_run)
        if not dry_run:
            super().copy_expert(sql, file, *args, **kwargs)

    def copy_to_csv(self, sql: str, file: str, *args, dry_run=False, **kwargs) -> None:
        r"""
        Logs the sql statement and executes it if ``dry_run = False``

        :param sql: The SQL statement to execute
        :param file: The path to the file to write to
        :param \*args: Any positional arguments
        :param dry_run: Do a dry run
        :param \**kwargs: Any named arguments
        """
        sql = f"COPY ({sql}) TO STDOUT CSV HEADER"
        self._log_statement(sql, dry_run)
        if not dry_run:
            self.logger.log(self.log_level, f"Writing query output to {file}.")
            with zopen(file, "wt") as fh:
                super().copy_expert(sql, fh)

    def execute(
        self,
        query: str,
        vars: Iterable | Mapping = None,
        *args,
        dry_run: bool = False,
        dont_use_savepoint: bool = False,
        **kwargs,
    ) -> None:
        r"""
        Logs the query and executes it if ``dry_run = False``

        :param query: The SQL query to execute
        :param vars: Variables to bind to the query
        :param \*args: Any positional arguments
        :param dry_run: Do a dry run
        :param dont_use_savepoint: Ignored
        :param \**kwargs: Any named arguments
        """
        self._log_statement(self.mogrify(query, vars).decode(), dry_run)
        if not dry_run:
            super().execute(query, vars, *args, **kwargs)

    def get_table_constraint_statements(self, table_list: Iterable[str]) -> list[str]:
        """
        Takes a list of tables and returns a list of all the SQL definitions of
        constraints defined on the tables.  The constraints are ordered by keys,
        then other constraints on the tables, and lastly foreign keys defined on
        other tables that reference one of the tables specified.
        The purpose of this is to be able to drop these constraints, load data, and
        recreate them for efficiency.

        :param table_list: The list of tables to get constraints on
        :return: The list of constraints
        """
        if not table_list:
            return []
        table_list = [split_schema_from_table(table) for table in table_list]
        # order constraints to list keys first for efficiency
        self.execute(
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
            ),
            dont_use_savepoint=True,
        )
        return [r[0] for r in self.fetchall()]

    def drop_table_constraints(
        self, table_list: Iterable[str], dry_run: bool = False
    ) -> None:
        """
        Takes a list of tables and finds all constraints defined on them, then
        drops them.  Foreign key constraints are processed first because
        attempting to drop a unique key on a column that is referenced in a
        foreign key results in an error.

        :param table_list: The list of tables to get constraints on
        :param dry_run: Do a dry run
        """
        table_list = [split_schema_from_table(table) for table in table_list]
        # order constraints to list foreign keys first for dropping
        self.execute(
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
            ),
            dont_use_savepoint=True,
        )
        constraints = self.fetchall()
        for nspname, relname, conname in constraints:
            self.execute(
                f"ALTER TABLE {nspname}.{relname} DROP CONSTRAINT {conname}",
                dry_run=dry_run,
            )

    def drop_table_keys(
        self, table_list: Optional[Iterable[str]] = None, dry_run: bool = False
    ) -> None:
        """
        Takes a list of tables and drops all indexes defined on them.

        :param table_list: The list of tables to drop index on
        :param dry_run: Do a dry run
        """
        if not table_list:
            return
        table_list = [split_schema_from_table(table) for table in table_list]
        self.execute(
            """SELECT n.nspname, c.relname AS table_name, c2.relname AS index_name
            FROM pg_index x
            JOIN pg_class c ON x.indrelid = c.oid
            JOIN pg_class c2 ON x.indexrelid = c2.oid
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE NOT x.indisprimary AND {}""".format(
                " OR ".join(
                    [
                        f"(n.nspname = '{schema}' AND c.relname = '{table}')"
                        for schema, table in table_list
                    ]
                )
            ),
            dont_use_savepoint=True,
        )
        indexes = self.fetchall()
        for nspname, table_name, index_name in indexes:
            self.execute(f"DROP INDEX {nspname}.{index_name}", dry_run=dry_run)

    def get_db_table_columns(self, table_name: str) -> list[str]:
        """
        Get the list of all non-generated column names for a PostgreSQL table.

        :param table_name: The possibly schema qualified table name
        :return: The list of non-generated columns from the table, ordered by their
            position in the database
        """
        table, schema = split_schema_from_table(table_name)
        self.execute(
            """SELECT column_name
            FROM information_schema.columns
            WHERE is_generated <> 'ALWAYS' AND table_schema = %s AND table_name = %s
            ORDER BY ordinal_position""",
            (table, schema),
            dont_use_savepoint=True,
        )
        return [row[0] for row in self.fetchall()]

    def confirm_table_and_file_columns_match(
        self,
        table_fn: str,
        table_name: str,
        sep: str = ",",
        sanitize: bool = False,
        allow_columns_subset: bool = False,
    ) -> list[str]:
        """
        Confirm that the field names in a table match those of a file.

        :param table_fn: The file name of the data source to check
        :param table_name: The name of the database table to check
        :param sep: The column separator to use
        :param sanitize: Whether to sanitize column names with
            :func:`~actio_python_utils.utils.get_csv_fields`
        :param allow_columns_subset: Whether to allow loading to the table with
            only a subset of the columns
        :raises ValueError: If file columns do not match database table columns
        :return: The list of column names to load
        """
        fields = get_csv_fields(table_fn, sep, sanitize)
        db_table_columns = self.get_db_table_columns(table_name)
        if set(fields) == set(db_table_columns):
            logger.debug(f"{table_fn} and {table_name} fields match.")
        elif allow_columns_subset and set(csv_fields) <= set(db_table_fields):
            logger.debug(
                f"{table_fn} and {table_name} fields do no match, but the file's "
                "are a subset of database.  Continuing."
            )
        else:
            fields = set(fields)
            db_table_columns = set(db_table_columns)
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
        self,
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
        recreate_indexes: bool = False,
    ) -> None:
        """
        Load a PostgreSQL CSV or TEXT format file to the database

        :param table_fn: The file name of the data source to load
        :param table_name: The name of the database table to load to
        :param csv_format: Whether to use CSV format (otherwise TEXT)
        :param sep: The column separator to use
        :param sanitize: Whether to sanitize column names with
            :func:`~actio_python_utils.utils.get_csv_fields`
        :param truncate: Whether to truncate the table before loading
        :param header: Whether the file has a header row
        :param quote: The quote character
        :param escape: The escape character
        :param allow_columns_subset: Whether to allow loading to the table with
            only a subset of the columns
        :param fields: A list of fields for the file; this value is required if
            there is no header
        :param recreate_indexes: Drop indexes before loading and then add them
            back at the end
        :raises ValueError: If header and fields specified or if neither is
            specified
        """
        if header == bool(fields):
            raise ValueError("One of header or fields must be specified and not both.")
        if header:
            fields = self.confirm_table_and_file_columns_match(
                table_fn,
                table_name,
                sep=sep if csv_format else "\t",
                sanitize=sanitize,
                allow_columns_subset=allow_columns_subset,
            )
        if recreate_indexes:
            constraints = self.get_table_constraint_statements([table_name])
            self.drop_table_constraints([table_name])
            self.drop_table_keys([table_name])
        fields_string = '"{}"'.format('","'.join(fields))
        statement = f"COPY {table_name} ({fields_string}) FROM STDIN"
        if csv_format:
            statement += f" CSV DELIMITER '{sep}' QUOTE {quote} ESCAPE {escape}"
        if truncate:
            self.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY")
        with zopen(table_fn, "rt") as table_fh:
            if header:
                next(table_fh)
            self.copy_expert(statement, table_fh)
        if recreate_indexes:
            for constraint_statement in constraints:
                self.execute(constraint_statement)


class SavepointCursor(LoggingCursor):
    """
    Wraps :class:`LoggingCursor` methods to use a savepoint context manager
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

    @savepoint_wrapper
    def get_table_constraint_statements(self, *args, **kwargs) -> list[str]:
        return super().get_table_constraint_statements(*args, **kwargs)

    @savepoint_wrapper
    def drop_table_constraints(self, *args, **kwargs) -> None:
        super().drop_table_constraints(*args, **kwargs)

    @savepoint_wrapper
    def drop_table_keys(self, *args, **kwargs) -> None:
        super().drop_table_keys(*args, **kwargs)

    @savepoint_wrapper
    def get_db_table_columns(self, *args, **kwargs) -> list[str]:
        return super().get_db_table_columns(*args, **kwargs)

    @savepoint_wrapper
    def import_table(self, *args, **kwargs) -> None:
        super().import_table(*args, **kwargs)


def replace_dict_values_with_global_definition(
    mapping: MutableMapping[Hashable, Any],
    global_mapping: Mapping[Hashable, Any] = globals(),
) -> MutableMapping[Hashable, Any]:
    """
    Replaces values in the mapping with those in global_mapping when the value
    is a str and it is a key in the global mapping

    :param mapping: The mapping for which to replace values
    :param global_mapping: The mapping from which to get replacement values
    :return: Updated mapping replacing (key, value) with
        (key, global_mapping[value]) for appropriate keys
    """
    for key, value in mapping.items():
        if isinstance(value, str) and value in global_mapping:
            mapping[key] = global_mapping[value]


# replace cursor_factory with the value defined here
replace_dict_values_with_global_definition(cfg["db"])


def get_db_args(
    service: Optional[str] = None,
    db_args: Optional[Mapping[Hashable, Any]] = None,
    logger: Optional[logging.Logger] = None,
    cursor_factory: psycopg2.extensions.cursor = LoggingCursor,
) -> dict:
    """
    Returns a dict of arguments to log in with psycopg2.
    Resolution order for connecting to a database is as follows:

    #.

       #. service, referring to a PostgreSQL service name, normally defined
          in ``~/.pg_service.conf``
       #. db_args, arbitrary dictionary of arguments

    #. Environment variable ``$DB_CONNECTION_STRING`` with format::

        postgres://your_user:your_password@your_host:your_port/your_database

    #. Environment variable ``$PGSERVICE``, referring to a PostgreSQL service name
    #. ``cfg["db"]``, which should resolve to a dictionary of arguments

    :param service: The PostgreSQL service to connect to
    :param db_args: A mapping of database connection arguments
    :param logger: Optional logger to write to
    :param cursor_factory: The class of cursor to use for the
        connection by default (used for ``service``/``$DB_CONNECTION_STRING``)
    :raises ValueError: If ``service`` and ``db_args`` are specified
    :return: The dict of login arguments
    """
    # Check for environment variables
    db_connection_string = os.getenv("DB_CONNECTION_STRING")
    pgservice = os.getenv("PGSERVICE")
    if service:
        if db_args:
            raise ValueError("Specify up to one of service/db_args, not both.")
        message = "Using service for login."
        db_args = {"service": service, "cursor_factory": cursor_factory}
    elif db_args:
        message = "Using db_args for login."
        db_args = db_args
    elif db_connection_string:
        message = "Using DB_CONNECTION_STRING environment variable for login."
        db_args = {
            "dsn": db_connection_string,
            "cursor_factory": cursor_factory,
        }
    elif pgservice:
        message = "Using PGSERVICE environment variable for login."
        db_args = {"service": pgservice, "cursor_factory": cursor_factory}
    else:
        message = "Using cfg for login."
        db_args = cfg["db"]
    if logger:
        logger.debug(message)
    return db_args


class DBConnection(object):
    """
    Creates a psycopg2 database connection with the specified parameters and
    acts as a context manager.

    :param service: The PostgreSQL service to connect to
    :param db_args: A mapping of database connection arguments
    :param log_name: The name to give the logger
    :param commit: Commit the transaction upon closing the connection if no
        errors were encountered
    :param cursor_factory: The class of cursor to use for the connection
        by default (used for ``service``/``$DB_CONNECTION_STRING``)
    :raises ValueError: If ``service`` and ``db_args`` are specified
    """

    def __init__(
        self,
        service: Optional[str] = None,
        db_args: Optional[Mapping[str, Any]] = None,
        log_name: str = cfg["logging"]["names"]["db"],
        commit: bool = False,
        cursor_factory: psycopg2.extensions.cursor = LoggingCursor,
    ) -> None:
        self.logger = logging.getLogger(log_name)
        self.commit = commit
        self.db_args = get_db_args(service, db_args, self.logger, cursor_factory)

    def connect(self) -> None:
        """
        Connects to the database and creates attributes ``db`` and ``cursor``
        for the connection and a cursor, respectively
        """
        self.logger.debug(f"Connecting to DB with parameters: {self.db_args}.")
        self.db = psycopg2.connect(**self.db_args)
        self.cur = self.db.cursor()

    def disconnect(self, exception: bool) -> None:
        """
        Disconnect from the database and commit if ``self.commit = True`` and
        no exception has been raised

        :param exception: Whether an exception has been raised or not
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
        """
        self.disconnect(exc_type is not None)


def connect_to_db(
    service: Optional[str] = None,
    db_args: Optional[MutableMapping[str, str]] = None,
) -> psycopg2.extensions.connection:
    """
    Return a connection to the specified PostgreSQL database

    :param service: The PostgreSQL service to connect to
    :param db_args: A mapping of database connection arguments
    :return: The database connection
    """
    db_args = get_db_args(service, db_args, logger)
    logger.debug(f"Connecting to DB with parameters: {db_args}.")
    return psycopg2.connect(**db_args)
