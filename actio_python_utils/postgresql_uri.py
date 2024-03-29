#!/usr/bin/env python3
"""
Output a PostgreSQL connection URI from ~/.pgpass, ~/.pg_service.conf for the specified connection
"""
import os
import sys
from pgtoolkit import pgpass, service
from . import argparse_functions as uaf, database_functions as udf

if __name__ == "__main__":
    parser = uaf.EnhancedArgumentParser()
    parser.add_argument(
        "-o",
        "--output",
        default=sys.stdout,
        type=uaf.ZFileType("w"),
        help="output file name",
    )
    parser.add_argument(
        "--pgpass-file",
        default=os.path.join(os.path.expanduser("~"), ".pgpass"),
        help="pgpass file to parse",
    )
    parser.add_argument(
        "--services-file", default=service.find(), help="services file to parse"
    )
    parser.add_argument(
        "-s", "--service", help="if specified, all subsequent arguments are ignored"
    )
    parser.add_argument("-n", "--hostname")
    parser.add_argument("-p", "--port")
    parser.add_argument("-d", "--database")
    parser.add_argument("-u", "--username")
    parser.add_argument(
        "-r",
        "--regex-matching",
        action="store_true",
        help="interpret given parameters as regular expressions for matching",
    )
    args = parser.parse_args()
    try:
        if args.service:
            pg_args = udf.get_pg_service_record(args.service, args.services_file)
        else:
            pg_args = {}
            for arg_name in ("hostname", "port", "database", "username"):
                if arg := getattr(args, arg_name):
                    pg_args[arg_name] = arg
            if not pg_args:
                raise ValueError("No requirements passed in.")
        args.output.write(
            udf.format_postgresql_uri_from_pgpass(
                udf.get_pgpass_record(pg_args, args.pgpass_file, args.regex_matching)
            )
            + "\n"
        )
    finally:
        if args.output is not sys.stdout:
            args.output.close()
