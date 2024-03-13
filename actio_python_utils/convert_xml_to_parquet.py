#!/usr/bin/env python3
"""
Convert an XML file to parquet with PySpark for subsequent usage.
N.B. it can be substantially more efficient to query parquet data than XML.
"""
import logging
from . import argparse_functions as uaf, spark_functions as usf

logger = logging.getLogger(__name__)


def tag_value_pairs(arg):
    return [key_value.split("=") for key_value in arg.split(",")]


if __name__ == "__main__":
    parser = uaf.EnhancedArgumentParser(use_logging=True, use_spark=True, use_xml=True)
    parser.add_argument(
        "-s", "--schema", type=uaf.file_exists, help="optional .xsd schema to valid XML"
    )
    parser.add_argument(
        "-t",
        "--value-tag",
        help="can help to specify a value to help with schema parsing errors "
        '(suggested to use "_tag_value")',
    )
    parser.add_argument(
        "--load-config-options",
        type=tag_value_pairs,
        help="any additional named arguments to pass when "
        "loading the XML (format is key1=value1,...,keyn=valuen)",
    )
    parser.add_argument("XML", type=uaf.file_exists, help="XML file to convert")
    parser.add_argument(
        "row_tag",
        metavar="ROW-TAG",
        help="the tag that indicates the beginning/end of a record",
    )
    parser.add_argument(
        "output_directory",
        metavar="OUTPUT-DIRECTORY",
        help="the directory to output to",
    )
    args = parser.parse_args()
    args.spark.convert_xml_to_parquet(
        xml_fn=args.XML,
        output_directory=args.output_directory,
        row_tag=args.row_tag,
        schema=args.schema,
        value_tag=args.value_tag,
        load_config_options=args.load_config_options,
        return_dataframe=False,
    )
