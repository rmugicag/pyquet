import argparse
import json
import logging
import sys

from pyquet.modules.generator import DataGenerator


def main():
    logging.info("Running Pyquet generator...")
    parser = argparse.ArgumentParser(description="Pyquet generator")

    parser.add_argument("--schema-path",
                        "-s",
                        metavar="SCHEMA_PATH",
                        type=str,
                        help="Path to the schema file",
                        required=True)

    parser.add_argument("--output-type",
                        "-t",
                        metavar="OUTPUT_TYPE",
                        type=str,
                        help="Output type, parquet or csv",
                        required=True)

    parser.add_argument("--destination-dir",
                        "-d",
                        metavar="DESTINATION_DIR",
                        type=str,
                        help="Destination directory",
                        required=False,
                        default=None)

    parser.add_argument("--partitions",
                        "-p",
                        metavar="PARTITIONS",
                        type=str,
                        help="Partitions",
                        required=False)

    parser.add_argument("--catalog-path",
                        "-c",
                        metavar="CATALOG_PATH",
                        type=str,
                        help="Catalog path",
                        required=False)

    parser.add_argument("--num-rows",
                        "-r",
                        metavar="NUM_ROWS",
                        type=int,
                        help="Number of rows",
                        required=False,
                        default=10)

    parser.add_argument("--limit-rows",
                        "-l",
                        help="Limit rows",
                        required=False,
                        action='store_true')

    parser.add_argument("--fixed-values",
                        "-v",
                        metavar="FIXED_VALUES",
                        type=str,
                        help="Fixed values",
                        required=False)

    if len(sys.argv) == 1:
        parser.print_help()
        exit(0)

    args = parser.parse_args()

    generator = DataGenerator(args.catalog_path, args.num_rows, args.limit_rows)


    if args.fixed_values:
        args.fixed_values = args.fixed_values.replace("'", "\"")
        fixed_values = json.loads(args.fixed_values)
        generator.catalog.update(fixed_values)

    partitions = args.partitions.split(",")

    generator.generate_data(args.schema_path, args.output_type, partitions, args.destination_dir)
