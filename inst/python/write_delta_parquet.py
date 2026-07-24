#!/usr/bin/env python3
"""Stream Arrow IPC into Parquet with explicit lossless column encodings."""

import argparse
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as parquet


parser = argparse.ArgumentParser()
parser.add_argument("--input", required=True)
parser.add_argument("--output", required=True)
parser.add_argument("--expected-rows", required=True, type=int)
parser.add_argument("--compression-level", type=int, default=6)
parser.add_argument("--row-group-size", type=int, default=1_000_000)
args = parser.parse_args()

input_path = Path(args.input)
output_path = Path(args.output)
if not input_path.is_file():
    raise FileNotFoundError(f"Arrow IPC input does not exist: {input_path}")
if args.compression_level < 1 or args.compression_level > 22:
    raise ValueError("compression level must be between 1 and 22")
if args.row_group_size < 1:
    raise ValueError("row group size must be positive")

ipc_source = pa.memory_map(str(input_path), "r")
ipc_reader = pa.ipc.open_file(ipc_source)
schema = ipc_reader.schema
input_rows = sum(
    ipc_reader.get_batch(i).num_rows for i in range(ipc_reader.num_record_batches)
)
if input_rows != args.expected_rows:
    raise ValueError(
        f"Expected {args.expected_rows} rows, found {input_rows} in staging file"
    )

column_encoding = {}
dictionary_columns = []
for field in schema:
    if pa.types.is_integer(field.type):
        column_encoding[field.name] = "DELTA_BINARY_PACKED"
    elif pa.types.is_floating(field.type):
        column_encoding[field.name] = "BYTE_STREAM_SPLIT"
    elif (
        pa.types.is_string(field.type)
        or pa.types.is_large_string(field.type)
        or pa.types.is_binary(field.type)
        or pa.types.is_large_binary(field.type)
        or pa.types.is_dictionary(field.type)
    ):
        dictionary_columns.append(field.name)

writer = parquet.ParquetWriter(
    output_path,
    schema,
    compression="zstd",
    compression_level=args.compression_level,
    use_dictionary=dictionary_columns if dictionary_columns else False,
    column_encoding=column_encoding,
    write_statistics=True,
)
try:
    for batch_index in range(ipc_reader.num_record_batches):
        writer.write_batch(
            ipc_reader.get_batch(batch_index),
            row_group_size=args.row_group_size,
        )
finally:
    writer.close()
    ipc_source.close()

metadata = parquet.read_metadata(output_path)
if metadata.num_rows != args.expected_rows:
    raise ValueError(
        f"Expected {args.expected_rows} rows, found {metadata.num_rows} in output"
    )
if metadata.schema.to_arrow_schema() != schema:
    raise ValueError("Output Parquet schema differs from the Arrow IPC schema")
if output_path.stat().st_size == 0:
    raise ValueError(f"Parquet output is empty: {output_path}")

