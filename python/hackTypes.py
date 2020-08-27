#!/usr/bin/env python

import pyarrow.parquet as pq
import numpy as np
import pandas as pd
import pyarrow
import argparse
import os
import yaml
from lsst.utils import getPackageDir


def main(inputFile, outputFile):
    """Temporary hackarounds from using old pyarrow"""
    parquet_file = pq.ParquetFile(inputFile)
    df = parquet_file.read().to_pandas()

    # Brutally fill nulls with 0
    # It can be interpreted as boolean
    df.fillna(value=0, inplace=True)

    typeMapping = {
        'FLOAT': 'float32',
        'DOUBLE': 'float64',
        'BOOLEAN': 'bool',
        'BIGINT': 'int64',
        'INTEGER': 'int32',
        'TEXT': 'str',
    }

    filepath = os.path.join(getPackageDir("sdm_schemas"), 'yml', 'hsc.yaml')
    with open(filepath, 'r') as f:
        hscSchema = yaml.safe_load(f)['tables']
    objectSchema = [table for table in hscSchema if table['name'] == 'Object']
    hackDict = dict()
    for column in objectSchema[0]['columns']:
        sqlType = column['mysql:datatype']
        # Hack the types for now
        if sqlType in typeMapping:
            sqlType = typeMapping[sqlType]
        hackDict[column['name']] = sqlType

    print(hackDict)
    df = df.astype(hackDict, copy=False)

    table = pyarrow.Table.from_pandas(df)
    pq.write_table(table, outputFile, compression='none')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Hack types of a parquet file")
    parser.add_argument("-i", "--inputFile", default="in.parq",
                        help="input file")
    parser.add_argument("-o", "--outputFile", type=str, default="out.parq",
                        help="output file")
    args = parser.parse_args()

    main(args.inputFile, args.outputFile)
