#!/usr/bin/env python

import argparse
import getpass
import logging
import os
import re
import requests
import sys

AUTH_PATH = "~/.lsst/qserv"

def main(filename, logger):

    try:
        with open(os.path.expanduser(AUTH_PATH), 'r') as f:
            authKey = f.read().strip()
    except:
        authKey = getpass.getpass()

    with open(filename, "r") as f:
        for line in f:
            url, transId, tableName, filepath, chunkId = line.split()
            overlap = int("overlap" in filepath)
            response = requests.post(url[:-1]+"4/ingest/file",
                json={
                    "transaction_id": int(transId),
                    "table": tableName,
                    "url": f"file:///lsstdata/user/staff{filepath}",
                    "chunk": int(chunkId),
                    "column_separator":",",
                    "overlap": overlap,
                    "auth_key": authKey
                }
            )
            responseJson = response.json()
            if responseJson['success']:
                logger.info("Loaded %s in transaction %s", filepath, transId)
            else:
                logger.info(responseJson)
                logger.critical("failed %s for chunk %d, error: %s",
                                filepath, chunkId, responseJson['error'])


if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler())

    parser = argparse.ArgumentParser(description="Load data")
    parser.add_argument("filename", type=str, help="file containing chunk files info")
    args = parser.parse_args()

    sys.exit(main(args.filename, logger))
