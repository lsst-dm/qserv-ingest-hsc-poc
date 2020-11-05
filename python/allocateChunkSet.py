#!/usr/bin/env python

import argparse
import getpass
import logging
import os
import re
import requests
import sys

AUTH_PATH = "~/.lsst/qserv"


def main(basepath, url, transactionId, tableName, logger):
    """Allocate chunks for all chunk files in the given folder"""
    try:
        with open(os.path.expanduser(AUTH_PATH), 'r') as f:
            authKey = f.read().strip()
    except:
        authKey = getpass.getpass()

    with os.scandir(basepath) as it:
        for entry in it:
            m = re.match(r"chunk_(\d+)\D*", entry.name)
            if not m:
                continue
            chunkId = int(m.group(1))
            response = requests.post(url, json={"transaction_id": transactionId,
                                                "chunk": chunkId,
                                                "auth_key": authKey})
            responseJson = response.json()
            if not responseJson['success']:
                logger.critical("failed %s for chunk %d, error: %s",
                                entry.name, chunkId, responseJson['error'])
            else:
                host = responseJson['location']['host']
                port = responseJson['location']['port']
                cmd = f"http://{host}:{port} {transactionId} {tableName} {entry.path} {chunkId}"
                logger.debug(cmd)


if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler())

    parser = argparse.ArgumentParser(description="Make loading commands for Qserv workers for chunk files")
    parser.add_argument("basepath", type=str, help="Path to the folder contain chunk files")
    parser.add_argument("--id", type=int, help="Super transaction id")
    parser.add_argument("--idFile", type=str, help="Path to file containing the super transaction id")
    parser.add_argument("--url", type=str, help="Web Service URL",
                        default="http://lsst-qserv-master03:25080/ingest/chunk")
    parser.add_argument("--tableName", type=str, help="Table name", default="Object")
    args = parser.parse_args()

    if args.id:
        transId = args.id
    else:
        with open(args.idFile, "r") as f:
            for line in f:
                m = re.match(r"Started transaction (\d+)", line)
                if m:
                    transId = int(m.group(1))
                    break
        if not isinstance(transId, int):
            raise RuntimeError("Failed to obtain the super transaction ID")

    sys.exit(main(args.basepath, args.url, transId, args.tableName, logger))
