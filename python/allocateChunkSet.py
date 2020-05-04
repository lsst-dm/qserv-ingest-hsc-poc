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
    """Make loading commands for all chunk files in the given folder"""
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
                #logger.info("%d %s %d" % (chunkId,host,port))
                cmd = f"docker run --rm -t --network host -u 1000:1000 " \
                      f"-v /etc/passwd:/etc/passwd:ro -v {basepath}:{basepath}:ro " \
                      f"--name qserv-replica-file-ingest- qserv/replica:tools " \
                      f"qserv-replica-file-ingest  --auth-key=AUTHKEY FILE " \
                      f"{host} {port} {transactionId} {tableName} P {entry.path}"
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
                        default="http://lsst-qserv-master01:25080/ingest/v1/chunk") 
    parser.add_argument("--tableName", type=str, help="Table name", default="Object")
    args = parser.parse_args()

    if args.id:
        transId = args.id
    else:
        with open(args.idFile, "r") as f:
            transId = int(f.read())

    sys.exit(main(args.basepath, args.url, transId, args.tableName, logger))
