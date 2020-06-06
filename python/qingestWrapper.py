#!/usr/bin/env python

import argparse
import getpass
import json
import logging
import os
import requests
import sys
import yaml

AUTH_PATH = "~/.lsst/qserv"


def authorize():
    """Set up the authorization key for Qserv Ingest system"""
    try:
        with open(os.path.expanduser(AUTH_PATH), 'r') as f:
            authKey = f.read().strip()
    except:
        logging.debug("Cannot find %s", AUTH_PATH)
        authKey = getpass.getpass()
    return authKey


def resolveUrl(command, host, port):
    """Configure the full URL

    Parameters
    ----------
    command: `str`
        The service operation; this determines the resource to be used
    host: `str`
        Base server URL
    port: `str`
        Port number of the web service

    Returns
    -------
    the resource URI
    """
    if "db" in command:
        resource = "/ingest/database"
    elif "transaction" in command:
        resource = "/ingest/trans"
    elif "table" in command:
        resource = "/ingest/table"
    elif "version" in command:
        resource = "/meta/version"
    else:
        raise NotImplementedError("Unrecognized command")
    return host + ':' + str(port) + resource


def _addArgumentCreateDb(subparser):
    subparser.add_argument("num-stripes", type=int,
                           help="number of stripes to divide the sky into; "
                           "same as the partitioning parameters num-stripes",
                           action=DataAction)
    subparser.add_argument("num-sub-stripes", type=int,
                           help="number of sub-stripes to divide each stripe into; "
                           "same as the partitioning parameters num-sub-stripes",
                           action=DataAction)
    subparser.add_argument("overlap", type=float,
                           help="chunk/sub-chunk overlap radius (deg); "
                           "same as the partitioning parameters overlap",
                           action=DataAction)


def _addArgumentCreateTable(subparser):
    subparser.add_argument("table", type=str, action=DataAction,
                           help="table name; will look for the schema in the felis file")
    subparser.add_argument("json", type=str, action=JsonAction,
                           help="a json config file containing the table parameters")
    subparser.add_argument("felis", type=str, action=FelisAction,
                           help="a felis schema file from cat containing the table schema")


def get(url, params=None):
    """Perform a standard put method

    Parameters
    ----------
    params: `dict`
        Parameters to be included in the URL's query string
    """
    response = requests.get(url, params=params)
    responseJson = response.json()
    if not responseJson["success"]:
        logging.critical(responseJson["error"])
        return 1
    logging.debug(responseJson)
    logging.debug("success")


def put(url, payload=None, params=None):
    """Perform a standard put method

    Parameters
    ----------
    payload: `dict`
        Application data as JSON
    params: `dict`
        Parameters to be included in the URL's query string
    """
    authKey = authorize()
    logging.debug(url)
    response = requests.put(url, json={"auth_key": authKey}, params=params)
    responseJson = response.json()
    if not responseJson['success']:
        logging.critical("%s %s", url, responseJson['error'])
        return 1
    logging.debug(responseJson)
    logging.info("success")


def post(url, payload):
    """Perform a standard post method

    Parameters
    ----------
    payload: `dict`
        Application data as JSON
    """
    authKey = authorize()
    payload["auth_key"] = authKey
    response = requests.post(url, json=payload)
    del payload["auth_key"]
    responseJson = response.json()
    if not responseJson["success"]:
        logging.critical(responseJson["error"])
        return 1
    logging.debug(responseJson)
    logging.debug("success")

    # For catching the super transaction ID in start-transaction
    # Want to print responseJson["databases"]["hsc_test_w_2020_14_00"]["transactions"]
    if "databases" in responseJson:
        # try:
        transId = list(responseJson["databases"].values())[0]["transactions"][0]["id"]
        logging.info(f"The super transaction ID is {transId}")

    # For catching the location host and port
    if "location" in responseJson and "chunk" in payload:
        host = responseJson["location"]["host"]
        port = responseJson["location"]["port"]
        logging.info("%d %s %d" % (payload["chunk"], host, port))


class DataAction(argparse.Action):
    """argparse action to pack values into the namespace.data dict"""

    def __init__(self, option_strings, dest, nargs=None, type=None, **kwargs):
        if nargs is not None:
            raise ValueError("nargs not allowed")
        self.vtype = type
        super(DataAction, self).__init__(option_strings, dest, **kwargs)

    def __call__(self, parser, namespace, values, option_string):
        if not hasattr(namespace, "data"):
            setattr(namespace, "data", dict())
        if self.vtype is not None:
            namespace.data[self.dest] = self.vtype(values)
            logging.debug(namespace.data[self.dest])
        else:
            try:
                namespace.data[self.dest] = float(values)
            except ValueError:
                namespace.data[self.dest] = values


class JsonAction(argparse.Action):
    """argparse action to read a json file into the namespace.data dict"""

    def __call__(self, parser, namespace, values, option_string):
        with open(values, "r") as f:
            x = json.load(f)
        for item in x:
            namespace.data[item] = x[item]


class FelisAction(argparse.Action):
    """argparse action to read a felis file into namespace.data["schema"]"""

    def __call__(self, parser, namespace, values, option_string):
        """figure out  the schema dict for create-table """
        tableName = namespace.data["table"]
        print(f"Look for {tableName} table in the felis schema at {values}")
        with open(values, "r") as f:
            tables = yaml.safe_load(f)["tables"]
        columns = [table for table in tables if table["name"] == tableName][0]["columns"]
        schema = list()
        for column in columns:
            datatype = column["mysql:datatype"]
            if "nullable" in column:
                nullable = column["nullable"]
            else:
                nullable = True
            if nullable:
                nullstring = " DEFAULT NULL"
            else:
                nullstring = " NOT NULL"
            schema.append({"name": column["name"], "type": column["mysql:datatype"] + nullstring})
        schema.append({"name": "chunkId", "type": "int(11) NOT NULL"})
        schema.append({"name": "subChunkId", "type": "int(11) NOT NULL"})
        namespace.data["schema"] = schema


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Qserv web services")

    subparsers = parser.add_subparsers(dest="command", title="operational commands",
                                       required=True,
                                       description="supported web service operations",
                                       help="use '<command> --help' to see more help")

    operations = {
        "version": "Retrieve the APi version",
        "create-db": "create a database",
        "publish-db": "publish the database",
        "create-table": "create a table",
        "start-transaction": "start a super-transaction",
        "abort-transaction": "abort a super-transaction",
        "commit-transaction": "commit a super-transaction"
    }
    for command in operations:
        subparser = subparsers.add_parser(command, help=operations[command])
        subparser.add_argument("host", type=str, help="Web service host base URL")
        subparser.add_argument("--port", type=int, help="Web service server port", default=25080)
        subparser.add_argument("--verbose", "-v", action="store_true", help="Use debug logging")

        if command in ("create-db", "publish-db", "create-table", "start-transaction"):
            subparser.add_argument("database", type=str, help="database name",
                                   action=DataAction)

        if command in ("commit-transaction", "abort-transaction"):
            subparser.add_argument("transactionId", type=int, help="Super-transaction ID")

        if command == "create-db":
            _addArgumentCreateDb(subparser)
        elif command == "create-table":
            _addArgumentCreateTable(subparser)
        elif command == "publish-db":
            subparser.add_argument("--consolidate-secondary-index",
                                   dest="consolidateSecondaryIndex",
                                   action="store_true",
                                   help="With this flag, consolidate secondary index "
                                        "while publishing the database")
        elif command == "commit-transaction":
            subparser.add_argument("--build-secondary-index", dest="buildSecondaryIndex",
                                   action="store_true",
                                   help="With this flag, build secondary index "
                                        "while commiting the transaction")

    args = parser.parse_args()

    logger = logging.getLogger()
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())

    payload = args.data if hasattr(args, "data") else dict()
    logging.debug("Request payload %s", payload)
    with open('temp_dump.json', 'w') as f:
        json.dump(payload, f, indent=2)

    url = resolveUrl(args.command, args.host, args.port)
    logging.debug("Starting a request: %s with %s", url, payload)

    if args.command in ("create-db", "create-table", "start-transaction"):
        sys.exit(post(url, payload))
    elif args.command == "commit-transaction":
        logging.info("Commiting Transaction %s", args.transactionId)
        logging.debug("build-secondary-index? %s", args.buildSecondaryIndex)
        params = {"abort": 0,
                  "build-secondary-index": int(args.buildSecondaryIndex)}
        sys.exit(put(url + "/" + str(args.transactionId), payload, params))
    elif args.command == "abort-transaction":
        params = {"abort": 1}
        sys.exit(put(url + "/" + str(args.transactionId), payload, params))
    elif args.command == "publish-db":
        params = {"consolidate-secondary-index": args.consolidateSecondaryIndex}
        sys.exit(put(url + "/" + str(args.data["database"]), params=params))
    elif args.command == "version":
        sys.exit(get(url, payload))
    sys.exit(1)
