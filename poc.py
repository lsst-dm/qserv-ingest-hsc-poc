#!/usr/bin/env python

# This is a proof-of-concept only
# Read a list of the input parquet files as from SciPi
# Make a Pegasus dax workflow, including temporary hackarounds,
# converting to csv, partitioning into chunk files, registering, etc.

import argparse
import logging
import os
import re
import Pegasus.DAX3 as peg

# Need a fresh database name.........!!!
database = "hsc_test_w_2020_14_14"

def generateDax(name="object", inputData=None):
    """Generate a Pegasus DAX abstract workflow"""
    dax = peg.ADAG(name)

    # These config-ish files are expected in the input/ folder
    schemaAbh = peg.File("schema.abh")
    dax.addFile(schemaAbh)
    sedScript = peg.File("fixCsv.sed")
    dax.addFile(sedScript)
    partCfg = peg.File("partition.json")
    dax.addFile(partCfg)
    tableJson = peg.File("table.json")
    dax.addFile(tableJson)
    catYaml = peg.File("hsc.yaml")
    dax.addFile(catYaml)

    # (Ab)using the shared filesystem....!!!
    chunkBaseFolder = os.path.join("/project", "hchiang2", "qserv", "qqpoc")
    if not os.path.isdir(chunkBaseFolder):
        logging.warning("Chunk file base folder %s invalid", chunkBaseFolder)

    # Create a new database
    task0a = peg.Job(name="qingest")
    task0a.addProfile(peg.Profile(peg.Namespace.CONDOR, "request_memory", "2GB"))
    task0a.addArguments("create-db", "http://lsst-qserv-master03", str(database), "-v",
                        "340", "3", "0.01667")
    dax.addJob(task0a)
    logfile = peg.File("qingest-a.log")
    dax.addFile(logfile)
    task0a.setStdout(logfile)
    task0a.setStderr(logfile)
    task0a.uses(logfile, link=peg.Link.OUTPUT)

    # Create the Object table in Qserv
    task0b = peg.Job(name="qingest")
    task0b.addProfile(peg.Profile(peg.Namespace.CONDOR, "request_memory", "2GB"))
    task0b.addArguments("create-table", "http://lsst-qserv-master03", str(database),
                        "Object", tableJson, catYaml, "-v")
    dax.addJob(task0b)
    logfile = peg.File("qingest-b.log")
    dax.addFile(logfile)
    task0b.setStdout(logfile)
    task0b.setStderr(logfile)
    task0b.uses(logfile, link=peg.Link.OUTPUT)
    task0b.uses(tableJson, link=peg.Link.INPUT)
    task0b.uses(catYaml, link=peg.Link.INPUT)
    dax.depends(parent=task0a, child=task0b)

    # Start a super-transaction
    # Need to get the super transaction id from the log file
    task0c = peg.Job(name="qingest")
    task0c.addProfile(peg.Profile(peg.Namespace.CONDOR, "request_memory", "2GB"))
    task0c.addArguments("start-transaction", "http://lsst-qserv-master03", str(database))
    dax.addJob(task0c)
    transIdFile = peg.File("qingest-c.log")
    dax.addFile(transIdFile)
    task0c.setStdout(transIdFile)
    task0c.setStderr(transIdFile)
    task0c.uses(transIdFile, link=peg.Link.OUTPUT)
    dax.depends(parent=task0b, child=task0c)

    i = 0
    with open(inputData, 'r') as f:
        for line in f:
            inparq = line.strip()
            i += 1
            logging.debug('Add file %d: %s', i, inparq)

            taskname = 'hackType'
            task1 = peg.Job(name=taskname)
            task1.addProfile(peg.Profile(peg.Namespace.CONDOR, "request_memory", "20GB"))
            outparq = peg.File("hack-%d.parq" % i)
            dax.addFile(outparq)
            task1.addArguments("-i", inparq, "-o", outparq)
            dax.addJob(task1)
            logfile = peg.File("%s-%s.log" % (taskname, i, ))
            dax.addFile(logfile)
            task1.setStdout(logfile)
            task1.setStderr(logfile)
            task1.uses(logfile, link=peg.Link.OUTPUT)
            task1.uses(outparq, link=peg.Link.OUTPUT)

            taskname = 'pq2csv'
            task2 = peg.Job(name=taskname)
            task2.addProfile(peg.Profile(peg.Namespace.CONDOR, "request_memory", "20GB"))
            outcsv = peg.File("csv-%d.csv" % i)
            dax.addFile(outcsv)
            task2.addArguments("--schema", schemaAbh, "--verbose", outparq, outcsv)
            dax.addJob(task2)
            logfile = peg.File("%s-%s.log" % (taskname, i, ))
            dax.addFile(logfile)
            task2.setStdout(logfile)
            task2.setStderr(logfile)
            task2.uses(logfile, link=peg.Link.OUTPUT)
            task2.uses(schemaAbh, link=peg.Link.INPUT)
            task2.uses(outparq, link=peg.Link.INPUT)
            task2.uses(outcsv, link=peg.Link.OUTPUT)
            dax.depends(parent=task1, child=task2)

            taskname = 'sed'
            task3 = peg.Job(name=taskname)
            task3.addProfile(peg.Profile(peg.Namespace.CONDOR, "request_memory", "2GB"))
            task3.addArguments("-f", sedScript, outcsv)
            dax.addJob(task3)
            logfile = peg.File("%s-%s.log" % (taskname, i, ))
            newcsv = peg.File("new-%s.csv" % (i, ))
            dax.addFile(logfile)
            task3.setStdout(newcsv)
            task3.setStderr(logfile)
            task3.uses(logfile, link=peg.Link.OUTPUT)
            task3.uses(newcsv, link=peg.Link.OUTPUT)
            task3.uses(outcsv, link=peg.Link.INPUT)
            task3.uses(sedScript, link=peg.Link.INPUT)
            dax.depends(parent=task2, child=task3)

            # My input csv files are larger than 1GB each and I am not splitting them for now
            taskname = 'partition'
            task4 = peg.Job(name=taskname)
            task4.addProfile(peg.Profile(peg.Namespace.CONDOR, "request_memory", "15GB"))
            outdir = os.path.join(chunkBaseFolder, 'chunksSet'+str(i))
            task4.addArguments("--verbose", "-c", partCfg, "--in.path", newcsv, "--out.dir", outdir)
            dax.addJob(task4)
            logfile = peg.File("%s-%s.log" % (taskname, i, ))
            dax.addFile(logfile)
            task4.setStdout(logfile)
            task4.setStderr(logfile)
            task4.uses(logfile, link=peg.Link.OUTPUT)
            task4.uses(newcsv, link=peg.Link.INPUT)
            task4.uses(partCfg, link=peg.Link.INPUT)
            dax.depends(parent=task3, child=task4)

            # Look for chunk files in the output folder of this partitiong
            # Cannot handle smaller job units at dax creation as the folder is not yet populated;
            # if we want smaller units, consider using dynamic subworkflow
            taskname = 'allocateChunk'
            task5 = peg.Job(name=taskname)
            task5.addProfile(peg.Profile(peg.Namespace.CONDOR, "request_memory", "2GB"))
            task5.addArguments(outdir, "--idFile", transIdFile)
            dax.addJob(task5)
            logfile = peg.File("%s-%s.log" % (taskname, i, ))
            dax.addFile(logfile)
            task5.setStdout(logfile)
            task5.setStderr(logfile)
            task5.uses(logfile, link=peg.Link.OUTPUT)
            task5.uses(transIdFile, link=peg.Link.INPUT)
            dax.depends(parent=task4, child=task5)
            dax.depends(parent=task0c, child=task5)

    return dax


if __name__ == "__main__":
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler())

    parser = argparse.ArgumentParser(description="Generate a DAX")
    parser.add_argument("-i", "--inputData", default="inparq.list",
                        help="a file including input data information")
    parser.add_argument("-o", "--outputFile", type=str, default="queso.dax",
                        help="file name for the output dax xml")
    args = parser.parse_args()

    dax = generateDax("qqpoc", inputData=args.inputData)
    with open(args.outputFile, "w") as f:
        dax.writeXML(f)
