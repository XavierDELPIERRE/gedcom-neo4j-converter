import pandas as pd
import os
import importlib
import urllib
from urllib import parse
from py2neo import Graph, authenticate, Node, Relationship


#create paths
currentFolder = os.getcwd() + os.sep
personPath = currentFolder + 'persons.csv'
familyPath = currentFolder + 'families.csv'

#config params
serverURL = 'localhost:7474'
dbURL = 'http://localhost:7474/db/data'
account = "neo4j"
password = "gedcom"

#connect to DB
authenticate(serverURL, user=account, password=password)
graph = Graph(dbURL)

#get os file URI
def path2url(path):
    return parse.urljoin('file:', urllib.request.pathname2url(path))

#cypher params
personname = "Person"

#BATCH IMPORT TO NEO4J FROM CSV FILE
#import node
cyper = "LOAD CSV WITH HEADERS FROM '%s' AS csvLine " % (path2url(personPath)) + " CREATE (p:"+personname+" { id: csvLine.id, gedcomID: csvLine.gedcomID, sex: csvLine.sex})"
print(cyper)
graph.run(cyper)
