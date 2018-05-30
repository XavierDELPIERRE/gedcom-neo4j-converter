from neo4j.v1 import GraphDatabase
from neo4j.util import watch
import logging
from sys import stdout

#MATCH(name: Name) RETURN DISTINCT name.surname, count(*) ORDER BY count(*) DESC

def add_person(tx, id, given, surn, sex):
    tx.run("CREATE (a:Person {id: $id, sex:$sex}) "
           "CREATE  (b:Name {given: $given, surname:$surn}) "
           "CREATE  (a)-[:NAME_IS]->(b)",
           id=id, sex=sex, given=given, surn=surn)
def add_family(tx, id, husb, wife):
    tx.run("MERGE (a:Person {id: $husb}) "
           "MERGE (b:Person {id: $wife}) "
           "MERGE (c:Family {id: $id}) "
           "MERGE  (a)-[:PARTNER_OF]->(c) "
           "MERGE  (b)-[:PARTNER_OF]->(c)",
           id=id, husb=husb, wife=wife)
def add_child(tx, id, childID):
    tx.run("MERGE (a:Person {id: $childID}) "
           "MERGE (c:Family {id: $id}) "
           "MERGE  (a)-[:CHILD_OF]->(c)",
           id=id, childID=childID)
def printPeople(tx):
    for record in  tx.run("MATCH (a:Person) return a"):
        print(record["a"]["id"], record["a"]["sex"])
def peopleWithoutParentsBySurname(tx, surname):
    for record in  tx.run("MATCH (a:Person)-[:NAME_IS]->(b:Name {surname: $surname}) WHERE NOT (a)-[:CHILD_OF]->() return a, b",
           surname=surname):
        print(record["a"]["id"], record["a"]["sex"], record["b"]["surname"], ',', record["b"]["given"])
def peopleWithoutParents(tx):
    for record in  tx.run("MATCH (a:Person)-[:NAME_IS]->(b) WHERE NOT (a)-[:CHILD_OF]->() return a, b"):
        print(record["a"]["id"], record["a"]["sex"], record["b"]["surname"], ',', record["b"]["given"])
def uniqueSurnames(tx):
    for record in  tx.run("MATCH(name: Name) RETURN DISTINCT name.surname ORDER BY name.surname"):
        print(record['name.surname'])
def peopleWithoutPartners(tx):
    for record in  tx.run("MATCH (a:Person)-[:NAME_IS]->(b) WHERE NOT (a)-[:PARTNER_OF]->() return a, b"):
        print(record["a"]["id"], record["a"]["sex"], record["b"]["surname"], ',', record["b"]["given"])
def peopleWithoutPartnersBySurname(tx, surname):
    for record in tx.run("MATCH (a:Person)-[:NAME_IS]->(b) WHERE NOT (a)-[:PARTNER_OF]->() return a, b",
           surname=surname):
        print(record["a"]["id"], record["a"]["sex"], record["b"]["surname"], ',', record["b"]["given"])

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "PASSWORD"))
watch("neo4j.bolt", logging.DEBUG, stdout)


file = open('gedcom.ged', 'r')
array = file.read().rsplit('0 @')
file.close()

def importGEDCOM(file):
    with driver.session() as session:
        for e in array:
            if e[0] =='I':
                arr = e.rsplit('\n')
                id = arr[0].rsplit('@ INDI')[0]

                for element in arr:
                    if element.__contains__('2 GIVN '):
                        given = element.rsplit('2 GIVN ')[1]
                    elif element.__contains__('2 SURN '):
                        surn = element.rsplit('2 SURN ')[1]
                    elif element.__contains__('1 SEX '):
                        sex = element.rsplit('1 SEX ')[1]
                    elif element.__contains__('1 FAMC @'):
                        pass
                    elif element.__contains__('1 FAMCS @'):
                        pass
                session.write_transaction(add_person, id, given, surn, sex)
            elif e[0] == 'F':
                arr = e.rsplit('\n')
                id = arr[0].rsplit('@ FAM')[0]
                children =[]
                for element in arr:
                    if element.__contains__('1 HUSB @'):
                        husb = element.rsplit('1 HUSB @')[1].rsplit('@')[0]
                    elif element.__contains__('1 WIFE @'):
                        wife = element.rsplit('1 WIFE @')[1].rsplit('@')[0]
                    elif element.__contains__('1 CHIL @'):
                        children.append(element)
                session.write_transaction(add_family, id, husb, wife)

                for element in children:
                    if element.__contains__('1 CHIL @'):
                        childID = element.rsplit('1 CHIL @')[1].rsplit('@')[0]
                        session.write_transaction(add_child, id, childID)


