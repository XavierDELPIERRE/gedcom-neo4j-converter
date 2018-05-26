from flask import Flask, g, request, send_from_directory, abort, request_started
from flask_restful import Resource, reqparse
import os
from flask_restful_swagger_2 import Api, swagger, Schema

from neo4j.v1 import GraphDatabase
from neo4j.util import watch
import logging
from sys import stdout


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

def peopleWithoutPartners(tx):
    for record in  tx.run("MATCH (a:Person)-[:NAME_IS]->(b) WHERE NOT (a)-[:PARTNER_OF]->() return a, b"):
        print(record["a"]["id"], record["a"]["sex"], record["b"]["surname"], ',', record["b"]["given"])
def peopleWithoutPartnersBySurname(tx, surname):
    for record in tx.run("MATCH (a:Person)-[:NAME_IS]->(b) WHERE NOT (a)-[:PARTNER_OF]->() return a, b",
           surname=surname):
        print(record["a"]["id"], record["a"]["sex"], record["b"]["surname"], ',', record["b"]["given"])

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "gedcom"))
watch("neo4j.bolt", logging.DEBUG, stdout)

app = Flask(__name__)
app.config['SECRET_KEY'] = 'super secret guy'
api = Api(app, title='Bloodline API', api_version='0.0.1')

def get_db():
    if not hasattr(g, 'gedcom'):
        g.neo4j_db = driver.session()
    return g.neo4j_db

@app.teardown_appcontext
def close_db(error):
    if hasattr(g, 'neo4j_db'):
        g.neo4j_db.close()

file = open('gedcom.ged', 'r')
array = file.read().rsplit('0 @')
file.close()

def serialize_person(person):
    return {
        'id': person['id'],
        'sex': person['sex']
    }

class PersonModel(Schema):
    type = 'object'
    properties = {
        'id': {
            'type': 'string',
        },
        'sex': {
            'type': 'string',
        }
    }

class PersonList(Resource):
    @swagger.doc({
        'tags': ['people'],
        'summary': 'Find all people',
        'description': 'Returns a list of people',
        'responses': {
            '200': {
                'description': 'A list of people',
                'schema': {
                    'type': 'array',
                    'items': PersonModel,
                }
            }
        }
    })
    def get(self):
        db = get_db()
        results = db.run(
            '''
            MATCH (a:Person) return a
        
            '''
        )
        return [serialize_person(record['a']) for record in results]


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
                print(id, husb, wife)
                session.write_transaction(add_family, id, husb, wife)

                for element in children:
                    if element.__contains__('1 CHIL @'):
                        childID = element.rsplit('1 CHIL @')[1].rsplit('@')[0]
                        #print(childID)
                        session.write_transaction(add_child, id, childID)

api.add_resource(PersonList, '/api/v0/people')

if __name__ == '__main__':
    app.debug = True
    # app.config['DATABASE_NAME'] = 'library.db'
    host = os.environ.get('IP', '0.0.0.0')
    port = int(os.environ.get('PORT', 8081))
    app.run(host=host, port=port)

#driver.session().read_transaction(peopleWithoutParentsBySurname, "Starnes")