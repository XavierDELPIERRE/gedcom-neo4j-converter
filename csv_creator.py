import uuid

# Get GEDCOM
file = open('gedcom.ged', 'r')
array = file.read().rsplit('0 @')
file.close()

personcsv = open('persons.csv', "w")
namescsv = open('names.csv', "w")
familycsv = open('families.csv', "w")
childrencsv = open('children.csv', "w")
# "w" indicates that you're writing strings to the file

columnTitleRow = "id, gedcomID, sex\n"
personcsv.write(columnTitleRow)

columnTitleRow = "personGedcomID, given, surname\n"
namescsv.write(columnTitleRow)

columnTitleRow = "id, gedcomID, husb, wife\n"
familycsv.write(columnTitleRow)

columnTitleRow = "personGedcomID, familyGedcomID\n"
childrencsv.write(columnTitleRow)

#CSV Files
for e in array:
    if e[0] == 'I':
        arr = e.rsplit('\n')
        id = arr[0].rsplit('@ INDI')[0]

        for element in arr:
            if element.__contains__('2 GIVN '):
                given = element.rsplit('2 GIVN ')[1]
            elif element.__contains__('2 SURN '):
                surn = element.rsplit('2 SURN ')[1]
            elif element.__contains__('1 SEX '):
                sex = element.rsplit('1 SEX ')[1]

        row = str(uuid.uuid4()) + ',' + id + ',' + sex + '\n'
        personcsv.write(row)
        row = id + ',' + given + ',' + surn + '\n'
        namescsv.write(row)
    elif e[0] == 'F':
        arr = e.rsplit('\n')
        id = arr[0].rsplit('@ FAM')[0]
        children = []
        husb=''
        wife=''
        for element in arr:
            if element.__contains__('1 HUSB @'):
                husb = element.rsplit('1 HUSB @')[1].rsplit('@')[0]
            elif element.__contains__('1 WIFE @'):
                wife = element.rsplit('1 WIFE @')[1].rsplit('@')[0]
            elif element.__contains__('1 CHIL @'):
                children.append(element)

        row = str(uuid.uuid4()) + ',' + id + ',' + husb + ',' + wife + '\n'
        familycsv.write(row)

        for element in children:
            if element.__contains__('1 CHIL @'):
                childID = element.rsplit('1 CHIL @')[1].rsplit('@')[0]
                row = childID + ',' + id + '\n'
                childrencsv.write(row)