import sqlalchemy # type: ignore 
import csvTest as db
import csv
from datetime import datetime
from math import ceil

def displayTable(tableHandler, connectionHandler):

    columnNames = list(tableHandler.columns.keys())
    columnLengths = list(map(lambda x: (ceil(len(x)/10) * 10)-2, columnNames))
    columnLengths[0] += 2
    output = connectionHandler.execute(tableHandler.select()).fetchall()

    print("| ", end ='')
    for i in range(len(columnNames)):
        print(f"{columnNames[i]}" + ' '*(columnLengths[i]-len(columnNames[i])) + " | ", end='')

    print()

    for row in output:
        print("| ", end ='')
        for j in range(len(row)):
            print(f"{row[j]}" + ' '*(columnLengths[j]-len(str(row[j]))) + " | ", end='')
        
        print()

def differentCSV(filePath, engineFlag, connectionHandler, engineHandler, metadataHandler):
    metadataHandler.drop_all(connectionHandler)
    metadataHandler.clear()
    connectionHandler.close()

    return db.init(filePath, 1, engineHandler, connectionHandler)

def addTuple(tableHandler, connHandler, titles):
    inDate = input("insert date in the dd/mm/yyyy format : ")
    inType = input("insert type of transfer (e.g. DEB, CRE, FTP...) : ")
    inDesc = input("insert short description : ")
    inDeb = input("insert the outgoing amount (just press enter if none) : ")
    inCred = input("insert incoming amounmt (just press enter if none) : ")
    inBal = input("insert new balance : ")

    print()

    
    try:
        if inDeb == '':
            deb = None
        else:
            deb = float(inDeb)
    except:
        deb = None

    try:
        if inCred == '':
            cred = None
        else:
            cred = float(inCred)
    except:
        cred = None

    insert_statement = tableHandler.insert().values(
        {
            titles[0]: datetime.strptime(datetime.strptime(inDate, "%d/%m/%Y").strftime("%Y-%m-%d"), "%Y-%m-%d"),  # Date in YYYY-MM-DD format
            titles[1]: inType,
            titles[4]: inDesc,
            titles[5]: deb,
            titles[6]: cred,
            titles[7]: float(inBal)
        }
    )
    connHandler.execute(insert_statement)

def remTuple(tableHandler, connHandler):
    while 1:
        inID = input("Insert Id of tuple you want to remove : ")
        try:
            intID = int(inID)
        except:
            print("Not an integer")
            continue

        if intID < 1:
            print("Needs to be positive")
            continue
        else:
            break

    connHandler.execute(sqlalchemy.delete(tableHandler).where(tableHandler.c.ID == intID))


def exportBack(tableHandler, connHandler, file, titles, sc, ac):
    output = connHandler.execute(tableHandler.select()).fetchall()
    
    modifiedArray = [list(sub_array[1:]) for sub_array in output]
    modifiedArrayStr = [[str(val) if val is not None else "" for val in sub_array] for sub_array in modifiedArray]
    for row in modifiedArrayStr:
        row.insert(2, sc)
        row.insert(3, ac)

    with open("outputTest.csv", 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)

        csvwriter.writerow(titles)
        csvwriter.writerows(modifiedArrayStr)

def userQuery(tableHandler, connHandler, titles):
    columnNames = list(tableHandler.columns.keys())[1:]

    print("According to which attribute do you want to query the table?")
    num = 1
    for item in columnNames:
        print(f"{num}. {item}")
        num += 1

    choice = input("Please enter the right number : ")
    print()

    if choice == "1":
        val = input("Enter the date exactly in this format dd/mm/yyyy :")
        print("1. On that date")
        print("2. On or after")
        print("3. On or before")

        while 1:
            op = input("Enter the correct number : ")
            if op == "1":
                output = connHandler.execute(sqlalchemy.text(f"SELECT * FROM Account WHERE \"Transaction Date\" = \'{datetime.strptime(val, "%d/%m/%Y").strftime("%Y-%m-%d")}\'"))
                break
            elif op == "2":
                output = connHandler.execute(sqlalchemy.text(f"SELECT * FROM Account WHERE \"Transaction Date\" >= \'{datetime.strptime(val, "%d/%m/%Y").strftime("%Y-%m-%d")}\'"))
                break
            elif op == "3":
                output = connHandler.execute(sqlalchemy.text(f"SELECT * FROM Account WHERE \"Transaction Date\" <= \'{datetime.strptime(val, "%d/%m/%Y").strftime("%Y-%m-%d")}\'"))
                break
            else:
                continue
    elif choice == "2":
        val = input("Enter the exact transaction type : ")
        output = connHandler.execute(sqlalchemy.text(f"SELECT * FROM Account WHERE \"Transaction Type\" = \"{val}\""))
    elif choice == "3":
        val = input("Enter the exact description : ")
        output = connHandler.execute(sqlalchemy.text(f"SELECT * FROM Account WHERE \"Description\" = \"{val}\""))
    elif choice == "4":
        val = input("Enter the exact outgoing amount : ")

        print("1. That amount")
        print("2. That amount or more")
        print("3. That amount or less")

        while 1:
            op = input("Enter the correct number : ")
            if op == "1":
                output = connHandler.execute(sqlalchemy.text(f"SELECT * FROM Account WHERE \"Debit Amount\" = {val}"))
                break
            elif op == "2":
                output = connHandler.execute(sqlalchemy.text(f"SELECT * FROM Account WHERE \"Debit Amount\" >= {val}"))
                break
            elif op == "3":
                output = connHandler.execute(sqlalchemy.text(f"SELECT * FROM Account WHERE \"Debit Amount\" <= {val}"))
                break
            else:
                continue
    elif choice == "5":
        val = input("Enter the exact incoming amount : ")

        print("1. That amount")
        print("2. That amount or more")
        print("3. That amount or less")

        while 1:
            op = input("Enter the correct number : ")
            if op == "1":
                output = connHandler.execute(sqlalchemy.text(f"SELECT * FROM Account WHERE \"Credit Amount\" = {val}"))
                break
            elif op == "2":
                output = connHandler.execute(sqlalchemy.text(f"SELECT * FROM Account WHERE \"Credit Amount\" >= {val}"))
                break
            elif op == "3":
                output = connHandler.execute(sqlalchemy.text(f"SELECT * FROM Account WHERE \"Credit Amount\" <= {val}"))
                break
            else:
                continue
    elif choice == "6":
        val = input("Enter the exact balance : ")

        print("1. That amount")
        print("2. That amount or more")
        print("3. That amount or less")

        while 1:
            op = input("Enter the correct number : ")
            if op == "1":
                output = connHandler.execute(f"SELECT * FROM Account WHERE \"Balance\" = {val}")
                break
            elif op == "2":
                output = connHandler.execute(f"SELECT * FROM Account WHERE \"Balance\" >= {val}")
                break
            elif op == "3":
                output = connHandler.execute(f"SELECT * FROM Account WHERE \"Balance\" <= {val}")
                break
            else:
                continue

    columnLengths = list(map(lambda x: (ceil(len(x)/10) * 10)-2, columnNames))
    columnLengths[0] += 2
    
    print("| ", end ='')
    for i in range(len(columnNames)):
        print(f"{columnNames[i]}" + ' '*(columnLengths[i]-len(columnNames[i])) + " | ", end='')

    print()

    for row in output:
        print("| ", end ='')
        for j in range(len(row)-1):
            print(f"{row[j]}" + ' '*(columnLengths[j]-len(str(row[j]))) + " | ", end='')
        
        print()

    # print(f"{titles}")
    # print(f"{output}")


