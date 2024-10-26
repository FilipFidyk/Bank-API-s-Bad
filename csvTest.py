import csv
from datetime import datetime, date
import sqlalchemy as db # type: ignore

def importCSV(file):
    with open(file, newline='') as csvFile:
        csv_reader = csv.reader(csvFile, delimiter=',')
        lineCount = 0
        data = []
        
        for row in csv_reader:
            if lineCount == 0:
                # Save header row separately
                titles = row
            else:
                data.append(row)
            lineCount += 1
        
        return titles, data
    
def findAccState(titles, data):
    sortCode =""
    accNum = ""
    balance = ""

    for i in range(len(titles)):
        if "sort code" in titles[i].lower():
            sortCode = data[0][i]
        elif "account number" in titles[i].lower():
            accNum = data[0][i]
        elif "balance" in titles[i].lower():
            balance = data[0][i]

    return sortCode, accNum, balance

def findType(title):
    if "date" in title.lower():
        return db.Date, None
    elif "amount" in title.lower():
        return db.Float, 0.0
    elif "balance" in title.lower():
        return db.Float, 0.0
    else:
        return db.String(255), None

def createDict(titles, data):

    # col_name, col_type, PK, nullable, default
    dbHeaders = [("ID", db.Integer, True, False, None)]
    

    for i in range(len(titles)):
        if "sort code" in titles[i].lower():
            continue
        elif "account number" in titles[i].lower():
            continue
        else:
            headerType, default = findType(titles[i])
            dbHeaders.append((titles[i], headerType, False, True, default))

    return dbHeaders

def createDB(titles, data, engineFlag, existingEngine, connectionHandler):
    engine = 0
    conn = 0
    if engineFlag == 0:
        engine = db.create_engine('sqlite:///datacamp.sqlite')
    else:
        engine = existingEngine

    conn = engine.connect()
    metadata = db.MetaData()
    transaction_table = db.Table('Account', metadata)

    columns = createDict(titles, data)

    for col_name, col_type, is_primary, nullable, default in columns:
        if is_primary:
            transaction_table.append_column(db.Column(col_name, col_type(), primary_key=True))
        else:
            if default is not None:
                if isinstance(col_type, db.String):   
                    transaction_table.append_column(db.Column(col_name, col_type, default=default, nullable=nullable))
                else:
                    transaction_table.append_column(db.Column(col_name, col_type(), default=default, nullable=nullable))
            else:
                if isinstance(col_type, db.String):   
                    transaction_table.append_column(db.Column(col_name, col_type, nullable=nullable))
                else:
                    transaction_table.append_column(db.Column(col_name, col_type(), nullable=nullable))

    metadata.create_all(engine)

    deb = 0
    cred = 0

    num = len(data)
    for i in range(len(data)):

        try:
            deb = float(data[i][5])
        except:
            deb = None

        try:
            cred = float(data[i][6])
        except:
            cred = None

        insert_statement = transaction_table.insert().values(
            {
                "ID": num,
                titles[0]: datetime.strptime(datetime.strptime(data[i][0], "%d/%m/%Y").strftime("%Y-%m-%d"), "%Y-%m-%d"),  # Date in YYYY-MM-DD format
                titles[1]: data[i][1],
                titles[4]: data[i][4],
                titles[5]: deb,
                titles[6]: cred,
                titles[7]: float(data[i][7])
            }
        )
        num -= 1
        conn.execute(insert_statement)

    # for row in data:
    #     print(row)

    return transaction_table, conn, engine, metadata


def init(file, engineFlag, existingEngine, connectionHandler):
    
    titles, data = importCSV(file)
    sortCode, accNum, balance = findAccState(titles, data)
    tableHandler, connHandler, engineHandler, metadata = createDB(titles, data, engineFlag, existingEngine, connectionHandler)
    return sortCode, accNum, balance, tableHandler, connHandler, engineHandler, metadata, titles


