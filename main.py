import pyfiglet as pf # type: ignore
import csvTest as csvImp
import functions as fs
import os

def printTop(sc, ac, bal):
    print()
    print(f"{pf.figlet_format("BANK API'S BAD")}")
    print(f"Sort Code : {sc}, Account Number : {ac}, Balance : {bal}")
    print()

def main():
    print(pf.figlet_format("BANK API'S BAD"))

    file = input("Please input the csv file path : ")
    
    while 1:
        if os.path.isfile(file):
            break
        else:
            file = input("File doesn't exist, enter valid file : ")

    sc, ac, bal, tableHandler, connHandler, engineHandler, metadataHandler, titles = csvImp.init(file, 0, 0, 0)

    printTop(sc, ac, bal)

    mainMenuFlag = 0
    while 1:

        if mainMenuFlag == 1:
            printTop(sc,ac,bal)

        print("1. Display everything")
        print("2. Choose a different csv file.")
        #print("Custom query")
        print("3. Add tuple")
        print("4. Remove Tuple")
        print("5. User Query")
        print("6. Export to csv file")
        print("7. Exit")
        choice = input("Choose an option : ")

        
        if choice == "1":
            fs.displayTable(tableHandler, connHandler)
            mainMenuFlag = 1
        elif choice == "2":
            file = input("Please input the csv file path : ")
    
            while 1:
                if os.path.isfile(file):
                    break
                else:
                    file = input("File doesn't exist, enter valid file : ")
                    
            sc, ac, bal, tableHandler, connHandler, engineHandler, metadataHandler, titles = fs.differentCSV(file, 1, connHandler, engineHandler, metadataHandler)
        elif choice == "3":
            fs.addTuple(tableHandler, connHandler, titles)
            mainMenuFlag = 1
        elif choice == "4":
            fs.remTuple(tableHandler, connHandler)
            mainMenuFlag = 1
        elif choice == "5":
            fs.userQuery(tableHandler, connHandler, titles)
            mainMenuFlag = 1
        elif choice == "6":
            fs.exportBack(tableHandler, connHandler, file, titles, sc, ac)
            mainMenuFlag = 1
        elif choice == "7":
            exit()
        else:
            print("No such option exists")
            mainMenuFlag = 0


main()
        


