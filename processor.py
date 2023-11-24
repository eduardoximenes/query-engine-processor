from dotenv import load_dotenv
load_dotenv()
import csv
import mysql_import
import os


schema = None
commands = {}

def check_existing_schema(schema):
    path = catch_schema_path(schema)
    return os.path.exists(path)

def catch_schema_path(schema: str):
    path = (os.getcwd() + "/schemas/{}").format(schema)
    return path

def create_schema(schema):
    path = catch_schema_path(schema)
    os.mkdir(path)
    os.mkdir(path + "/tables")

def query():
    return 

def data_import():
    answer= None
    while not (answer == "mysql" or answer == "postgres" or answer == "csv"):
        print("Selecione csv ou um servidor (mysql ou postgres): ")
        answer = input(">> ")
    if answer == "mysql":
        mysql_import.mysqlimport()
    
    return

def main():
    # wait for user interaction 
    answer = None
    while not (answer == "i" or answer == "c" or answer == "s"):
        print("Importar, consultar ou sair? (i/c/s)")
        answer = input(">> ")
    if answer == "i":
        data_import()
    elif answer == "c":
       print ("Não implementado.")
       # query()
    elif answer == "s":
        return False
    
    return True


if __name__ == "__main__":
    while main():
        continue