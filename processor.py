from dotenv import load_dotenv
load_dotenv()
import csv
import mysql_import
import os

SELECT = 1
SELECT_WHERE = 6
JOIN_USING = 7
JOIN_ON = 8
INSERT = 3
DELETE = 4
UPDATE = 5

schema = None
data = []

def list_schemas():
    schemas = os.listdir(os.getcwd() + "/schemas")
    for file in schemas:
        print("- " + file.replace(".csv", ""))

def check_existing_schema(schema):
    path = catch_schema_path(schema)
    return os.path.exists(path)

def check_existing_table(schema, table):
    path = catch_table_path(schema, table)
    return os.path.exists(path)

def catch_schema_path(schema: str):
    path = (os.getcwd() + "/schemas/{}").format(schema)
    return path

def catch_table_path(schema, table):
    path = (catch_schema_path(schema) + "/tables/{}.csv").format(table)
    return os.path.exists(path)

def create_schema(schema):
    path = catch_schema_path(schema)
    os.mkdir(path)
    os.mkdir(path + "/tables")

def read_csv(path):
    with open(path, newline = '') as file:
        reader = csv.DictReader(file)
        data = []
        for row in reader:
            data.append(row)
        return data

def write_csv(path):
    return
    
def get_table_data(schema, table):
    if check_existing_table(schema, table):
        table_path = catch_table_path(schema, table)
        data = read_csv(table_path)
        return data


def query():

    confirm_schema = False
    confirm_query = False

    # Schema selection
    while not confirm_schema:
        print("Select schema: ")
        list_schemas()
        schema = input("> ")

        cs = input("Confirm Schema? (Y / N)")
        if cs.upper() == "Y":
            if check_existing_schema(schema):
                confirm_schema = True
            else:
                print("Error: Schema not found.")
   
    # Query definition
    while not confirm_query:
        print("Selected Schema: {}".format(schema))
        print("Insert query: ")
        query = input("> ")
        
        cq = input("Confirm query? (Y / N)")
        if cq.upper() == "Y":
            confirm_query = True

    process_query(query)

    return True

def process_query(query):

    query_list = query.split(' ')
    query_parts = [element.replace(',', '') for element in query_list]

    try:
        if "select" in query_parts:
            select_i = query_parts.index("select")
            select_column = query_parts[select_i + 1]

            if "from" in query_parts:
                from_i = query_parts.index("from")
                table = query_parts[from_i + 1]

                if "where" in query_parts:
                    where_i = query_parts.index("where")
                    where_column = query_parts[where_i + 1]

                    where_condition = query_parts[where_i + 2]
                    value = query_parts[where_i + 3]

                    data = [select_column, table, where_column, where_condition, value]
                    
                    if "order" in query_parts:
                        order_i = query_parts.index("order")
                        order_column = query_parts[order_i + 1]
                        data.append(order_column)                   

                    select_where(data)

                elif "join" in query_parts: 
                    join_i = query_parts.index("join")
                    join_table = query_parts[join_i + 1]

                    if "using" in query_parts:
                        using_i = query_parts.index("using")
                        using_column = query_parts[using_i + 1]

                        data = [select_column, table, join_table, using_column]

                        if "order" in query_parts:
                            order_i = query_parts.index("order")
                            order_column = query_parts[order_i + 1]
                            data.append(order_column)    
                            
                        join_using(data)

                    elif "on" in query_parts:
                        on_i = query_parts.index("on")
                        on_column_1 = query_parts[on_i + 1]
                        on_column_2 = query_parts[on_i + 3]

                        data = [select_column, table, join_table, on_column_1, on_column_2]

                        if "order" in query_parts:
                            order_i = query_parts.index("order")
                            order_column = query_parts[order_i + 1]  
                            data.append(order_column)  

                        join_on(data)
                      

        elif "insert" in query_parts:
            insert_i = query_parts.index("insert")
            insert_table = query_parts[insert_i + 2]

            if "values" in query_parts:
                values_i = query_parts.index("order")
                in_values = query_parts[values_i + 1]

                data = [insert_table, in_values]
                insert(data)

        elif "delete" in query_parts:
            delete_i = query_parts.index("delete")
            
            if "from" in query_parts:
                from_i = query_parts.index("from")
                table = query_parts[from_i + 1]

                if "where" in query_parts:
                    where_i = query_parts.index("where")
                    where_column = query_parts[where_i + 1]

                    where_condition = query_parts[where_i + 2]
                    value = query_parts[where_i + 3]

                    data = [table, where_column, where_condition, value]
                    delete(data)

        elif "update" in query_parts:
            update_i = query_parts.index("update")
            update_table = query_parts[update_i + 1]

            if "set" in query_parts:
                set_i = query_parts.index("set")
                set_column = query_parts[set_i + 1]

                if "where" in query_parts:
                    where_i = query_parts.index("where")
                    where_column = query_parts[where_i + 1]

                    where_condition = query_parts[where_i + 2]
                    value = query_parts[where_i + 3]

                    data = [update_table, set_column, where_column, where_condition, value]
                    update(data)

    except:
        print("Error: Invalid query.")
        
    return True


def select_where(data):
    select_column = data[0]
    table = data[1]
    where_column = data[2]
    where_condition = data[3]
    value = data[4]
    if data[5]:
        order_column = data[5]


def join_using(data):
    select_column = data[0]
    table = data[1]
    join_table = data[2]
    using_column = data[3]
    if data[4]:
        order_column = data[4]
        
def join_on(data):
    select_column = data[0]
    table = data[1]
    join_table = data[2]
    on_column_1 = data[3]
    on_column_2 = data[4]
    if data[5]:
        order_column = data[5]

def insert(data):
    insert_table = data[0]
    in_values = data[1]

    get_table_data(insert_table)


def delete(data):
    table = data[0]
    where_column = data[1]
    where_condition = data[2]
    value = data[3]

def update(data):
    update_table = data[0]
    set_column = data[1]
    where_column = data[2]
    where_condition = data[3]
    value   = data[4]


def data_import():
    answer= None
    while not (answer == "mysql" or answer == "postgres" or answer == "csv"):
        print("Selecione csv ou um servidor (csv / mysql / postgres): ")
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
       query()
    elif answer == "s":
        return False
    
    return True


if __name__ == "__main__":
    while main():
        continue