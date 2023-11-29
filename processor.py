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

        print("Confirm Schema? (Y / N)")
        cs = input("> ")
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
        
        print("Confirm query? (Y / N)")
        cq = input("> ")
        if cq.upper() == "Y":
            confirm_query = True

    process_query(query)

    return True


def process_query(query):

    query_list = query.split(' ')
    query_parts = [element.replace(',', '') for element in query_list]

    try:
        if "select" in query_parts[0]:
            select_i = query_parts.index("select")
            select_columns = []
            while query_parts[select_i + 1] != "from":
                select_columns.append(query_parts[select_i + 1])

            if "from" in query_parts:
                from_i = query_parts.index("from")
                table = query_parts[from_i + 1]

                result = _from(select_columns, table)

                if "where" in query_parts:
                    where_i = query_parts.index("where")
                    
                    where_column = query_parts[where_i + 1]
                    where_condition = query_parts[where_i + 2]
                    value = query_parts[where_i + 3]

                    _where(where_column, where_condition, value)

                    if "or" in query_parts:
                        or_i = query_parts.index("or")
                        where_column2 = query_parts[or_i + 1]
                        where_condition2 = query_parts[or_i + 2]
                        value2 = query_parts[or_i + 3]

                        _where(where_column2, where_condition2, value2)

                    elif "and" in query_parts:
                        or_i = query_parts.index("or")
                        where_column2 = query_parts[or_i + 1]
                        where_condition2 = query_parts[or_i + 2]
                        value2 = query_parts[or_i + 3]

                        _and(where_column, where_condition, value)
                    
                    if "order" in query_parts:
                        order_i = query_parts.index("order")
                        order_column = query_parts[order_i + 1]                
                        
                elif "join" in query_parts: 
                    join_i = query_parts.index("join")
                    join_table = query_parts[join_i + 1]

                    _join(join_table)

                    if "using" in query_parts:
                        using_i = query_parts.index("using")
                        using_column = query_parts[using_i + 1]

                        _using(using_column)

                    elif "on" in query_parts:
                        on_i = query_parts.index("on")
                        on_column_1 = query_parts[on_i + 1]
                        on_column_2 = query_parts[on_i + 3]

                        _on(on_column_1, on_column_2)

                if "order" in query_parts:
                    order_i = query_parts.index("order")
                    order_column = query_parts[order_i + 1]  
                        
                    _order(order_column)

        elif "insert" in query_parts:
            insert_i = query_parts.index("insert")
            insert_table = query_parts[insert_i + 2]

            if "values" in query_parts:
                values_i = query_parts.index("order")
                in_values = query_parts[values_i + 1]

                insert(insert_table, in_values)

        elif "delete" in query_parts:
            
            if "from" in query_parts:
                from_i = query_parts.index("from")
                table = query_parts[from_i + 1]

                _from(None, table)

                if "where" in query_parts:
                    where_i = query_parts.index("where")
                    where_column = query_parts[where_i + 1]

                    where_condition = query_parts[where_i + 2]
                    value = query_parts[where_i + 3]

                    delete(table, where_column, where_condition, value)

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

                    update(update_table, set_column, where_column, where_condition, value)

    except:
        print("Error: Invalid query.")
        
    return True

def _from(columns, table):
    global schema

    data = []

    data = get_table_data(schema, table)

    if columns == None:
        return data

    for row in data:
        output = []
        for key in iter(row):
            if key in columns:
                output.append(row[key])  
    
    return output

def _where(column, condition, value):
    data = []
    data = _from(column)

    for row in data:
        output = []´
        if row

def _and():

def _join():

def _using():

def _on():

def _order():

def insert():

def delete():

def update():



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