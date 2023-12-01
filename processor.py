from dotenv import load_dotenv
load_dotenv()
import csv
import mysql_import
import postgres_import
import csv_import
import os

SELECT  = "escolhe" 
UPDATE  = "altere"
SET     = "mete"
INSERT  = "bota"
DELETE  = "apaga"
INTO    = "dentro"
VALUES  = "paradas"
FROM    = "de"
JOIN    = "cola"
ON      = "naonde"
USING   = "usando" 
WHERE   = "se"
AND     = "e"
OR      = "ou"
ORDER   = "arruma"

schema = None
commands = {}

def list_schemas():
    schemas = os.listdir(os.getcwd() + "/schemas")
    for file in schemas:
        print("- " + file.replace(".csv", ""))

def check_existing_schema(schema):
    path = catch_schema_path(schema)
    return os.path.exists(path)

def check_existing_table(schema: str, table: str):
    path = catch_table_path(schema, table)
    return os.path.exists(path)

def catch_schema_path(schema: str):
    path = (os.getcwd() + "/schemas/{}").format(schema)
    return path

def catch_table_path(schema: str, table: str):
    path = (catch_schema_path(schema) + "/tables/{}.csv").format(table)
    return path

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

def write_csv(table: str, cursor, colum_names: list, schema: str) -> bool:
    path_for_file = catch_table_path(table, schema)
    table_data = []

    for row in cursor:
        table_data.append(row)

    # headers = cursor.column_names

    with open(path_for_file, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=colum_names)
        writer.writeheader()
        writer.writerows(table_data)
    
def get_table_data(schema, table):
    if check_existing_table(schema, table):
        table_path = catch_table_path(schema, table)
        data = read_csv(table_path)
        return data


def query():

    global schema
    global commands

    commands = {
        "escolhe": None,            # SELECT
        "altere": None,             # UPDATE
        "mete": None,               # SET
        "bota": None,               # INSERT
        "apaga": None,              # DELETE
        "dentro": None,             # INTO
        "paradas": None,            # VALUES
        "de": None,                 # FROM
        "cola": None,               # JOIN
        "naonde": None,             # ON
        "usando": None,             # USING    
        "se": None,                 # WHERE
        "e": None,                  # AND
        "ou": None,                 # OR
        "arruma": None,             # ORDER BY 
    }

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
    print(query_parts)

    try:
        if SELECT in query_parts:
            select_i = query_parts.index(SELECT)
            select_columns = []
            i = 0
            while query_parts[select_i + i] != FROM:
                select_columns.append(query_parts[select_i + i])
                i += 1
             
            if FROM in query_parts:
                from_i = query_parts.index(FROM)
                table = query_parts[from_i + 1]

                result = _from(select_columns, table)
                from_data = result

                if WHERE in query_parts:
                    where_i = query_parts.index(WHERE)
                    
                    where_column = query_parts[where_i + 1]
                    where_condition = query_parts[where_i + 2]
                    value = query_parts[where_i + 3]

                    result = _where(from_data, where_column, where_condition, value)
                    where_data = result

                    if OR in query_parts:
                        or_i = query_parts.index(OR)
                        where_column2 = query_parts[or_i + 1]
                        where_condition2 = query_parts[or_i + 2]
                        value2 = query_parts[or_i + 3]

                        result = _or(where_data, where_column2, where_condition2, value2)

                    elif AND in query_parts:
                        and_i = query_parts.index(AND)
                        where_column2 = query_parts[and_i + 1]
                        where_condition2 = query_parts[and_i + 2]
                        value2 = query_parts[and_i + 3]

                        result = _and(where_data, where_column, where_condition, value)
                    
                    if ORDER in query_parts:
                        order_i = query_parts.index(ORDER)
                        order_column = query_parts[order_i + 1]     

                        _order(result, order_column)           
                        
                elif JOIN in query_parts: 
                    join_i = query_parts.index(JOIN)
                    join_table = query_parts[join_i + 1]

                    if USING in query_parts:
                        using_i = query_parts.index(USING)
                        using_column = query_parts[using_i + 1] 

                        _using(from_data, join_table, using_column)

                    elif ON in query_parts:
                        on_i = query_parts.index(ON)
                        on_column_1 = query_parts[on_i + 1]
                        on_column_2 = query_parts[on_i + 3]

                        _on(from_data, join_table, on_column_1, on_column_2)

                if ORDER in query_parts:
                    order_i = query_parts.index(ORDER)
                    order_column = query_parts[order_i + 1]  
                        
                    _order(order_column)

        elif INSERT in query_parts:
            insert_i = query_parts.index(INSERT)
            insert_table = query_parts[insert_i + 2]

            if VALUES in query_parts:
                values_i = query_parts.index(ORDER)
                in_values = query_parts[values_i + 1]

                _insert(insert_table, in_values)

        elif DELETE in query_parts:
            
            if FROM in query_parts:
                from_i = query_parts.index(FROM)
                table = query_parts[from_i + 1]


                if WHERE in query_parts:
                    where_i = query_parts.index(WHERE)
                    where_column = query_parts[where_i + 1]

                    where_condition = query_parts[where_i + 2]
                    value = query_parts[where_i + 3]

                    _delete(table, where_column, where_condition, value)

        elif UPDATE in query_parts:
            update_i = query_parts.index(UPDATE)
            update_table = query_parts[update_i + 1]

            if SET in query_parts:
                set_i = query_parts.index(SET)
                set_column = query_parts[set_i + 1]

                if WHERE in query_parts:
                    where_i = query_parts.index(WHERE)
                    where_column = query_parts[where_i + 1]

                    where_condition = query_parts[where_i + 2]
                    value = query_parts[where_i + 3]

                    _update(update_table, set_column, where_column, where_condition, value)

    except:
        print("Error: Invalid query.")

    print(result)
        
    return True

def _from(columns, table):
    global schema

    data = []
    data = get_table_data(schema, table)

    if "*" in columns:
        return data

    output = []

    for row in data:
        filtered_row = {}
        for key in iter(row):
            if key in columns:
                filtered_row[key] = row[key]
                output.append(filtered_row)  
    
    return output

def _where(from_data, column, condition, value):

    output = [] 

    for row in from_data:
        filtered_row = {}
        if condition == ">":
            if row.get(column) > value.strip():
                filtered_row[column] = row[column]
                output.append(filtered_row)
        elif condition == "<":
            if row.get(column) < value.strip():
                filtered_row[column] = row[column]
                output.append(filtered_row)
        elif condition == ">=":
            if row.get(column) >= value.strip():
                filtered_row[column] = row[column]
                output.append(filtered_row)
        elif condition == "<=":
            if row.get(column) <= value.strip():
                filtered_row[column] = row[column]
                output.append(filtered_row)
        elif condition == "=":
            if row.get(column) == value.strip():
                filtered_row[column] = row[column]
                output.append(filtered_row)

    return output

def _or(where_data, column, condition, value):
    data = _where(column, condition, value)

    for row in where_data:
        data.append(row)

    return data
        

def _and(where_data, column, condition, value):
    data = _where(column, condition, value)

    output = []

    for row in data:
        if row in where_data:
            output.append(row)

    return output

def _order(data, order_column):
    if order_column in data[0]:
        data.sort(key = lambda x: int(x[order_column])) 

    return data

def _on(from_data, join_table, column1, column2):
    data = _from("*", join_table)
    
    for row in data:
        if row.get(column2) in from_data.get(column1):
            data.append(row)

def _insert(table, values):
    
    return 

def _update(update_table, set_column, where_column, where_condition, value):
    data = _where(where_column, where_condition, value)

    

    return

def _delete(table, where_column, where_condition, value):
    data = _from("*", table)
    delete_data = _where(where_column, where_condition, value)

    for row in data:
        if row in delete_data:
            data.remove(row)

    return data



def data_import():
    answer = None
    while not (answer == "mysql" or answer == "postgres" or answer == "csv" or answer == "ml" or answer == "ps"):
        print("Select csv or a server (csv | mysql | postgres): ")
        answer = input(">> ")

    if answer == "mysql" or answer == "ml":
        mysql_import.mysqlimport()
    elif answer == "postgres" or answer == "ps":
        postgres_import.postgresimport()
    elif answer == "csv":
        csv_import.csv_import()

    return


def main():
    # wait for user interaction 
    answer = None
    while not (answer == "i" or answer == "c" or answer == "s"):
        print("Importar, consultar ou sair? (i | c | s)")
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