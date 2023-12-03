from dotenv import load_dotenv
load_dotenv()
import csv
import mysql_import
import postgres_import
import csv_import
import os

SELECT  = "selecione"
UPDATE  = "atualize"
SET     = "defina"
INSERT  = "insira"
DELETE  = "apague"
INTO    = "na"
VALUES  = "valores"
FROM    = "de"
JOIN    = "junte"
ON      = "em"
USING   = "usando"
WHERE   = "onde"
AND     = "e"
OR      = "ou"
ORDER   = "ordene"

schema = None
commands = {}
result = []

def list_schemas():
    schemas = os.listdir(os.getcwd() + "\\schemas")
    for file in schemas:
        print("- " + file.replace(".csv", ""))

def check_existing_schema(schema):
    path = catch_schema_path(schema)
    return os.path.exists(path)

def check_existing_table(schema: str, table: str):
    path = catch_table_path(schema, table)
    return os.path.exists(path)

def catch_schema_path(schema: str):
    path = (os.getcwd() + "\\schemas\\{}").format(schema)
    return path

def catch_table_path(schema: str, table: str):
    path = (catch_schema_path(schema) + "\\tables\\{}.csv").format(table)
    return path

def create_schema(schema):
    path = catch_schema_path(schema)
    os.mkdir(path)
    os.mkdir(path + "\\tables")

def read_csv(path):
    with open(path, newline = '') as file:
        reader = csv.DictReader(file)
        data = []
        for row in reader:
            data.append(row)
        return data

def write_csv(table: str, cursor, colum_names: list, schema: str) -> bool:
    path_for_file = catch_table_path(schema, table)
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
        "selecione": None,          # SELECT
        "atualize": None,           # UPDATE
        "defina": None,             # SET
        "insira": None,             # INSERT
        "apague": None,             # DELETE
        "na": None,                 # INTO
        "valores": None,            # VALUES
        "de": None,                 # FROM
        "junte": None,              # JOIN
        "em": None,                 # ON
        "usando": None,             # USING    
        "onde": None,               # WHERE
        "e": None,                  # AND
        "ou": None,                 # OR
        "ordene": None,             # ORDER BY 
    }

    confirm_schema = False
    confirm_query = False

    # Schema selection
    while not confirm_schema:
        print("Selecione Schema: ")
        list_schemas()
        schema = input(">> ")

        print("Confirmar Schema? (S | N)")
        cs = input(">> ")
        if cs.upper() == "S":
            if check_existing_schema(schema):
                confirm_schema = True
            else:
                print("Erro: Schema não encontrado.")
   
    # Query definition
    while not confirm_query:
        print("Schema Selecionado: {}".format(schema))
        print("Insira query: ")
        query = input(">> ")
        
        print("Confirmar query? (S | N)")
        cq = input(">> ")
        if cq.upper() == "S":
            confirm_query = True

    process_query(query)

    return True


def process_query(query):

    global result
    result = []

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

            commands[SELECT] = select_columns

            if FROM in query_parts:
                from_i = query_parts.index(FROM)
                table = query_parts[from_i + 1]

                commands[FROM] = table

                result = _select()

                if WHERE in query_parts:
                    where_i = query_parts.index(WHERE)
                    
                    where_column = query_parts[where_i + 1]
                    where_condition = query_parts[where_i + 2]
                    value = query_parts[where_i + 3]

                    commands[WHERE] = where_column, where_condition, value

                    if OR in query_parts:
                        or_i = query_parts.index(OR)
                        where_column2 = query_parts[or_i + 1]
                        where_condition2 = query_parts[or_i + 2]
                        value2 = query_parts[or_i + 3]

                        commands[OR] = where_column2, where_condition2, value2

                    elif AND in query_parts:
                        and_i = query_parts.index(AND)
                        where_column2 = query_parts[and_i + 1]
                        where_condition2 = query_parts[and_i + 2]
                        value2 = query_parts[and_i + 3]

                        commands[AND] = where_column2, where_condition2, value2

                    result = _where()       
                        
                elif JOIN in query_parts: 
                    join_i = query_parts.index(JOIN)
                    join_table = query_parts[join_i + 1]

                    commands[JOIN] = join_table

                    if USING in query_parts:
                        using_i = query_parts.index(USING)
                        using_column = query_parts[using_i + 1] 

                        commands[USING] = using_column

                        _using()

                    elif ON in query_parts:
                        on_i = query_parts.index(ON)
                        on_column_1 = query_parts[on_i + 1]
                        on_column_2 = query_parts[on_i + 3]

                        commands[ON] = on_column_1, on_column_2

                        _on()

                if ORDER in query_parts:
                    order_i = query_parts.index(ORDER)
                    order_column = query_parts[order_i + 1]  
                    order_direc = query_parts[order_i + 2]
                        
                    commands[ORDER] = order_column, order_direc

                    result = _order()

        elif INSERT in query_parts:
            insert_i = query_parts.index(INSERT)
            insert_table = query_parts[insert_i + 2]

            commands[INSERT] = insert_table
            commands[FROM] = insert_table

            if VALUES in query_parts:
                values_i = query_parts.index(VALUES)
                in_values = []
                i = 1
                while values_i + i < len(query_parts) :
                    in_values.append(query_parts[values_i + i])
                    i += 1

                commands[VALUES] = in_values    

                _insert()

        elif DELETE in query_parts:
            commands[SELECT] = ["*"]
            
            if FROM in query_parts:
                from_i = query_parts.index(FROM)
                table = query_parts[from_i + 1]

                commands[FROM] = table

                result = _select()

                if WHERE in query_parts:
                    where_i = query_parts.index(WHERE)
                    
                    where_column = query_parts[where_i + 1]
                    where_condition = query_parts[where_i + 2]
                    value = query_parts[where_i + 3]

                    commands[WHERE] = where_column, where_condition, value

                    _delete()

        elif UPDATE in query_parts:
            update_i = query_parts.index(UPDATE)
            update_table = query_parts[update_i + 1]

            commands[UPDATE] = update_table
            commands[FROM] = update_table
            commands[SELECT] = ["*"]

            if SET in query_parts:
                set_i = query_parts.index(SET)
                set_columns = []
                set_values = []
                i = 1

                while query_parts[set_i + i] != WHERE:
                    set_columns.append(query_parts[set_i + i])
                    set_values.append(query_parts[set_i + 2 + i])
                    i += 3
                    
                commands[SET] = set_columns, set_values

                result = _select()

                if WHERE in query_parts:
                    where_i = query_parts.index(WHERE)

                    where_column = query_parts[where_i + 1]
                    where_condition = query_parts[where_i + 2]
                    value = query_parts[where_i + 3]

                    commands[WHERE] = where_column, where_condition, value

                    _update()

    except:
        print("Erro: Query inválida.")

    print(result)

    return True

def _select():

    global schema
    columns = commands[SELECT]
    table = commands[FROM]
   
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

def _where():

    column = commands[WHERE][0]
    condition = commands[WHERE][1]
    value = commands[WHERE][2]

    output = []

    data = _where_condition(column, condition, value)

    if commands[OR] != None:
        column2 = commands[OR][0]
        condition2 = commands[OR][1]
        value2 = commands[OR][2]
        data2 = _where_condition(column2, condition2, value2)

        for row in data:
            output.append(row)
        for row2 in data2: 
            output.append(row2)

    elif commands[AND] != None:
        column2 = commands[AND][0]
        condition2 = commands[AND][1]
        value2 = commands[AND][2]
        data2 = _where_condition(column2, condition2, value2)
        
        for row in data:
            if (row in data2):
                output.append(row)

    else:
        return data
    
    return output

def _where_condition(column, condition, value):
    
    global result

    output = []

    for row in result:
        if condition == ">":
            if float(row.get(column)) > float(value.strip()):
                output.append(row)
        elif condition == "<":
            if float(row.get(column)) < float(value.strip()):
                output.append(row)
        elif condition == ">=":
            if float(row.get(column)) >= float(value.strip()):
                output.append(row)
        elif condition == "<=":
            if float(row.get(column)) <= float(value.strip()):
                output.append(row)
        elif condition == "!=":
            if row.get(column) != value.strip():
                output.append(row)
        elif condition == "=":
            if row.get(column) == value.strip():
                output.append(row)

    return output

def _order():

    global result

    order_column = commands[ORDER][0]
    order_direc = commands[ORDER][1]

    desc = False

    if order_direc.lower() == "decrescente":
        desc = True

    output = sorted(result, key=lambda x: x[order_column], reverse=desc)

    return output

def _on():
    from_data = commands[FROM]
    join_data = commands[JOIN]
    column1 = commands[ON][0]
    column2 = commands[ON][1]


    for row in join_data:
        if row.get(column2) in from_data.get(column1):
            return

def _using():
    return

def _insert():

    global schema

    try:
        insert_table = commands[INSERT]
        values = commands[VALUES]

        commands[SELECT] = ["*"]

        data = _select()

        headers = list(data[0])

        if len(headers) != len(values):
            print("Error: Wrong insert arguments")
            return False

        new_row = {field: value for field, value in zip(data[0].keys(), values)}
        print(new_row)
        data.append(new_row)

        write_csv(insert_table, data, headers, schema)
        return True
    
    except:
        return False

def _update():

    global schema

    update_table = commands[UPDATE]
    update_columns = commands[SET][0]
    update_values = commands[SET][1]

    data = result

    headers = list(data[0])

    update_data = _where()

    for row in update_data:
        for value, column in zip(update_values, update_columns):
            row[column] = value
    
    write_csv(update_table, data, headers, schema)

    return True

def _delete():

    global result

    try:
        data = result
        delete_data = _where()

        headers = list(data[0])

        for row in delete_data:
            data.remove(row)

        write_csv(commands[FROM], data, headers, schema)
        return True
    
    except:
        return False

def data_import():
    answer = None
    while not (answer == "mysql" or answer == "postgres" or answer == "csv" or answer == "ml" or answer == "ps"):
        print("Selecione csv ou um servidor (csv | mysql | postgres): ")
        answer = input(">> ")

        answer = answer.lower()

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
        print("Importar, consultar ou sair? (I | C | S)")
        answer = input(">> ")

    answer = answer.lower()

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