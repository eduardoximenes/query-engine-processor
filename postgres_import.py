import psycopg2
import processor
import os

host_glob = os.environ.get('HOST_POSTGRES')
user_glob = os.environ.get('USER_POSTGRES')
password_glob = os.environ.get('PASSWORD_POSTGRES')
port_glob = os.environ.get('PORT_POSTGRES')
database_glob = None 

def postgresconect():

    conn_params = {
        'database': database_glob,
        'user': user_glob,
        'host' : host_glob,
        'password': password_glob,
        'port': port_glob,
    }

    try:
        db_connection = psycopg2.connect(**conn_params)
    except:
        print("erro : Schema não encontrado")
        return 0

    print('Conectado ao servidor!')
    return db_connection


def postgres_check_table(table: str, cursor):
    try:
        query = ('select * from {}').format(table)
        cursor.execute(query)
        return 1
    except:
        return 0

def show_tables(cursor):
    print("Tabelas em {}:".format(database_glob))
    cursor.execute("SELECT * FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema';")
    for row in cursor:
        print('* '+row[1])

def show_database():
    db_connection = psycopg2.connect(user=user_glob,password=password_glob,
                                     host=host_glob,port=port_glob)
    cursor = db_connection.cursor()
    databases = "SELECT datname FROM pg_database WHERE datistemplate = false;"
    cursor.execute(databases)
    print("Schemas no servidor POSTGRES :")
    for databases in cursor:
        print("* "+databases[0].strip("'"))

def postgresimport():
    global database_glob

    show_database()
    conn = None
    while not conn:
        print("Selecione schema: ")
        database_glob = input('>> ')
        conn = postgresconect()

    cursor = conn.cursor()

    show_tables(cursor)
    print('Digite a tabela para importar : ')
    table = input('>> ')

    if postgres_check_table(table, cursor):

        if not processor.check_existing_schema(schema=database_glob):

            create_folder = None
            while not (create_folder == 's' or create_folder == 'n'):
                print('Schema näo encontrado localmente, gostaria de criar? (s| n)')
                create_folder = input('>> ')
                create_folder = create_folder.lower()


            if create_folder == 'y':
                processor.create_schema(schema=database_glob)
            else:
                return 0

        if processor.check_existing_table(table, schema=database_glob):
            overwrite = None
            while not (overwrite == 'y' or overwrite == 'n'):
                print('Tabela existente, sobreescrever ? (y/n)')
                overwrite = input('>> ')            
                overwrite = overwrite.lower()

            if overwrite == 's':
                headers = [desc[0] for desc in cursor.description]
                cursorDict = [dict(zip(headers, row)) for row in cursor.fetchall()]
                processor.write_csv(table, cursorDict, headers, schema=database_glob)
            elif overwrite == 'n':
                return 0
        else:
            # create a new file with the name of table
            headers = [desc[0] for desc in cursor.description]
            cursorDict = [dict(zip(headers, row)) for row in cursor.fetchall()]
            processor.write_csv(table, cursorDict, headers, schema=database_glob)

    else:
        print("erro : Tabela não existe no servidor")
        return 0

    for row in cursor:
        print(row)

    cursor.close()
    conn.close()