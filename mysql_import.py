import mysql.connector
import processor
import os

host_glob = os.environ.get('HOST_MYSQL')
user_glob = os.environ.get('USER_MYSQL')
password_glob = os.environ.get('PASSWORD_MYSQL')
port_glob = os.environ.get('PORT_MYSQL')
database_glob = None #Leave this field unchanged 

def mysql_check_table(table:str,cursor):
    try:
        query = ('select * from {}').format(table)
        cursor.execute(query)
        return True
    except:
        return False

def show_tables(cursor):
    
    print("Tabelas em {}:".format(database_glob))
    tables = ("show tables;")
    cursor.execute(tables)
    for (row) in cursor:
        for key in row:
            print('* '+row[key].strip("'"))

def mysqlconnect():

    conn_params = {
        'host':host_glob,
        'user':user_glob,
        'password':password_glob,
        'database': database_glob,
        'port': port_glob
    }
    try:
        # Double * unpack the dictionary
        db_connection=mysql.connector.connect(**conn_params)
    except:
        print("erro : Esquema não encontrado.")
        return False
    
    print('Conectado ao servidor!')

    return db_connection

def show_database():
    conn = mysql.connector.connect (user=user_glob, password=password_glob,
                                    host=host_glob,buffered=True)
    cursor = conn.cursor()
    databases = ("show databases;")
    cursor.execute(databases)
    print("Esquemas no MySQL server:")
    for (databases) in cursor:
        print ('* '+databases[0])

def mysqlimport():
    global database_glob

    show_database()
    conn = None
    while not conn :
        print("Selecione um esquema: ")
        database_glob = input('>> ')
        conn = mysqlconnect() 
   
    cursor = conn.cursor(dictionary=True,buffered=True)
    
    show_tables(cursor)
    print('Escolha a tabela para importar: ')
    table_imp = input('>> ')
    
    while True:
        if mysql_check_table(table_imp, cursor):
            break
        else :
            print("erro : Tabela não existe no servidor.")
            print('Escolha a tabela para importar: ')
            table_imp = input('>> ')     

    
    if not processor.check_existing_schema(schema=database_glob): 
        create_folder = None
        while not(create_folder == 's' or create_folder == 'n'): 
            print('Esquema não encontrado localmente, gostaria de criá-lo? (s/n)')
            create_folder = input('>> ')
        
        if create_folder=='s':
            processor.create_schema(schema=database_glob)
        else :
            return True
    
    if processor.check_existing_table(schema=database_glob, table=table_imp):
        overwrite = None
        while not(overwrite == 's' or overwrite =='n'):
            print('Tabela já existente, gostaria de sobreescrever? (s/n)')
            overwrite = input('>> ')
        if overwrite == 's':
            headers = cursor.column_names
            processor.write_csv(table_imp, cursor, headers, schema=database_glob)
        elif overwrite == 'n':
            print("Importação encerrada.")
            return True
    else : 
        headers = cursor.column_names
        processor.write_csv(table_imp, cursor, headers, schema=database_glob)
        print("Importação finalizada.")
            
    cursor.close()
    conn.close()

    return True
