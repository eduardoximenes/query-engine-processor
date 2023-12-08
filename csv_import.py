import csv
import os

def csv_import():
    #Define the path of csv to import
    print('Digite o caminho do csv para importar:')
    input_path = input('>> ')

    #Verify if the csv exists
    if not os.path.isfile(input_path):
        print("Csv não encontrado!")
        return

    #Define the path to import the csv content
    print('Digite o caminho o qual será importado:')
    output_path = input('>> ')

    #Verify if the file already exists, if exists allow to override or abort
    if os.path.isfile(output_path):
        confirm = input("O arquivo já existe, gostaria de sobreescrever? (s/n): ")
        if confirm.lower() != 's':
            print("Cancelado!")
            return

    #Creates the csv file
    open(output_path, 'w').close()

    #Write the content in csv
    try:
        with open(input_path, 'r') as input_file, open(output_path, 'w', newline='') as output_file:
            csv_reader = csv.reader(input_file)
            csv_writer = csv.writer(output_file)

            for line in csv_reader:
                csv_writer.writerow(line)

        print("Importação encerrada!")
    except Exception as e:
        print("Error:", str(e))