import csv
import sqlite3

# Função para criar a tabela no banco de dados
def create_table(cursor):
    cursor.execute('''CREATE TABLE IF NOT EXISTS dados (
                        year TEXT,
                        make TEXT,
                        model TEXT,
                        trim TEXT,
                        body TEXT,
                        transmission TEXT,
                        vin TEXT,
                        state TEXT,
                        vehicle_condition TEXT,
                        odometer TEXT,
                        color TEXT,
                        interior TEXT,
                        seller TEXT,
                        mmr TEXT,
                        sellingprice TEXT,
                        saledate TEXT

                    )''')
    print('Tabela Criada')

# Função para inserir dados do CSV no banco de dados
def insert_data(cursor, row):
    cursor.execute('''INSERT INTO dados (year,make,model,trim,body,transmission,vin,state,vehicle_condition,odometer,color,interior,seller,mmr,sellingprice,saledate) 
                      VALUES (?, ?, ? , ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', row)

# Função para ler os dados do CSV e inseri-los no banco de dados
def import_csv_to_database(csv_file, database_file):
    conn = sqlite3.connect(database_file)
    cursor = conn.cursor()

    create_table(cursor)
    cont = 0
    with open(csv_file, 'r') as file:
        csv_reader = csv.reader(file)
        next(csv_reader)  # Pula a linha de cabeçalho se existir
        for row in csv_reader:
            cont += 1
            print(cont)            
            insert_data(cursor, row)
        print('Inserção de dados concluida')

    conn.commit()
    conn.close()

# Exemplo de uso
csv_file = 'car_prices.csv'
database_file = 'dados.db'

import_csv_to_database(csv_file, database_file)
