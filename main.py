import mysql.connector
import csv
from mysql.connector import errorcode

# Obtain connection string information from the portal

config = {
  'host':'<DATABASE_HOST>.mysql.database.azure.com',
  'user':'<USERNAME>',
  'password':'<PASSWORD>',
  'database':'DATABASE_NAME',
  'client_flags': [mysql.connector.ClientFlag.SSL],
  'ssl_ca': 'DigiCertGlobalRootCA.crt.pem'
}

# Construct connection string

try:
    conn = mysql.connector.connect(**config)
    print("Connection established")
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with the user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    else:
        print(err)
else:
    cursor = conn.cursor()

    '''# Drop previous table of same name if one exists
    cursor.execute("DROP TABLE IF EXISTS dados;")
    print("Finished dropping table (if existed).")'''

    # Create table named "dados"
    def create_table(cursor):
        cursor.execute('''CREATE TABLE IF NOT EXISTS vendas (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            year INT, 
                            make VARCHAR(255), 
                            model VARCHAR(255),
                            trim VARCHAR(255), 
                            body VARCHAR(255), 
                            transmission VARCHAR(50), 
                            vin VARCHAR(50), 
                            state VARCHAR(50),
                            vehicle_condition VARCHAR(50), 
                            odometer INT NULL, 
                            color VARCHAR(50), 
                            interior VARCHAR(50), 
                            seller VARCHAR(255), 
                            mmr DECIMAL(10,2),
                            sellingprice DECIMAL(10,2), 
                            saledate TEXT

                )  
        ''')

    def insert_data_batch(cursor, rows):
        query = '''INSERT INTO vendas (year,make,model,trim,body,transmission,vin,state,vehicle_condition,odometer,color,interior,seller,mmr,sellingprice,saledate) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )'''
        cursor.executemany(query, rows)


    
    def import_csv_to_database(csv_file, batch_size=1000):
        cursor = conn.cursor()
        create_table(cursor)

        with open(csv_file, 'r', encoding='utf-8') as arquivo:
            csv_reader = csv.DictReader(arquivo)
            rows = []
            for row in csv_reader:
                # Converter dados e lidar com valores nulos
                for chave, valor in row.items():
                    if valor == '':
                        row[chave] = None
                    elif chave in ('odometer', 'year'):  # Converter para inteiros
                        row[chave] = int(valor)
                    elif chave in ('mmr', 'sellingprice'):  # Converter para decimais
                        row[chave] = float(valor)
                rows.append(tuple(row.values()))

                if len(rows) >= batch_size:
                    insert_data_batch(cursor, rows)
                    conn.commit()
                    rows = []
                    print("Inseridas", batch_size, "linhas")

            if rows:  # Inserir as linhas restantes
                insert_data_batch(cursor, rows)
                conn.commit()
                print("Inseridas", len(rows), "linhas")

        cursor.close()
        print("Concluído.")

csv_file = 'car_prices.csv'
import_csv_to_database(csv_file)