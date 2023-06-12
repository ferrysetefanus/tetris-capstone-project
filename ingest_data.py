import os
import pandas as pd
import datetime
from sqlalchemy import create_engine

def extract_data(url: str):
   
    csv_name = 'crime_data_la.csv'

    os.system(f"curl --insecure -o {csv_name} {url}")

    df = pd.read_csv(csv_name)

    return df

def transform_data(df):

    transformed_df = df.copy()

    #mendrop kolom yang tidak perlukan karena berisikan informasi yang sudah tersedia di kolom lain dan berisikan kode khusus
    transformed_df.drop(['DR_NO', 'AREA', 'Rpt Dist No', 'Part 1-2', 'Crm Cd', 
                         'Mocodes', 'Premis Cd', 'Weapon Used Cd', 'Status', 'Crm Cd 1', 
                         'Crm Cd 2', 'Crm Cd 3', 'Crm Cd 4', 'Cross Street'], axis = 1, inplace=True)

    #merename kolom agar lebih mudah dibaca
    transformed_df.rename(columns={'Date Rptd': 'date reported', 
                                   'DATE OCC': 'date occurance', 
                                   'TIME OCC': 'time occurance',
                                   'AREA NAME' : 'area name',
                                   'Crm Cd Desc' : 'crime description',
                                   'Vict Age' : 'victim age',
                                   'Vict Sex' : 'victim sex',
                                   'Vict Descent' : 'victim descendant',
                                   'Premis Desc' : 'premis description',
                                   'Weapon Desc' : 'weapon description',
                                   'Status Desc' : 'case status',
                                   'LOCATION' : 'location',
                                   'LAT' : 'lat',
                                   'LON' : 'lon'},
                                    inplace=True)

    #mengubah tipe data kolom date reported dan date occurance menjadi datetime
    transformed_df['date reported'] = pd.to_datetime(transformed_df['date reported'], format="%m/%d/%Y %I:%M:%S %p")
    transformed_df['date occurance'] = pd.to_datetime(transformed_df['date occurance'], format="%m/%d/%Y %I:%M:%S %p")


    # melakukan transformasi terhadap kolom time occurance
    transformed_df['time occurance'] = transformed_df['time occurance'].astype(str) 
    transformed_df['time occurance'] = transformed_df['time occurance'].str[:-2].str.zfill(2) + ':' + transformed_df['time occurance'].str[-2:]
    transformed_df['time occurance'] = pd.to_datetime(transformed_df['time occurance'], format='%H:%M').dt.strftime('%H:%M')    

    #mengisi nilai kosong
    transformed_df['victim sex'].fillna('unknown', inplace=True)
    transformed_df['victim descendant'].fillna('unknown', inplace=True)
    transformed_df['weapon description'].fillna('UNKNOWN WEAPON/OTHER WEAPON', inplace=True)
    transformed_df['victim age'] = transformed_df['victim age'].abs()

    transformed_df['location'] = transformed_df['location'].replace('\s+', ' ', regex=True).str.strip()

    #mapping value
    sex_mapping = {'F' : 'female', 'M' : 'male', 'X' : 'unknown', 'H' : 'female', 'N' : 'male', 'nan' : 'unknown'}
    desc_mapping = {'A' : 'other asian', 'B' : 'black', 'C' : 'chinese',
                    'D' : 'cambodian', 'F' : 'filipino', 'G' : 'guamanian',
                    'H' : 'hispanic/latin/mexican', 'I' : 'american indian/alaskan native',
                    'J' : 'japanese', 'K' : 'korean', 'L' : 'laotian', 'O' : 'other', 'P' : 'pacific islander',
                    'S' : 'samoan', 'U' : 'hawaiian', 'V' : 'vietnamese', 'W' : 'white',
                    'X' : 'unknown', 'Z' : 'asian indian',
                    '-' : 'unknown',
                    'nan' : 'unknown'
                   }

    transformed_df['victim sex'] = transformed_df['victim sex'].map(sex_mapping)
    transformed_df['victim descendant'] = transformed_df['victim descendant'].map(desc_mapping)

    transformed_df['victim sex'].fillna('unknown', inplace=True)
    transformed_df['victim descendant'].fillna('unknown', inplace=True)

    return transformed_df

def load_data(df, table_name, connection_string):
    engine = create_engine(connection_string)
    database_name = "la_crime_db"

    # Mengecek apakah database sudah ada
    with engine.connect() as conn:
        existing_databases = conn.execute(
            "SELECT datname FROM pg_catalog.pg_database"
        )
        existing_databases = [db[0] for db in existing_databases]
        
        if database_name not in existing_databases:
            # Jika database belum ada, buat database baru
            conn.execute(f"COMMIT")  # Mengakhiri transaksi sebelum membuat database
            conn.execute(f"CREATE DATABASE {database_name}")
    
    # Membuat koneksi baru ke database yang sudah ada atau baru saja dibuat
    connection_string_with_db = f'{connection_string}/{database_name}'
    engine_with_db = create_engine(connection_string_with_db)
    with engine_with_db.connect() as conn:
        df.to_sql(table_name, conn, index=False, if_exists='replace')

    print(f"Data berhasil dimuat ke tabel '{table_name}' di PostgreSQL")

if __name__ == '__main__':
    csv_url = "https://data.lacity.org/api/views/2nrs-mtv8/rows.csv?accessType=DOWNLOAD"
    df = extract_data(csv_url)
    transformed_data = transform_data(df)

    # Menyimpan file CSV hasil transformasi
    transformed_data = transformed_data.sort_values('date occurance')
    transformed_csv_name = 'transformed_crime_data_la.csv'
    transformed_data.to_csv(transformed_csv_name, index=False)
    print(f"File CSV hasil transformasi disimpan sebagai '{transformed_csv_name}'")

    # Memuat data ke PostgreSQL
    connection_string = 'postgresql://postgres:lostnick@localhost:5432'
    table_name = 'crime_data'
    load_data(transformed_data, table_name, connection_string)

    # Menghapus file CSV sebelum transformasi
    os.remove('crime_data_la.csv')
    print("File CSV sebelum transformasi dihapus")
