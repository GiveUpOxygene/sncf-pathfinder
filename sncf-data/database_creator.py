import psycopg2
from dotenv import load_dotenv
import os
import csv
import json
import math

script_dir = os.path.dirname(__file__)

# Increase CSV field size limit to handle large geo shape fields
csv.field_size_limit(10000000)  # Set to 10MB

troncons_csv_path = os.path.join(script_dir, "csv", "lignes-par-type.csv")
gares_csv_path = os.path.join(script_dir, "csv", "liste-des-gares.csv")
tarifs_csv_path = os.path.join(script_dir, "csv", "tarifs-tgv-inoui-ouigo.csv")
vitesse_csv_path = os.path.join(script_dir, "csv", "vitesse-maximale-nominale-sur-ligne.csv")

def db_connect():
    load_dotenv(os.path.join(script_dir, '../.env'))

    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")

    try:
        conn = psycopg2.connect(
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
            database="sncf"
        )
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to database: {e}")
        return None

def create_database():
    print("Starting create_database...")
    conn = db_connect()
    if not conn:
        print("Failed to connect to database for create_database.")
        return

    try:
        conn.autocommit = True
        cur = conn.cursor()

        cur.execute("SELECT 1 FROM pg_database WHERE datname = 'sncf'")
        exists = cur.fetchone()

        if not exists:
            cur.execute("CREATE DATABASE sncf")
            print("Database 'sncf' created successfully.")
        else:
            print("Database 'sncf' already exists.")

        cur.close()
        conn.close()
        print("Finished create_database.")

    except psycopg2.Error as e:
        print(f"Error connecting to or creating database: {e}")

def create_gares_table():
    print("Starting create_gares_table...")
    conn = db_connect()
    if not conn:
        print("Failed to connect to database for create_gares_table.")
        return

    try:
        conn.autocommit = True
        cur = conn.cursor()

        cur.execute("DROP TABLE IF EXISTS gares CASCADE;")
        print("Dropped table 'gares' if it existed.")

        with open(gares_csv_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.reader(f, delimiter=';')
            headers = next(reader)
            data_rows = list(reader)
        
        headers = [h.lower().strip() for h in headers]
        
        headers = [h.replace(' ', '_') for h in headers]
        
        columns_to_drop = ['x_l93', 'y_l93', 'x_wgs84', 'y_wgs84']
        
        drop_indices = [i for i, h in enumerate(headers) if h in columns_to_drop]
        
        if drop_indices:
            data_rows = [[val for i, val in enumerate(row) if i not in drop_indices] 
                        for row in data_rows]
        
        headers = [h for h in headers if h not in columns_to_drop]
        
        # Remove FRET column
        if 'fret' in headers:
            fret_index = headers.index('fret')
            headers.pop(fret_index)
            data_rows = [row[:fret_index] + row[fret_index+1:] for row in data_rows]
        
        if 'voyageurs' in headers:
            voyageurs_index = headers.index('voyageurs')
            data_rows = [row for row in data_rows if row[voyageurs_index].upper() == 'O']
            headers.pop(voyageurs_index)
            data_rows = [row[:voyageurs_index] + row[voyageurs_index+1:] for row in data_rows]
                
        column_defs_list = []
        for header in headers:
            if header == 'code_uic':
                column_defs_list.append(f'"{header}" VARCHAR(10) UNIQUE')
            else:
                column_defs_list.append(f'"{header}" TEXT')
        columns_def = ", ".join(column_defs_list)
        
        conn = db_connect()
        if not conn:
            return
        conn.autocommit = True
        cur = conn.cursor()

        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS gares (
            id SERIAL PRIMARY KEY,
            {columns_def}
        )
        """
        
        cur.execute(create_table_query)
        print("Table 'gares' created successfully.")
        print(f"Columns: {headers}")

        placeholders = ", ".join(["%s"] * len(headers))
        column_names = ", ".join([f'"{header}"' for header in headers])
        insert_query = f"""
        INSERT INTO gares ({column_names})
        VALUES ({placeholders})
        ON CONFLICT (code_uic) DO NOTHING;
        """
        
        inserted_count = 0
        for row in data_rows:
            try:
                cur.execute(insert_query, row)
                inserted_count += 1
            except psycopg2.Error as e:
                print(f"Error inserting row: {e}")
                continue
        
        print(f"Inserted {inserted_count} rows into 'gares' table.")

        cur.close()
        conn.close()
        print("Finished create_gares_table.")

    except FileNotFoundError:
        print(f"Error: CSV file not found at {gares_csv_path}")
    except psycopg2.Error as e:
        print(f"Error creating table: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

def create_lignes_table():
    print("Starting create_lignes_table...")
    conn = db_connect()
    if not conn:
        print("Failed to connect to database for create_lignes_table.")
        return

    try:
        with open(tarifs_csv_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.reader(f, delimiter=';')
            headers = next(reader)
            data_rows = list(reader)

        # Find the indices for the relevant columns
        try:
            gare_origine_uic_index = headers.index('Gare origine - code UIC')
            gare_destination_uic_index = headers.index('Gare destination - code UIC')
        except ValueError as e:
            print(f"Error: Missing expected column in CSV: {e}")
            return

        unique_lignes = set()
        for row in data_rows:
            if len(row) > max(gare_origine_uic_index, gare_destination_uic_index):
                gare1_uic = row[gare_origine_uic_index]
                gare2_uic = row[gare_destination_uic_index]
                if gare1_uic and gare2_uic:
                    # Store as a sorted tuple to ensure uniqueness regardless of order (A-B is same as B-A)
                    unique_lignes.add(tuple(sorted((gare1_uic, gare2_uic))))

        conn.autocommit = True
        cur = conn.cursor()

        create_table_query = """
        CREATE TABLE IF NOT EXISTS lignes (
            id SERIAL PRIMARY KEY,
            gare_origine_code_uic VARCHAR(10) NOT NULL,
            gare_destination_code_uic VARCHAR(10) NOT NULL
        )
        """
        cur.execute(create_table_query)
        print("Table 'lignes' created successfully.")

        insert_query = """
        INSERT INTO lignes (gare_origine_code_uic, gare_destination_code_uic)
        VALUES (%s, %s)
        ON CONFLICT DO NOTHING;
        """
        
        inserted_count = 0
        for gare1_uic, gare2_uic in unique_lignes:
            try:
                cur.execute(insert_query, (gare1_uic, gare2_uic))
                inserted_count += 1
            except psycopg2.Error as e:
                print(f"Error inserting ligne ({gare1_uic}, {gare2_uic}): {e}")
                continue
        
        print(f"Inserted {inserted_count} unique lignes into 'lignes' table.")

        cur.close()
        conn.close()
        print("Finished create_lignes_table.")

    except FileNotFoundError:
        print(f"Error: CSV file not found at {tarifs_csv_path}")
    except psycopg2.Error as e:
        print(f"Error creating or inserting into 'lignes' table: {e}")
    except Exception as e:
        print(f"Unexpected error in create_lignes_table: {e}")

def create_troncons_table():
    print("Starting create_troncons_table...")
    conn = db_connect()
    if not conn:
        print("Failed to connect to database for create_troncons_table.")
        return

    try:
        conn.autocommit = True
        cur = conn.cursor()

        cur.execute("DROP TABLE IF EXISTS troncons CASCADE;")
        print("Dropped table 'troncons' if it existed.")

        with open(troncons_csv_path, 'r', encoding='utf-8-sig') as f:  # utf-8-sig removes BOM
            reader = csv.reader(f, delimiter=';')
            headers_troncons_original = next(reader)
            data_troncons = list(reader)
        
        with open(vitesse_csv_path, 'r', encoding='utf-8-sig') as f:  # utf-8-sig removes BOM
            reader = csv.reader(f, delimiter=';')
            headers_speed_original = next(reader)
            data_speed = list(reader)
        
        headers_troncons = [h.lower().strip().replace(' ', '_') for h in headers_troncons_original]
        headers_speed = [h.lower().strip().replace(' ', '_') for h in headers_speed_original]
        
        columns_to_drop = ['x_d_l93', 'y_d_l93', 'x_f_l93', 'y_f_l93', 
                          'x_d_wgs84', 'y_d_wgs84', 'x_f_wgs84', 'y_f_wgs84',
                          'x_l93', 'y_l93', 'x_wgs84', 'y_wgs84']
        
        drop_indices_troncons = [i for i, h in enumerate(headers_troncons) if h in columns_to_drop]
        drop_indices_speed = [i for i, h in enumerate(headers_speed) if h in columns_to_drop]
        
        if drop_indices_troncons:
            data_troncons = [[val for i, val in enumerate(row) if i not in drop_indices_troncons] 
                         for row in data_troncons]
        if drop_indices_speed:
            data_speed = [[val for i, val in enumerate(row) if i not in drop_indices_speed] 
                         for row in data_speed]
        
        headers_troncons = [h for h in headers_troncons if h not in columns_to_drop]
        headers_speed = [h for h in headers_speed if h not in columns_to_drop]
        
        all_headers = headers_troncons.copy()
        for header in headers_speed:
            if header not in all_headers:
                all_headers.append(header)
        
        all_headers.append("temps_trajet")
        
        columns = ", ".join([f'"{header}" TEXT' for header in all_headers])
        
        conn = db_connect()
        if not conn:
            return
        conn.autocommit = True
        cur = conn.cursor()

        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS troncons (
            id SERIAL PRIMARY KEY,
            {columns}
        )
        """
        
        cur.execute(create_table_query)
        print("Table 'troncons' created successfully.")
        print(f"Columns: {all_headers}")

        merged_data = {}
        
        # Merge data from both CSVs
        for row in data_troncons:
            if len(row) >= len(headers_troncons):
                key = f"{row[headers_troncons.index('code_ligne')]}_{row[headers_troncons.index('rg_troncon')]}_{row[headers_troncons.index('pkd')]}_{row[headers_troncons.index('pkf')]}"
                merged_data[key] = dict(zip(headers_troncons, row))
        
        for row in data_speed:
            if len(row) >= len(headers_speed):
                code_ligne_value = row[headers_speed.index('code_ligne')]
                
                for key, data_dict in merged_data.items():
                    if data_dict.get('code_ligne') == code_ligne_value:
                        for i, header in enumerate(headers_speed):
                            if header not in data_dict or not data_dict[header]:
                                data_dict[header] = row[i]
        
        placeholders = ", ".join(["%s"] * len(all_headers))
        column_names = ", ".join([f'"{header}"' for header in all_headers])
        insert_query = f"INSERT INTO troncons ({column_names}) VALUES ({placeholders})"
        
        inserted_count = 0
        for key, data_dict in merged_data.items():
            try:
                row_data = [data_dict.get(header, None) for header in all_headers]
                cur.execute(insert_query, row_data)
                inserted_count += 1
            except psycopg2.Error as e:
                print(f"Error inserting row: {e}")
                continue
        
        print(f"Inserted {inserted_count} rows into 'troncons' table.")

        cur.close()
        conn.close()
        print("Finished create_troncons_table.")

    except FileNotFoundError:
        print(f"Error: CSV file not found - {e}")
    except psycopg2.Error as e:
        print(f"Error creating table: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")


if __name__ == "__main__":
    create_database()
    create_gares_table()
    create_lignes_table()
    create_troncons_table()