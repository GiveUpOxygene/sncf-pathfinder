import psycopg2
from dotenv import load_dotenv
import os
import csv
import json
import math

# Increase CSV field size limit to handle large geo shape fields
csv.field_size_limit(10000000)  # Set to 10MB

def create_database():
    load_dotenv('../.env')

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
            database="postgres"
        )
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

    except psycopg2.Error as e:
        print(f"Error connecting to or creating database: {e}")

def create_gares_table():
    load_dotenv('../.env')

    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")

    csv_path = "./csv/liste-des-gares.csv"

    try:
        with open(csv_path, 'r', encoding='utf-8-sig') as f:
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
                
        conn = psycopg2.connect(
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
            database="sncf"
        )
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
        insert_query = f"INSERT INTO gares ({column_names}) VALUES ({placeholders})"
        
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

    except FileNotFoundError:
        print(f"Error: CSV file not found at {csv_path}")
    except psycopg2.Error as e:
        print(f"Error creating table: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

def create_lignes_table():
    load_dotenv('../.env')

    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")

    csv_lines_path = "./csv/lignes-par-type.csv"
    csv_speed_path = "./csv/vitesse-maximale-nominale-sur-ligne.csv"

    try:
        with open(csv_lines_path, 'r', encoding='utf-8-sig') as f:  # utf-8-sig removes BOM
            reader = csv.reader(f, delimiter=';')
            headers_lines_original = next(reader)
            data_lines = list(reader)
        
        with open(csv_speed_path, 'r', encoding='utf-8-sig') as f:  # utf-8-sig removes BOM
            reader = csv.reader(f, delimiter=';')
            headers_speed_original = next(reader)
            data_speed = list(reader)
        
        headers_lines = [h.lower().strip().replace(' ', '_') for h in headers_lines_original]
        headers_speed = [h.lower().strip().replace(' ', '_') for h in headers_speed_original]
        
        columns_to_drop = ['x_d_l93', 'y_d_l93', 'x_f_l93', 'y_f_l93', 
                          'x_d_wgs84', 'y_d_wgs84', 'x_f_wgs84', 'y_f_wgs84',
                          'x_l93', 'y_l93', 'x_wgs84', 'y_wgs84']
        
        drop_indices_lines = [i for i, h in enumerate(headers_lines) if h in columns_to_drop]
        drop_indices_speed = [i for i, h in enumerate(headers_speed) if h in columns_to_drop]
        
        if drop_indices_lines:
            data_lines = [[val for i, val in enumerate(row) if i not in drop_indices_lines] 
                         for row in data_lines]
        if drop_indices_speed:
            data_speed = [[val for i, val in enumerate(row) if i not in drop_indices_speed] 
                         for row in data_speed]
        
        headers_lines = [h for h in headers_lines if h not in columns_to_drop]
        headers_speed = [h for h in headers_speed if h not in columns_to_drop]
        
        all_headers = headers_lines.copy()
        for header in headers_speed:
            if header not in all_headers:
                all_headers.append(header)
        
        all_headers.append("temps_trajet")
        
        columns = ", ".join([f'"{header}" TEXT' for header in all_headers])
        
        conn = psycopg2.connect(
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
            database="sncf"
        )
        conn.autocommit = True
        cur = conn.cursor()

        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS lignes (
            id SERIAL PRIMARY KEY,
            {columns}
        )
        """
        
        cur.execute(create_table_query)
        print("Table 'lignes' created successfully.")
        print(f"Columns: {all_headers}")

        cur.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_lignes_code_ligne ON lignes ("code_ligne")')
        print("Unique index on 'code_ligne' created successfully.")

        merged_data = {}
        
        # Merge data from both CSVs
        for row in data_lines:
            if len(row) >= len(headers_lines):
                key = f"{row[headers_lines.index('code_ligne')]}_{row[headers_lines.index('rg_troncon')]}_{row[headers_lines.index('pkd')]}_{row[headers_lines.index('pkf')]}"
                merged_data[key] = dict(zip(headers_lines, row))
        
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
        insert_query = f"INSERT INTO lignes ({column_names}) VALUES ({placeholders})"
        
        inserted_count = 0
        for key, data_dict in merged_data.items():
            try:
                row_data = [data_dict.get(header, None) for header in all_headers]
                cur.execute(insert_query, row_data)
                inserted_count += 1
            except psycopg2.Error as e:
                print(f"Error inserting row: {e}")
                continue
        
        print(f"Inserted {inserted_count} rows into 'lignes' table.")

        cur.close()
        conn.close()

    except FileNotFoundError as e:
        print(f"Error: CSV file not found - {e}")
    except psycopg2.Error as e:
        print(f"Error creating table: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

def haversine_distance(lon1, lat1, lon2, lat2):
    """Calculate the great circle distance between two points on Earth (in kilometers)"""
    # Convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(math.radians, [lon1, lat1, lon2, lat2])
    
    # Haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    r = 6371  # Radius of Earth in kilometers
    
    return c * r

def calculate_line_distance(geo_shape):
    """Calculate total distance of a line from its GeoJSON coordinates"""
    try:
        geo_data = json.loads(geo_shape)
        coordinates = geo_data.get('coordinates', [])
        
        if len(coordinates) < 2:
            return 0
        
        total_distance = 0
        for i in range(len(coordinates) - 1):
            lon1, lat1 = coordinates[i]
            lon2, lat2 = coordinates[i + 1]
            total_distance += haversine_distance(lon1, lat1, lon2, lat2)
        
        return total_distance
    except (json.JSONDecodeError, KeyError, ValueError):
        return 0

def calculate_travel_times():
    load_dotenv('../.env')

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
        cur = conn.cursor()

        cur.execute('SELECT id, geo_shape, v_max FROM lignes WHERE geo_shape IS NOT NULL')
        rows = cur.fetchall()
        
        updated_count = 0
        for row in rows:
            row_id, geo_shape, v_max = row
            
            if not geo_shape or not v_max:
                continue
            
            try:
                distance_km = calculate_line_distance(geo_shape)
                
                if distance_km == 0:
                    continue
                
                speed_kmh = float(v_max)
                
                if speed_kmh == 0:
                    continue
                
                time_hours = distance_km / speed_kmh
                time_minutes = time_hours * 60
                
                cur.execute(
                    'UPDATE lignes SET temps_trajet = %s WHERE id = %s',
                    (str(time_minutes), row_id)
                )
                updated_count += 1
                
            except (ValueError, ZeroDivisionError) as e:
                print(f"Error processing row {row_id}: {e}")
                continue
        
        conn.commit()
        print(f"Updated {updated_count} rows with calculated travel times.")
        
        cur.close()
        conn.close()

    except psycopg2.Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

def add_foreign_key_constraint():
    load_dotenv('../.env')

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
        conn.autocommit = True
        cur = conn.cursor()

        # foreign key
        try:
            cur.execute('''
                ALTER TABLE gares 
                ADD CONSTRAINT fk_gares_code_ligne 
                FOREIGN KEY ("code_ligne") 
                REFERENCES lignes ("code_ligne")
            ''')
            print("Foreign key constraint on 'code_ligne' created successfully.")
        except psycopg2.Error as e:
            print(f"Foreign key constraint already exists or error: {e}")

        cur.close()
        conn.close()

    except psycopg2.Error as e:
        print(f"Database error: {e}")

if __name__ == "__main__":
    create_database()
    create_lignes_table()
    create_gares_table()
    add_foreign_key_constraint()
    calculate_travel_times()