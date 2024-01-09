import os
import sqlite3
import pandas as pd

def database_setup(db_name="../../database/power_data.db"):
    try:
        with sqlite3.connect(db_name) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS Curtailment (
                    ID TEXT PRIMARY KEY,
                    Start TIMESTAMP,
                    Ende TIMESTAMP,
                    Dauer INTEGER,
                    Gebiet TEXT,
                    Ort_Engpass TEXT,
                    Stufe INTEGER,   
                    Anlagenschlüssel TEXT,
                    Anforderer TEXT,
                    Netzbetreiber TEXT,
                    Anlagen_ID TEXT,
                    Abrechnungs_ID TEXT,
                    nominal_power INTEGER,
                    curtailment_power REAL
                )
            ''')
            conn.commit()
    except sqlite3.Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"Exception in _query: {e}")

def insert_with_temp_table(df, db_name="../../database/power_data.db", table_name="Curtailment", primary_key_col="ID"):
    try:
        with sqlite3.connect(db_name) as conn:
            # Create a temporary table
            df.to_sql('temp_table', conn, if_exists='replace', index=False)

            # Prepare and execute the SQL query for inserting data
            columns = ', '.join(df.columns)
            query = f'''
            INSERT INTO {table_name} ({columns})
            SELECT {columns} FROM temp_table 
            WHERE {primary_key_col} NOT IN (SELECT {primary_key_col} FROM {table_name})
            '''
            conn.execute(query)

            # Drop the temporary table
            conn.execute('DROP TABLE temp_table')
            conn.commit()
            print("insert done!")
    except sqlite3.Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"Exception in _query: {e}")



if __name__=="__main__":
    df_ready = pd.read_csv("../../data/df_ready.csv")
    database_setup()
    insert_with_temp_table(df_ready)
