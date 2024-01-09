import streamlit as st
import pandas as pd
import sqlite3
import os
from plot_utils import plot_data, plot_single_plant_weekday, plot_single_plant_hour
from utils import recalculate_curtailment_power

def create_db_connection(db_name="power_data.db"):
    db_path = get_database_path(db_name)
    return sqlite3.connect(db_path)

def match_pk_fk(val: int) -> str:
    """ Match the value returned by the pk column in SQLite pragma_table_info() """
    if not show_types or val == 0:
        return ''
    if val == 1:
        return 'PK'
    if val == 2:
        return 'FK'
    raise TypeError(f'Expected type None or int, not {type(val)}, {val =}')

def get_database_path(db_name="power_data.db"):
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    db_path = os.path.join(base_dir, "database", db_name)
    return db_path

def get_data_from_db(query, db_name="power_data.db"):
    db_path = get_database_path(db_name)
    with sqlite3.connect(db_path) as conn:
        return pd.read_sql_query(query, conn)

def prepare_data_for_anlagenschlüssel(anlagenschlüssel, freq="H"):
    db_path = get_database_path("power_data.db")
    with sqlite3.connect(db_path) as conn:
        query = f"""
        SELECT Start, Ende, nominal_power, curtailment_power 
        FROM Curtailment 
        WHERE Anlagenschlüssel = '{anlagenschlüssel}'
        """
        df = pd.read_sql_query(query, conn)

    df_timeline = recalculate_curtailment_power(df, freq)
    return df_timeline


st.title('Curtailment per Anlagenschlüssel')

if 'conn' not in st.session_state:
    # Initialize 'conn' in session state
    st.session_state.conn = create_db_connection()
    
df_anlagenschlüssel = get_data_from_db("SELECT DISTINCT Anlagenschlüssel FROM Curtailment")

# Dropdown to select Anlagenschlüssel and frequency
selected_anlagenschlüssel = st.selectbox('Select Anlagenschlüssel', df_anlagenschlüssel['Anlagenschlüssel'])
selected_frequency = st.selectbox('Select Time Frequency', ['H', 'T', 'D'])

# Prepare and plot data for the selected Anlagenschlüssel
if selected_anlagenschlüssel:
    df_timeline = prepare_data_for_anlagenschlüssel(selected_anlagenschlüssel, freq=selected_frequency)
    fig = plot_data(df_timeline, selected_anlagenschlüssel)
    st.plotly_chart(fig)
    fig_weekday = plot_single_plant_weekday(df_timeline, selected_anlagenschlüssel)
    st.plotly_chart(fig_weekday)
    fig_hour = plot_single_plant_hour(df_timeline, selected_anlagenschlüssel)
    st.plotly_chart(fig_hour)

# sidebar/ schema
with st.sidebar:
    show_types = st.checkbox('Show types', value=True, help='Show data types for each column ?')
    schema = ''
    with st.session_state.conn as cursor:

        for x in cursor.execute("SELECT name FROM sqlite_master WHERE type='table'"):
            table = x[0]
            schema += f'\n\n * {table}:'

            for row in cursor.execute(f"PRAGMA table_info('{table}')"):
                col_name = row[1]
                col_type = row[2].upper() if show_types is True else ''
                schema += f'\n     - {col_name:<15} {col_type} \t {match_pk_fk(row[5])}'

    st.text('DataBase Schema:')
    st.text(schema)

    st.markdown('---')
    st.markdown('Author: Boyu Wang')
    st.markdown('[Source](https://bit.ly/3zZwpim)')