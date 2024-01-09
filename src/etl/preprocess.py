import pandas as pd
import numpy as np

def get_mode_or_default(series, default_value):
    return series.mode()[0] if not series.mode().empty else default_value

def etl_curtailment_data(df:pd.DataFrame)->pd.DataFrame:
    df = df.drop(columns=['Einsatz-ID', 'Ursache', 'Entschädigungspflicht'])
    df = df.dropna(subset=['Anlagenschlüssel', 'Start', 'Ende'])

    mode_per_group = df.groupby('Anlagenschlüssel')['Stufe (%)'].transform(lambda x: get_mode_or_default(x, 30))
    df['Stufe (%)'] = df['Stufe (%)'].fillna(mode_per_group)

    mode_value = get_mode_or_default(df['Dauer (Min)'], 8)
    df['Dauer (Min)'] = df['Dauer (Min)'].fillna(mode_value)

    df.fillna('Unknown', inplace=True)

    df['Start'] = pd.to_datetime(df['Start'], errors='coerce')
    df['Ende'] = pd.to_datetime(df['Ende'], errors='coerce')
    df['Stufe (%)'] = df['Stufe (%)'].astype(np.int8)
    df['ID'] = df['ID'].astype(str)
    
    df = df.rename(columns={
        'Dauer (Min)': 'Dauer', 
        'Stufe (%)': 'Stufe', 
        'Ort Engpass': 'Ort_Engpass', 
        'Anlagen-ID': 'Anlagen_ID', 
        'Abrechnungs-ID': 'Abrechnungs_ID'
    }) 
    return df

def etl_EEG_data(df):
    df = df.drop(columns=['Straße_Flurstück', 'Ort_Gemarkung', 'Einspeisespannungsebene', 'Leistungsmessung', 'Außerbetriebnahme', 'Netzzugang', 'Netzabgang'])
    df = df.dropna(subset=['Installierte_Leistung'])
    df = df.drop_duplicates(subset='EEG_Anlagenschlüssel')
    df.fillna('Unknown', inplace=True)

    df['Inbetriebnahme'] = pd.to_datetime(df['Inbetriebnahme'], errors='coerce')
    df['NB_BNR'] = df['NB_BNR'].astype(str)
    df['Gemeindeschlüssel'] = df['Gemeindeschlüssel'].astype(str)
    df['Installierte_Leistung'] = df['Installierte_Leistung'].str.replace(',', '').astype(int)
    df['EEG_Anlagenschlüssel'] = df['EEG_Anlagenschlüssel'].astype(str)

    df.rename(columns={
        'EEG_Anlagenschlüssel': 'Anlagenschlüssel', 
        'Installierte_Leistung': 'nominal_power'
    }, inplace=True)

    df['nominal_power'] = df['nominal_power'] / 1000  # Convert to kW
    return df

def merge_and_calculate(df_curtailment_etl:pd.DataFrame, df_eeg_etl:pd.DataFrame)->pd.DataFrame:
    df_ready = pd.merge(df_curtailment_etl, df_eeg_etl[['Anlagenschlüssel','nominal_power']], on='Anlagenschlüssel', how='inner')
    df_ready['curtailment_power'] = (100-df_ready['Stufe']) * df_ready['nominal_power']/100
    df_ready.to_csv("../../data/df_ready.csv",index=False)
    return df_ready


if __name__=='__main__':
    df_curtailment = pd.read_csv('../../data/curtailment_historical.csv')
    df_eeg = pd.read_csv('../../data/eeg_historical.csv')
    print("curtilment data:", df_curtailment.head(3))
    print("eeg data:" , df_curtailment.head(3))
    df_curtailment_etl = etl_curtailment_data(df_curtailment)
    df_eeg_etl = etl_EEG_data(df_eeg)
    df_ready = merge_and_calculate(df_curtailment_etl, df_eeg_etl)
    print("ready to insert:" , df_ready.head(3))
   

