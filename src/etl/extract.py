import pandas as pd
from datetime import datetime, timedelta

def download_curtailment_data(start:str, end:str)->pd.DataFrame:
    url = f'https://redispatch-run.azurewebsites.net/api/export/csv?&networkoperator=ava&type=finished&orderDirection=desc&orderBy=start&chunkNr=1&param1=start&op1=gt&startOp=gt&val1={start}&param2=end&op2=lt&endOp=lt&val2={end}'
    try:
        df = pd.read_csv(url, sep=';')
        print(f"downloaded {len(df)} rows")
        return df
    except Exception as e:
        print(f"An error occurred: {e}")
        return pd.DataFrame()

def download_curtailment_historical_data(end: str = 'now'):
    final_end_date = datetime.now() if end == 'now' else datetime.strptime(end, '%Y-%m-%d')
    start_date = datetime(2021,1,1)
    end_date = datetime(2021,2,1)
    all_data = pd.DataFrame()
    while start_date < final_end_date:
        str_start_date = start_date.strftime('%Y-%m-%d')
        str_end_date = min(end_date,final_end_date).strftime('%Y-%m-%d')
        chunk_data = download_curtailment_data(str_start_date, str_end_date)
        all_data = pd.concat([all_data, chunk_data], ignore_index=True)
        start_date = end_date + timedelta(days=1)
        end_date += timedelta(days=30)
        if end_date > final_end_date:
            end_date = final_end_date
    all_data.drop_duplicates(subset=['ID'], keep='first', inplace=True)
    all_data.to_csv("../../data/curtailment_historical.csv",index=False)
    return all_data

def download_eeg_data(year:str)->pd.DataFrame:
    if year =='2021':
        url = 'https://www.netztransparenz.de/xspproxy/api/staticfiles/ntp-relaunch/dokumente/erneuerbare%20energien%20und%20umlagen/eeg/eeg-abrechnungen/eeg-jahresabrechnungen/eeg-anlagenstammdaten/tennettsogmbheeg-zahlungenstammdaten2021.zip'
    elif year =='2022':
        url = "https://www.netztransparenz.de/xspproxy/api/staticfiles/ntp-relaunch/dokumente/zuordnung_unklar/eeg-anlagenstammdaten/2022/tennettsogmbheeg-zahlungenstammdaten2022.zip"
    else:
        print("Year not supported")
        return None
    try:
        df = pd.read_csv(url, sep=';', encoding='ISO-8859-1')
        print(f"downloaded {len(df)} rows")
    except Exception as e:
        print(f"An error occurred: {e}")
    return df

def download_eeg_historical_data()->pd.DataFrame:
    df_2021 = download_eeg_data('2021')
    df_2022 = download_eeg_data('2022')
    all_data = pd.concat([df_2021, df_2022], ignore_index=True)
    all_data.to_csv("../../data/eeg_historical.csv",index=False)
    return 

if __name__ == '__main__':
    download_curtailment_historical_data('2022-12-31')
    download_eeg_historical_data()
    