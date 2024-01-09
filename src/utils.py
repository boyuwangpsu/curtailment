import pandas as pd

def generate_complete_timeline(freq="H"):
    # Generate a DataFrame with the specified intervals
    all_times = pd.date_range(start='2021-01-01 00:00:00', end='2021-12-31 23:59:59', freq=freq).to_series()
    df_timeline = pd.DataFrame({'TimeSlot': all_times})

    return df_timeline

def recalculate_curtailment_power(df,freq="H"):
    df_timeline = generate_complete_timeline(freq)
    # Initialize columns for nominal and curtailed power
    df_timeline['curtailment_power'] = 0
    df_timeline['curtailment_energy'] = 0
    df['Start'] = pd.to_datetime(df['Start'])
    df['Ende'] = pd.to_datetime(df['Ende'])
    if freq == "H":
        delta = pd.Timedelta(hours=1)
        unit_factor = 1
    elif freq == "T":
        delta = pd.Timedelta(minutes=1)
        unit_factor = 60
    elif freq == "D":
        delta = pd.Timedelta(days=1)
        unit_factor = 1/24
    else:
        print("freq should be H or D or T")
        return
    # Iterate over each row in the original dataframe
    for _, row in df.iterrows():
        # Find overlapping time slots
        overlapping_times = df_timeline[(df_timeline['TimeSlot'] >= row['Start']) & (df_timeline['TimeSlot'] <= row['Ende'])]

        # Calculate curtailment for each overlapping time slot
        for time_slot in overlapping_times['TimeSlot']:
            duration = min(time_slot + delta, row['Ende']) - max(time_slot, row['Start'])
            duration_hours = duration.total_seconds() / 3600
            curtailment_energy = row['curtailment_power'] * duration_hours
            # Update timeline DataFrame
            df_timeline.loc[df_timeline['TimeSlot'] == time_slot, 'curtailment_energy'] += curtailment_energy
    df_timeline['curtailment_power'] = df_timeline['curtailment_energy']*unit_factor
    # Calculate cumulative curtailed energy
    df_timeline['cumulative_energy'] = df_timeline['curtailment_energy'].cumsum()

    return df_timeline

def prepare_data_for_anlagenschlüssel_df(df, anlagenschlüssel, freq="H"):
    df_single_anlagenschlüssel = df[df['Anlagenschlüssel'] == anlagenschlüssel]
    # Generate complete timeline and calculate power
    df_timeline = recalculate_curtailment_power(df_single_anlagenschlüssel, freq)
    return df_timeline