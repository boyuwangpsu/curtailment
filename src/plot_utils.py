import pandas as pd
import plotly.graph_objects as go

def plot_data(df_timeline, selected_anlagenschlüssel):
    # Create a figure
    fig = go.Figure()

    # Add line plot for curtailment power
    fig.add_trace(go.Scatter(x=df_timeline['TimeSlot'], y=df_timeline['curtailment_power'], name='Power', mode='lines', yaxis='y'))

    # Add line plot for cumulative curtailed energy
    fig.add_trace(go.Scatter(x=df_timeline['TimeSlot'], y=df_timeline['cumulative_energy'], name='Energy', mode='lines', yaxis='y2'))

    # Update layout
    fig.update_layout(
        title='Facility ID: ' + str(selected_anlagenschlüssel),
        title_x=0.5,
        xaxis=dict(
            title='date',
            tickmode='array',
            tickvals=df_timeline['TimeSlot'][::len(df_timeline)//12],  # Adjust to show fewer x-axis labels
            tickformat='%Y-%m',
            tickangle=-45
        ),
        yaxis=dict(
            title='Power in kw',
        ),
        yaxis2=dict(
            title='Energy in kwh',
            overlaying='y',
            side='right',
            showgrid=False,  # Hide the gridlines for the secondary axis
        ),
        barmode='overlay',
        width = 800,
        height = 600,
        legend=dict(
            x=1,  
            y=1,
            orientation='v',
        )
    )
    return fig

def plot_single_plant_weekday(df_in,selected_anlagenschlüssel):
    df_timeline = df_in.copy()
    df_timeline['weekday'] = df_timeline['TimeSlot'].dt.weekday
    weekdays = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    weekday_mapping = {day: name for day, name in enumerate(weekdays)}
    df_timeline['weekday'] = pd.Categorical(df_timeline['weekday'].replace(weekday_mapping),
                                            categories=weekdays, 
                                            ordered=True)
    df_show = df_timeline.groupby('weekday').mean().reset_index()
    # Create a figure
    fig = go.Figure()
    
    # Add line plot for curtailment power
    fig.add_trace(go.Scatter(x=df_show['weekday'], y=df_show['curtailment_power'], name='Power', mode='lines', yaxis='y'))

    # Update layout
    fig.update_layout(
        title='Facility ID: ' + str(selected_anlagenschlüssel),
        title_x=0.5,
        xaxis=dict(
            title='date',
            tickmode='array',
            tickvals= weekdays,
        ),
        yaxis=dict(
            title='Power in kw',
        ),
        width = 800,
        height = 600,
        legend=dict(
            x=1,  
            y=1,
            orientation='v',
        )
    )
    return fig

def plot_single_plant_hour(df_in,selected_anlagenschlüssel):
    df_timeline = df_in.copy()
    df_timeline['hour'] = df_timeline['TimeSlot'].dt.hour
    df_show = df_timeline.groupby('hour').mean().reset_index()
    # Create a figure
    fig = go.Figure()
    
    # Add line plot for curtailment power
    fig.add_trace(go.Scatter(x=df_show['hour'], y=df_show['curtailment_power'], name='Power', mode='lines', yaxis='y'))

    # Update layout
    fig.update_layout(
        title='Facility ID: ' + str(selected_anlagenschlüssel),
        title_x=0.5,
        xaxis=dict(
            title='hour'
        ),
        yaxis=dict(
            title='Power in kw',
        ),
        width = 800,
        height = 600,
        legend=dict(
            x=1,  
            y=1,
            orientation='v',
        )
    )
    return fig