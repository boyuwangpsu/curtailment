import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from statsmodels.tsa.arima.model import ARIMA
from prophet import Prophet
import seaborn as sns

def prediction_plot(df: pd.DataFrame, start_date: pd.Timestamp, method:str):
    df[['curtailment_power', 'curtailment_power_pred']].plot(figsize=(20, 6))
    plt.title(f'{method}'+' Prediction---MAE: {:.2f}'.format(np.mean(np.abs(df.loc[df.TimeSlot >= start_date, 'curtailment_power'] - df.loc[df.TimeSlot >= start_date, 'curtailment_power_pred']))))
    plt.show()

def curtailment_power_prediction(df_in: pd.DataFrame, start_date: pd.Timestamp, method:str, plot=True) -> pd.DataFrame:
    df = df_in.copy()
    df['curtailment_power_pred'] = None
    #from start_date loop all time slots
    for target_time in df.loc[df.TimeSlot >= start_date, 'TimeSlot']:
        training_data = df[df.TimeSlot < target_time]['curtailment_power']
        if method =='Naive':
            prediction = df[df.TimeSlot < target_time]['curtailment_power'][-1]
        elif method == 'WSS':
            prediction =wss_forecast(training_data)
        elif method =='Croston':
            croston = Croston(alpha=0.3,beta=0.2)
            croston.fit(training_data.values, method='tsb')
            prediction = croston.forecast(steps=1)
        df.loc[df.TimeSlot==target_time, 'curtailment_power_pred'] = prediction
    if plot:
        prediction_plot(df, start_date, method)
    return df

class Croston:
    def __init__(self, alpha=0.4, beta=0.4):
        self.alpha = alpha
        self.beta = beta
        self.data = None
        self.fitted = False

    def fit(self, ts:np.array, method='standard'):
        self.data = ts
        cols = len(self.data)
        if method == 'standard':
            self.a, self.p, self.f = self._croston_standard(self.data, cols)
        elif method == 'tsb':
            self.a, self.p, self.f = self._croston_tsb(self.data, cols)
        else:
            raise ValueError("Invalid method specified. Use 'standard' or 'tsb'.")
        self.fitted = True

    def forecast(self, steps=3):
        if not self.fitted:
            raise RuntimeError("Model not fitted. Call fit method first.")
        return np.repeat(np.mean(self.f[-1]), steps)

    def _croston_standard(self, d, cols):
        #level (a), periodicity (p) and forecast (f)
        a, p, f = np.full((3, cols+1), np.nan)
        q = 1  # periods since last demand observation

        # Initialization
        first_occurrence = np.argmax(d > 0)
        a[0] = d[first_occurrence]
        p[0] = 1 + first_occurrence
        f[0] = a[0] / p[0]

        # Create all the t+1 forecasts
        for t in range(0, cols):        
            if d[t] > 0:
                a[t+1] = self.alpha * d[t] + (1 - self.alpha) * a[t]
                p[t+1] = self.alpha * q + (1 - self.alpha) * p[t]
                f[t+1] = a[t+1] / p[t+1]
                q = 1           
            else:
                a[t+1] = a[t]
                p[t+1] = p[t]
                f[t+1] = f[t]
                q += 1
        return a, p, f

    def _croston_tsb(self, d, cols):
        #level (a), probability (p) and forecast (f)
        a, p, f = np.full((3, cols+1), np.nan)

        # Initialization
        first_occurrence = np.argmax(d > 0)
        a[0] = d[first_occurrence]
        p[0] = 1 / (1 + first_occurrence)
        f[0] = p[0] * a[0]

        # Create all the t+1 forecasts
        for t in range(0, cols):
            if d[t] > 0:
                a[t+1] = self.alpha * d[t] + (1 - self.alpha) * a[t]
                p[t+1] = self.beta * 1 + (1 - self.beta) * p[t]
            else:
                a[t+1] = a[t]
                p[t+1] = (1 - self.beta) * p[t]
            f[t+1] = p[t+1]*a[t+1]
        return a, p, f

def wss_forecast(df:pd.Series, lead_time=1, quantile=0.75, n_samples=1000)->int:
    demand_data = df.copy()
    # Convert demand data to a binary series
    demand_binary = (demand_data > 0).astype(int)

    # Estimate transition probabilities
    transition_matrix = pd.crosstab(demand_binary.shift(), demand_binary, normalize='index')

    # Initialize forecast samples
    forecasts = []
    for _ in range(n_samples):
        forecast = []
        current_state = demand_binary.iloc[-1]
        for _ in range(lead_time):
            current_state = np.random.choice([0, 1], p=transition_matrix.loc[current_state])
            if current_state == 1:
                positive_demand_sample = np.random.choice(demand_data[demand_data > 0], 1)[0]
                forecast.append(positive_demand_sample)
            else:
                forecast.append(0)
        forecasts.append(forecast)

    return np.quantile(forecasts, quantile, axis=0)

def naive_predict(df_in: pd.DataFrame, start_date: pd.Timestamp, plot=True) -> pd.DataFrame:
    df = df_in.copy()
    # Add a column for naive predictions
    df['Naive_Prediction'] = None
    df.loc[df.index >= start_date, 'Naive_Prediction'] = df['curtailment_power'].shift(24).loc[df.index >= start_date]
    # Plot the predictions vs the actual values
    if plot:
        df[['curtailment_power', 'Naive_Prediction']].plot(figsize=(20, 6))
        plt.title('Naive Predicion---MAE: {:.2f}'.format(np.mean(np.abs(df.loc[df.index >= start_date, 'curtailment_power'] - df.loc[df.index >= start_date, 'Naive_Prediction']))))
        plt.show()
    return df

def arima_predict(df_in: pd.DataFrame, start_date: pd.Timestamp,plot=True) -> pd.DataFrame:
    df = df_in.copy()
    df['ARIMA_Prediction'] = None
    df['day'] = df['timestamp'].dt.date
    for target_date in df.loc[df.timestamp >= start_date, 'day'].unique():
        training_data = df[df.day < target_date]['Day-ahead Price [EUR/MWh]']

        # Fit ARIMA model
        model = ARIMA(training_data, order=(7, 0, 1)) 
        model_fit = model.fit()

        # Forecasting the next 24 hours
        forecast = model_fit.forecast(steps=24)

        # Update predictions in DataFrame
        forecast_timestamp = pd.date_range(start=target_date, periods=24, freq='H')
        df.loc[df.day==target_date, 'ARIMA_Prediction'] = forecast.values
    # Plot the predictions vs the actual values
    if plot:
        df[['Day-ahead Price [EUR/MWh]', 'ARIMA_Prediction']].plot(figsize=(20, 6))
        plt.title('ARIMA_Prediction---MAE: {:.2f}'.format(np.mean(np.abs(df.loc[df.timestamp >= start_date, 'Day-ahead Price [EUR/MWh]'] - df.loc[df.timestamp >= start_date, 'ARIMA_Prediction']))))
        plt.show()
    return df

def algo_prophet(price: pd.DataFrame, start_date: pd.Timestamp, plot=True) -> pd.DataFrame:
    # Prepare data for Prophet
    df = price[['timestamp', 'Day-ahead Price [EUR/MWh]']].copy()
    df.columns = ['ds', 'y']
    df['day'] = df['ds'].dt.date
    df['Prophet_Prediction'] = None

    for target_date in df.loc[df.ds >= start_date, 'day'].unique():
        model = Prophet(daily_seasonality=True)
        model.fit(df[df['day'] < target_date])
        future = model.make_future_dataframe(periods=24, freq='H', include_history=False)
        forecast = model.predict(future)
        forecast_timestamp = pd.date_range(start=target_date, periods=24, freq='H')
        df.loc[df.day==target_date, 'Prophet_Prediction'] = forecast.set_index('ds')['yhat'].values
    # Plot the predictions vs the actual values
    df.columns = ['timestamp', 'Day-ahead Price [EUR/MWh]', 'day', 'Prophet_Prediction']
    if plot:
        df[['Day-ahead Price [EUR/MWh]', 'Prophet_Prediction']].plot(figsize=(20, 6))
        plt.title('Prophet_Prediction---MAE: {:.2f}'.format(np.mean(np.abs(df.loc[df.timestamp >= start_date, 'Day-ahead Price [EUR/MWh]'] - df.loc[df.timestamp >= start_date, 'Prophet_Prediction']))))
        plt.show()

    return df