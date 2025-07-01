import pandas as pd
import datetime

def get_event_timestamp(events_list, event_type):
    timestamps = []
    for events in events_list:
        found_timestamp = None
        for event in events:
            if event['type'] == event_type:
                found_timestamp = pd.to_datetime(event['timestamp'], utc=True)
                break
        timestamps.append(found_timestamp)
    return timestamps

def calc_duration_minutes(row):

    if pd.isna(row['delivered_time']) or pd.isna(row['start_time']):
        return pd.NA
    
    delta = row['delivered_time'] - row['start_time']
    
    return int(delta.total_seconds() // 60)


def format_utc_cols_with_t_z(df, columns):
    for col in columns:
        df[col] = df[col].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    return df

def calculate_delivery_status(row):
    if pd.isnull(row['delivered_time']):
        return "missing"
    else:
        time_difference = row['delivered_time'] - row['scheduled_time']
        if time_difference <= datetime.timedelta(minutes=15):
            return "on-time"
        else:
            return "late"