from kaggle.api.kaggle_api_extended import KaggleApi
import gspread
from google.oauth2.service_account import Credentials
import os
import pandas as pd
import schedule
import time

def download_data(api, path):
    # check if file exist
    existing_file = os.listdir(path)
    if existing_file:
        print("Data Already exists. Skipping Download")
        return
    
    #download dataset from kaggle
    api.dataset_download_files(
    dataset='pdaasha/ga4-obfuscated-sample-ecommerce-jan2021',
    path= path,
    unzip= True
)
    
def load_data(path):
    #check file ends with .csv
    files = [f for f in os.listdir(path) if f.endswith('.csv')]
    if not files:
        return None
    return pd.read_csv(os.path.join(path, files[0]), low_memory=False)

def process_data(df):
    # forward fill event_date, event_timestamp, event_name, and user_pseudo_id
    df[['event_date', 'event_timestamp','event_name', 'user_pseudo_id']] = df[['event_date', 'event_timestamp','event_name', 'user_pseudo_id']].ffill()
    
    # add universal value column
    df['value'] = (df['event_params.value.string_value']
                   .fillna(df['event_params.value.int_value'])
                   .fillna(df['event_params.value.float_value'])
                   .fillna(df['event_params.value.double_value'])
                   )
    
    # create event_df dataframe by pivoting df
    event_df = df.pivot_table(
    index=[
        'event_timestamp',
        'event_name',
        'user_pseudo_id'
    ],
    columns='event_params.key',
    values='value',
    aggfunc='first').reset_index()

    # adding dimension to the event_df
    extra_cols = df[[
    'event_timestamp',
    'event_name',
    'user_pseudo_id',
    'device.category',
    'device.mobile_brand_name',
    'device.web_info.browser',
    'geo.country',
    'traffic_source.medium',
    'traffic_source.name',
    'traffic_source.source']].groupby(['event_timestamp','event_name','user_pseudo_id']).first().reset_index()

    # merging event_df with the extra columns
    event_df = event_df.merge(
    extra_cols,
    on = ['event_timestamp','event_name','user_pseudo_id'],
    how= 'left')

    # create session_key, concate user_pseudo_id + ga_session_id
    event_df['session_key'] = (event_df['user_pseudo_id'].astype(str) + '_' + event_df['ga_session_id'].astype(str))

    session_df = event_df.groupby('session_key').agg({
    'user_pseudo_id': 'first',
    'ga_session_id': 'first',
    'device.category': 'first',
    'geo.country': 'first',
    'traffic_source.source': 'first',
    'traffic_source.medium': 'first',
    'engagement_time_msec': 'sum',
    'event_timestamp': 'min',}).reset_index()

    # convert engagement_time_msec to numeric
    session_df['engagement_time_msec'] = pd.to_numeric(session_df['engagement_time_msec'], errors='coerce')

    session_df = session_df.dropna()

    # Page views per session
    page_counts = (
    event_df[event_df['event_name'] == 'page_view']
    .groupby('session_key').size())
    
    session_df['page_views'] = session_df['session_key'].map(page_counts).fillna(0)

    # Event per session
    event_counts = event_df.groupby('session_key').size()
    session_df['events'] = session_df['session_key'].map(event_counts)

    # Bounce flag
    session_df['bounced'] = session_df['page_views'] == 1

    # Engaged session

    session_df['engaged_session'] = ((session_df['page_views'] >= 2) | (session_df['engagement_time_msec'] > 10000)).astype(int)

    print(session_df.head())

    return session_df

def upload_to_sheets(df):
        
        SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

        creds = Credentials.from_service_account_file(
            "./credentials/g-drive-api-1-b53a768ea031.json",
            scopes=SCOPES
        )

        client = gspread.authorize(creds)

        sheet = client.open("Session Data").sheet1
        
        sheet.clear()
        sheet.update([df.columns.values.tolist()] + df.values.tolist())
        


def script():
    print(f"Running automation at {time.strftime('%Y-%m-%d %H:%M:%S')}")
    api = KaggleApi()
    api.authenticate()

    path = './kaggle_dataset'
    os.makedirs(path, exist_ok=True)

    print("Downloading...")
    download_data(api, path)

    print("Loading...")
    df = load_data(path)

    if df is not None:
        print("Processing...")
        process_df = process_data(df)

        print("Uploading to Google Sheets...")
        upload_to_sheets(process_df)

        print("Done!")

schedule.every(10).seconds.do(script)
print("Scheduler starting...")

try:
    while True:
        print("Checking...")
        schedule.run_pending()
        time.sleep(1)
except KeyboardInterrupt:
    print("Scheduler Stopped")



# if __name__ == "__main__":
#     script()