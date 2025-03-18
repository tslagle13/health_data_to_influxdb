# %%
import base64, requests, schedule, time, json, pytz, logging, os, sys
from requests.exceptions import ConnectionError
from datetime import datetime, timedelta
# for influxdb 1.x
from influxdb import InfluxDBClient
from influxdb.exceptions import InfluxDBClientError
# for influxdb 2.x
from influxdb_client import InfluxDBClient as InfluxDBClient2
from influxdb_client.client.exceptions import InfluxDBError
from influxdb_client.client.write_api import SYNCHRONOUS

# %% [markdown]
# ## Variables

# %%
FITBIT_LOG_FILE_PATH = os.environ.get("FITBIT_LOG_FILE_PATH", "/app/logs/fitbit.log")
TOKEN_FILE_PATH = os.environ.get("TOKEN_FILE_PATH", "/app/tokens/tokens.json")
OVERWRITE_LOG_FILE = True
FITBIT_LANGUAGE = os.environ.get("FITBIT_LANGUAGE", 'en_US')
INFLUXDB_VERSION = os.environ.get("INFLUXDB_VERSION", "2")
# Update these variables for influxdb 1.x versions
INFLUXDB_HOST = os.environ.get("INFLUXDB_HOST", 'influxdb')
INFLUXDB_PORT = int(os.environ.get("INFLUXDB_PORT", "8086"))
INFLUXDB_USERNAME = os.environ.get("INFLUXDB_USERNAME", 'tim')
INFLUXDB_PASSWORD = os.environ.get("INFLUXDB_PASSWORD", "")
INFLUXDB_DATABASE = os.environ.get("INFLUXDB_DATABASE", 'fitbit')
# Update these variables for influxdb 2.x versions
INFLUXDB_BUCKET = os.environ.get("INFLUXDB_BUCKET", "health_data")
INFLUXDB_ORG = os.environ.get("INFLUXDB_ORG", "home")
INFLUXDB_TOKEN = os.environ.get("INFLUXDB_TOKEN", "")
INFLUXDB_URL = os.environ.get("INFLUXDB_URL", "http://influxdb:8086")
# MAKE SURE you set the application type to PERSONAL. Otherwise, you won't have access to intraday data series, resulting in 40X errors.
client_id = os.environ.get("FITBIT_CLIENT_ID", "")
client_secret = os.environ.get("FITBIT_CLIENT_SECRET", "")
DEVICENAME = os.environ.get("FITBIT_DEVICE_NAME", "Pixel Watch 3")
ACCESS_TOKEN = "" # Empty Global variable initialization, will be replaced with a functional access code later using the refresh code
AUTO_DATE_RANGE = os.environ.get("AUTO_DATE_RANGE", "true").lower() == "true"
auto_update_date_range = int(os.environ.get("AUTO_UPDATE_DATE_RANGE", "1"))
LOCAL_TIMEZONE = os.environ.get("LOCAL_TIMEZONE", "America/New_York")
SCHEDULE_AUTO_UPDATE = True if AUTO_DATE_RANGE else False # Scheduling updates of data when script runs
SERVER_ERROR_MAX_RETRY = 3
EXPIRED_TOKEN_MAX_RETRY = 5
SKIP_REQUEST_ON_SERVER_ERROR = True

# %% [markdown]
# ## Logging setup

# %%
if OVERWRITE_LOG_FILE:
    with open(FITBIT_LOG_FILE_PATH, "w"): pass

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(FITBIT_LOG_FILE_PATH, mode='a'),
        logging.StreamHandler(sys.stdout)
    ]
)

# %% [markdown]
# ## Setting up base API Caller function

# %%
# Generic Request caller for all 
def request_data_from_fitbit(url, headers={}, params={}, data={}, request_type="get"):
    global ACCESS_TOKEN
    retry_attempts = 0
    logging.debug("Requesting data from fitbit via Url : " + url)
    while True: # Unlimited Retry attempts
        if request_type == "get":
            headers = {
                "Authorization": f"Bearer {ACCESS_TOKEN}",
                "Accept": "application/json",
                'Accept-Language': FITBIT_LANGUAGE
            }
        try:        
            if request_type == "get":
                response = requests.get(url, headers=headers, params=params, data=data)
            elif request_type == "post":
                response = requests.post(url, headers=headers, params=params, data=data)
            else:
                raise Exception("Invalid request type " + str(request_type))
        
            if response.status_code == 200: # Success
                return response.json()
            elif response.status_code == 429: # API Limit reached
                retry_after = int(response.headers["Fitbit-Rate-Limit-Reset"]) + 300 # Fitbit changed their headers.
                logging.warning("Fitbit API limit reached. Error code : " + str(response.status_code) + ", Retrying in " + str(retry_after) + " seconds")
                print("Fitbit API limit reached. Error code : " + str(response.status_code) + ", Retrying in " + str(retry_after) + " seconds")
                time.sleep(retry_after)
            elif response.status_code == 401: # Access token expired ( most likely )
                logging.info("Current Access Token : " + ACCESS_TOKEN)
                logging.warning("Error code : " + str(response.status_code) + ", Details : " + response.text)
                print("Error code : " + str(response.status_code) + ", Details : " + response.text)
                ACCESS_TOKEN = Get_New_Access_Token(client_id, client_secret)
                logging.info("New Access Token : " + ACCESS_TOKEN)
                time.sleep(30)
                if retry_attempts > EXPIRED_TOKEN_MAX_RETRY:
                    logging.error("Unable to solve the 401 Error. Please debug - " + response.text)
                    raise Exception("Unable to solve the 401 Error. Please debug - " + response.text)
            elif response.status_code in [500, 502, 503, 504]: # Fitbit server is down or not responding ( most likely ):
                logging.warning("Server Error encountered ( Code 5xx ): Retrying after 120 seconds....")
                time.sleep(120)
                if retry_attempts > SERVER_ERROR_MAX_RETRY:
                    logging.error("Unable to solve the server Error. Retry limit exceed. Please debug - " + response.text)
                    if SKIP_REQUEST_ON_SERVER_ERROR:
                        logging.warning("Retry limit reached for server error : Skipping request -> " + url)
                        return None
            else:
                logging.error("Fitbit API request failed. Status code: " + str(response.status_code) + " " + str(response.text) )
                print(f"Fitbit API request failed. Status code: {response.status_code}", response.text)
                response.raise_for_status()
                return None

        except ConnectionError as e:
            logging.error("Retrying in 5 minutes - Failed to connect to internet : " + str(e))
            print("Retrying in 5 minutes - Failed to connect to internet : " + str(e))
        retry_attempts += 1
        time.sleep(30)

# %% [markdown]
# ## Token Refresh Management

# %%
def refresh_fitbit_tokens(client_id, client_secret, refresh_token):
    logging.info("Attempting to refresh tokens...")
    url = "https://api.fitbit.com/oauth2/token"
    headers = {
        "Authorization": "Basic " + base64.b64encode((client_id + ":" + client_secret).encode()).decode(),
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token
    }
    json_data = request_data_from_fitbit(url, headers=headers, data=data, request_type="post")
    access_token = json_data["access_token"]
    new_refresh_token = json_data["refresh_token"]
    tokens = {
        "access_token": access_token,
        "refresh_token": new_refresh_token
    }
    with open(TOKEN_FILE_PATH, "w") as file:
        json.dump(tokens, file)
    logging.info("Fitbit token refresh successful!")
    return access_token, new_refresh_token

def load_tokens_from_file():
    with open(TOKEN_FILE_PATH, "r") as file:
        tokens = json.load(file)
        return tokens.get("access_token"), tokens.get("refresh_token")

def Get_New_Access_Token(client_id, client_secret):
    try:
        access_token, refresh_token = load_tokens_from_file()
    except FileNotFoundError:
        refresh_token = input("No token file found. Please enter a valid refresh token : ")
    access_token, refresh_token = refresh_fitbit_tokens(client_id, client_secret, refresh_token)
    return access_token

ACCESS_TOKEN = Get_New_Access_Token(client_id, client_secret)

# %% [markdown]
# ## Influxdb Database Initialization

# %%
if INFLUXDB_VERSION == "2":
    try:
        influxdbclient = InfluxDBClient2(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
        influxdb_write_api = influxdbclient.write_api(write_options=SYNCHRONOUS)
    except InfluxDBError as err:
        logging.error("Unable to connect with influxdb 2.x database! Aborted")
        raise InfluxDBError("InfluxDB connection failed:" + str(err))
elif INFLUXDB_VERSION == "1":
    try:
        influxdbclient = InfluxDBClient(host=INFLUXDB_HOST, port=INFLUXDB_PORT, username=INFLUXDB_USERNAME, password=INFLUXDB_PASSWORD)
        influxdbclient.switch_database(INFLUXDB_DATABASE)
    except InfluxDBClientError as err:
        logging.error("Unable to connect with influxdb 1.x database! Aborted")
        raise InfluxDBClientError("InfluxDB connection failed:" + str(err))
else:
    logging.error("No matching version found. Supported values are 1 and 2")
    raise InfluxDBClientError("No matching version found. Supported values are 1 and 2:")

def write_points_to_influxdb(points):
    if INFLUXDB_VERSION == "2":
        try:
            influxdb_write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=points)
            logging.info("Successfully updated influxdb database with new points")
        except InfluxDBError as err:
            logging.error("Unable to connect with influxdb 2.x database! " + str(err))
            print("Influxdb connection failed! ", str(err))
    elif INFLUXDB_VERSION == "1":
        try:
            influxdbclient.write_points(points)
            logging.info("Successfully updated influxdb database with new points")
        except InfluxDBClientError as err:
            logging.error("Unable to connect with influxdb 1.x database! " + str(err))
            print("Influxdb connection failed! ", str(err))
    else:
        logging.error("No matching version found. Supported values are 1 and 2")
        raise InfluxDBClientError("No matching version found. Supported values are 1 and 2:")

# %% [markdown]
# ## Set Timezone from profile data

# %%
if LOCAL_TIMEZONE == "Automatic":
    LOCAL_TIMEZONE = pytz.timezone(request_data_from_fitbit("https://api.fitbit.com/1/user/-/profile.json")["user"]["timezone"])
else:
    LOCAL_TIMEZONE = pytz.timezone(LOCAL_TIMEZONE)

# %% [markdown]
# ## Selecting Dates for update

# %%
if AUTO_DATE_RANGE:
    end_date = datetime.now(LOCAL_TIMEZONE)
    start_date = end_date - timedelta(days=auto_update_date_range)
    end_date_str = end_date.strftime("%Y-%m-%d")
    start_date_str = start_date.strftime("%Y-%m-%d")
else:
    start_date_str = input("Enter start date in YYYY-MM-DD format : ")
    end_date_str = input("Enter end date in YYYY-MM-DD format : ")
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

# %% [markdown]
# ## Setting up functions for Requesting data from server

# %%
collected_records = []

def update_working_dates():
    global end_date, start_date, end_date_str, start_date_str
    end_date = datetime.now(LOCAL_TIMEZONE)
    start_date = end_date - timedelta(days=auto_update_date_range)
    end_date_str = end_date.strftime("%Y-%m-%d")
    start_date_str = start_date.strftime("%Y-%m-%d")

# Get last synced battery level of the device
def get_battery_level():
    device = request_data_from_fitbit("https://api.fitbit.com/1/user/-/devices.json")[0]
    if device != None:
        collected_records.append({
            "measurement": "DeviceBatteryLevel",
            "time": LOCAL_TIMEZONE.localize(datetime.fromisoformat(device['lastSyncTime'])).astimezone(pytz.utc).isoformat(),
            "fields": {
                "value": float(device['batteryLevel'])
            }
        })
        logging.info("Recorded battery level for " + DEVICENAME)
    else:
        logging.error("Recording battery level failed : " + DEVICENAME)

# For intraday detailed data, max possible range in one day. 
def get_intraday_data_limit_1d(date_str, measurement_list):
    for measurement in measurement_list:
        data = request_data_from_fitbit('https://api.fitbit.com/1/user/-/activities/' + measurement[0] + '/date/' + date_str + '/1d/' + measurement[2] + '.json')["activities-" + measurement[0] + "-intraday"]['dataset']
        if data != None:
            for value in data:
                log_time = datetime.fromisoformat(date_str + "T" + value['time'])
                utc_time = LOCAL_TIMEZONE.localize(log_time).astimezone(pytz.utc).isoformat()
                collected_records.append({
                        "measurement":  measurement[1],
                        "time": utc_time,
                        "tags": {
                            "Device": DEVICENAME
                        },
                        "fields": {
                            "value": int(value['value'])
                        }
                    })
            logging.info("Recorded " +  measurement[1] + " intraday for date " + date_str)
        else:
            logging.error("Recording failed : " +  measurement[1] + " intraday for date " + date_str)

# Max range is 30 days, records BR, SPO2 Intraday, skin temp and HRV - 4 queries
def get_daily_data_limit_30d(start_date_str, end_date_str):

    hrv_data_list = request_data_from_fitbit('https://api.fitbit.com/1/user/-/hrv/date/' + start_date_str + '/' + end_date_str + '.json')['hrv']
    if hrv_data_list != None:
        for data in hrv_data_list:
            log_time = datetime.fromisoformat(data["dateTime"] + "T" + "00:00:00")
            utc_time = LOCAL_TIMEZONE.localize(log_time).astimezone(pytz.utc).isoformat()
            collected_records.append({
                    "measurement":  "HRV",
                    "time": utc_time,
                    "tags": {
                        "Device": DEVICENAME
                    },
                    "fields": {
                        "dailyRmssd": data["value"]["dailyRmssd"],
                        "deepRmssd": data["value"]["deepRmssd"]
                    }
                })
        logging.info("Recorded HRV for date " + start_date_str + " to " + end_date_str)
    else:
        logging.error("Recording failed HRV for date " + start_date_str + " to " + end_date_str)

    br_data_list = request_data_from_fitbit('https://api.fitbit.com/1/user/-/br/date/' + start_date_str + '/' + end_date_str + '.json')["br"]
    if br_data_list != None:
        for data in br_data_list:
            log_time = datetime.fromisoformat(data["dateTime"] + "T" + "00:00:00")
            utc_time = LOCAL_TIMEZONE.localize(log_time).astimezone(pytz.utc).isoformat()
            collected_records.append({
                    "measurement":  "BreathingRate",
                    "time": utc_time,
                    "tags": {
                        "Device": DEVICENAME
                    },
                    "fields": {
                        "value": data["value"]["breathingRate"]
                    }
                })
        logging.info("Recorded BR for date " + start_date_str + " to " + end_date_str)
    else:
        logging.error("Recording failed : BR for date " + start_date_str + " to " + end_date_str)

    skin_temp_data_list = request_data_from_fitbit('https://api.fitbit.com/1/user/-/temp/skin/date/' + start_date_str + '/' + end_date_str + '.json')["tempSkin"]
    if skin_temp_data_list != None:
        for temp_record in skin_temp_data_list:
            log_time = datetime.fromisoformat(temp_record["dateTime"] + "T" + "00:00:00")
            utc_time = LOCAL_TIMEZONE.localize(log_time).astimezone(pytz.utc).isoformat()
            collected_records.append({
                    "measurement":  "Skin Temperature Variation",
                    "time": utc_time,
                    "tags": {
                        "Device": DEVICENAME
                    },
                    "fields": {
                        "RelativeValue": temp_record["value"]["nightlyRelative"]
                    }
                })
        logging.info("Recorded Skin Temperature Variation for date " + start_date_str + " to " + end_date_str)
    else:
        logging.error("Recording failed : Skin Temperature Variation for date " + start_date_str + " to " + end_date_str)

    spo2_data_list = request_data_from_fitbit('https://api.fitbit.com/1/user/-/spo2/date/' + start_date_str + '/' + end_date_str + '/all.json')
    if spo2_data_list != None:
        for days in spo2_data_list:
            data = days["minutes"]
            for record in data: 
                log_time = datetime.fromisoformat(record["minute"])
                utc_time = LOCAL_TIMEZONE.localize(log_time).astimezone(pytz.utc).isoformat()
                collected_records.append({
                        "measurement":  "SPO2_Intraday",
                        "time": utc_time,
                        "tags": {
                            "Device": DEVICENAME
                        },
                        "fields": {
                            "value": float(record["value"]),
                        }
                    })
        logging.info("Recorded SPO2 intraday for date " + start_date_str + " to " + end_date_str)
    else:
        logging.error("Recording failed : SPO2 intraday for date " + start_date_str + " to " + end_date_str)

# Only for sleep data - limit 100 days - 1 query
def get_daily_data_limit_100d(start_date_str, end_date_str):

    sleep_data = request_data_from_fitbit('https://api.fitbit.com/1.2/user/-/sleep/date/' + start_date_str + '/' + end_date_str + '.json')["sleep"]
    if sleep_data != None:
        for record in sleep_data:
            log_time = datetime.fromisoformat(record["startTime"])
            utc_time = LOCAL_TIMEZONE.localize(log_time).astimezone(pytz.utc).isoformat()
            try:
                minutesLight= record['levels']['summary']['light']['minutes']
                minutesREM = record['levels']['summary']['rem']['minutes']
                minutesDeep = record['levels']['summary']['deep']['minutes']
            except:
                minutesLight= record['levels']['summary']['asleep']['minutes']
                minutesREM = record['levels']['summary']['restless']['minutes']
                minutesDeep = 0

            collected_records.append({
                    "measurement":  "Sleep Summary",
                    "time": utc_time,
                    "tags": {
                        "Device": DEVICENAME,
                        "isMainSleep": record["isMainSleep"],
                    },
                    "fields": {
                        'efficiency': record["efficiency"],
                        'minutesAfterWakeup': record['minutesAfterWakeup'],
                        'minutesAsleep': record['minutesAsleep'],
                        'minutesToFallAsleep': record['minutesToFallAsleep'],
                        'minutesInBed': record['timeInBed'],
                        'minutesAwake': record['minutesAwake'],
                        'minutesLight': minutesLight,
                        'minutesREM': minutesREM,
                        'minutesDeep': minutesDeep
                    }
                })
            
            sleep_level_mapping = {'wake': 3, 'rem': 2, 'light': 1, 'deep': 0, 'asleep': 1, 'restless': 2, 'awake': 3, 'unknown': 4}
            for sleep_stage in record['levels']['data']:
                log_time = datetime.fromisoformat(sleep_stage["dateTime"])
                utc_time = LOCAL_TIMEZONE.localize(log_time).astimezone(pytz.utc).isoformat()
                collected_records.append({
                        "measurement":  "Sleep Levels",
                        "time": utc_time,
                        "tags": {
                            "Device": DEVICENAME,
                            "isMainSleep": record["isMainSleep"],
                        },
                        "fields": {
                            'level': sleep_level_mapping[sleep_stage["level"]],
                            'duration_seconds': sleep_stage["seconds"]
                        }
                    })
            wake_time = datetime.fromisoformat(record["endTime"])
            utc_wake_time = LOCAL_TIMEZONE.localize(wake_time).astimezone(pytz.utc).isoformat()
            collected_records.append({
                        "measurement":  "Sleep Levels",
                        "time": utc_wake_time,
                        "tags": {
                            "Device": DEVICENAME,
                            "isMainSleep": record["isMainSleep"],
                        },
                        "fields": {
                            'level': sleep_level_mapping['wake'],
                            'duration_seconds': None
                        }
                    })
        logging.info("Recorded Sleep data for date " + start_date_str + " to " + end_date_str)
    else:
        logging.error("Recording failed : Sleep data for date " + start_date_str + " to " + end_date_str)

# Max date range 1 year, records HR zones, Activity minutes and Resting HR - 4 + 3 + 1 + 1 = 9 queries
def get_daily_data_limit_365d(start_date_str, end_date_str):
    activity_minutes_list = ["minutesSedentary", "minutesLightlyActive", "minutesFairlyActive", "minutesVeryActive"]
    for activity_type in activity_minutes_list:
        activity_minutes_data_list = request_data_from_fitbit('https://api.fitbit.com/1/user/-/activities/tracker/' + activity_type + '/date/' + start_date_str + '/' + end_date_str + '.json')["activities-tracker-"+activity_type]
        if activity_minutes_data_list != None:
            for data in activity_minutes_data_list:
                log_time = datetime.fromisoformat(data["dateTime"] + "T" + "00:00:00")
                utc_time = LOCAL_TIMEZONE.localize(log_time).astimezone(pytz.utc).isoformat()
                collected_records.append({
                        "measurement": "Activity Minutes",
                        "time": utc_time,
                        "tags": {
                            "Device": DEVICENAME
                        },
                        "fields": {
                            activity_type : int(data["value"])
                        }
                    })
            logging.info("Recorded " + activity_type + "for date " + start_date_str + " to " + end_date_str)
        else:
            logging.error("Recording failed : " + activity_type + " for date " + start_date_str + " to " + end_date_str)
        

    activity_others_list = ["distance", "calories", "steps"]
    for activity_type in activity_others_list:
        activity_others_data_list = request_data_from_fitbit('https://api.fitbit.com/1/user/-/activities/tracker/' + activity_type + '/date/' + start_date_str + '/' + end_date_str + '.json')["activities-tracker-"+activity_type]
        if activity_others_data_list != None:
            for data in activity_others_data_list:
                log_time = datetime.fromisoformat(data["dateTime"] + "T" + "00:00:00")
                utc_time = LOCAL_TIMEZONE.localize(log_time).astimezone(pytz.utc).isoformat()
                activity_name = "Total Steps" if activity_type == "steps" else activity_type
                collected_records.append({
                        "measurement": activity_name,
                        "time": utc_time,
                        "tags": {
                            "Device": DEVICENAME
                        },
                        "fields": {
                            "value" : float(data["value"])
                        }
                    })
            logging.info("Recorded " + activity_name + " for date " + start_date_str + " to " + end_date_str)
        else:
            logging.error("Recording failed : " + activity_name + " for date " + start_date_str + " to " + end_date_str)
        

    HR_zones_data_list = request_data_from_fitbit('https://api.fitbit.com/1/user/-/activities/heart/date/' + start_date_str + '/' + end_date_str + '.json')["activities-heart"]
    if HR_zones_data_list != None:
        for data in HR_zones_data_list:
            log_time = datetime.fromisoformat(data["dateTime"] + "T" + "00:00:00")
            utc_time = LOCAL_TIMEZONE.localize(log_time).astimezone(pytz.utc).isoformat()
            collected_records.append({
                    "measurement": "HR zones",
                    "time": utc_time,
                    "tags": {
                        "Device": DEVICENAME
                    },
                    # Using get() method with a default value 0 to prevent keyerror ( see issue #31)
                    "fields": {
                        "Normal" : data["value"]["heartRateZones"][0].get("minutes", 0),
                        "Fat Burn" :  data["value"]["heartRateZones"][1].get("minutes", 0),
                        "Cardio" :  data["value"]["heartRateZones"][2].get("minutes", 0),
                        "Peak" :  data["value"]["heartRateZones"][3].get("minutes", 0)
                    }
                })
            if "restingHeartRate" in data["value"]:
                collected_records.append({
                            "measurement":  "RestingHR",
                            "time": utc_time,
                            "tags": {
                                "Device": DEVICENAME
                            },
                            "fields": {
                                "value": data["value"]["restingHeartRate"]
                            }
                        })
        logging.info("Recorded RHR and HR zones for date " + start_date_str + " to " + end_date_str)
    else:
        logging.error("Recording failed : RHR and HR zones for date " + start_date_str + " to " + end_date_str)

# records SPO2 single days for the whole given period - 1 query
def get_daily_data_limit_none(start_date_str, end_date_str):
    data_list = request_data_from_fitbit('https://api.fitbit.com/1/user/-/spo2/date/' + start_date_str + '/' + end_date_str + '.json')
    if data_list != None:
        for data in data_list:
            log_time = datetime.fromisoformat(data["dateTime"] + "T" + "00:00:00")
            utc_time = LOCAL_TIMEZONE.localize(log_time).astimezone(pytz.utc).isoformat()
            collected_records.append({
                    "measurement":  "SPO2",
                    "time": utc_time,
                    "tags": {
                        "Device": DEVICENAME
                    },
                    "fields": {
                        "avg": data["value"]["avg"],
                        "max": data["value"]["max"],
                        "min": data["value"]["min"]
                    }
                })
        logging.info("Recorded Avg SPO2 for date " + start_date_str + " to " + end_date_str)
    else:
        logging.error("Recording failed : Avg SPO2 for date " + start_date_str + " to " + end_date_str)

def get_cardio_score(start_date_str, end_date_str):
    """Fetches Cardio Fitness Score (VO2 Max)"""
    data = request_data_from_fitbit(f'https://api.fitbit.com/1/user/-/cardioscore/date/{start_date_str}/{end_date_str}.json')
    if data and 'cardioScore' in data:
        for score in data['cardioScore']:
            log_time = datetime.fromisoformat(score['dateTime'] + "T00:00:00")
            utc_time = LOCAL_TIMEZONE.localize(log_time).astimezone(pytz.utc).isoformat()
            
            # Handle VO2 Max range values
            vo2_max = score['value']['vo2Max']
            if isinstance(vo2_max, str) and '-' in vo2_max:
                # If it's a range like "34-38", take the average
                low, high = map(float, vo2_max.split('-'))
                vo2_max_value = (low + high) / 2
            else:
                # If it's a single value
                vo2_max_value = float(vo2_max)

            collected_records.append({
                "measurement": "CardioScore",
                "time": utc_time,
                "tags": {
                    "Device": DEVICENAME
                },
                "fields": {
                    "value": vo2_max_value,
                    "range_low": low if isinstance(vo2_max, str) and '-' in vo2_max else vo2_max_value,
                    "range_high": high if isinstance(vo2_max, str) and '-' in vo2_max else vo2_max_value
                }
            })
        logging.info(f"Recorded Cardio Score for date {start_date_str} to {end_date_str}")
    else:
        logging.error(f"Recording failed: Cardio Score for date {start_date_str} to {end_date_str}")

def get_stress_score(start_date_str, end_date_str):
    """Fetches Daily Stress Score"""
    data = request_data_from_fitbit(f'https://api.fitbit.com/1/user/-/stress/score/date/{start_date_str}/{end_date_str}.json')
    if data and 'dailyStress' in data:
        for score in data['dailyStress']:
            log_time = datetime.fromisoformat(score['dateTime'] + "T00:00:00")
            utc_time = LOCAL_TIMEZONE.localize(log_time).astimezone(pytz.utc).isoformat()
            collected_records.append({
                "measurement": "StressScore",
                "time": utc_time,
                "tags": {
                    "Device": DEVICENAME
                },
                "fields": {
                    "value": score.get('value', None)
                }
            })
        logging.info(f"Recorded Stress Score for date {start_date_str} to {end_date_str}")
    else:
        logging.error(f"Recording failed: Stress Score for date {start_date_str} to {end_date_str}")

def get_temperature_data(start_date_str, end_date_str):
    """Fetches both core and skin temperature data"""
    # Core temperature
    core_data = request_data_from_fitbit(f'https://api.fitbit.com/1/user/-/temp/core/date/{start_date_str}/{end_date_str}.json')
    if core_data and 'tempCore' in core_data:
        for temp in core_data['tempCore']:
            log_time = datetime.fromisoformat(temp['dateTime'] + "T00:00:00")
            utc_time = LOCAL_TIMEZONE.localize(log_time).astimezone(pytz.utc).isoformat()
            collected_records.append({
                "measurement": "CoreTemperature",
                "time": utc_time,
                "tags": {
                    "Device": DEVICENAME
                },
                "fields": {
                    "value": float(temp['value']['value'])
                }
            })
        logging.info(f"Recorded Core Temperature for date {start_date_str} to {end_date_str}")
    else:
        logging.error(f"Recording failed: Core Temperature for date {start_date_str} to {end_date_str}")

def get_ecg_data(start_date_str, end_date_str):
    """Fetches ECG readings"""
    offset = 0
    limit = 10
    while True:
        params = {
            'beforeDate': end_date_str,
            'sort': 'desc',
            'limit': limit,
            'offset': offset
        }
        
        data = request_data_from_fitbit('https://api.fitbit.com/1/user/-/ecg/list.json', params=params)
        
        if not data or 'ecgReadings' not in data or not data['ecgReadings']:
            break
            
        for reading in data['ecgReadings']:
            try:
                # Check if we have the required fields
                if 'startTime' not in reading:
                    logging.warning(f"ECG reading missing startTime: {reading}")
                    continue

                log_time = datetime.fromisoformat(reading['startTime'].replace('Z', ''))
                utc_time = LOCAL_TIMEZONE.localize(log_time).astimezone(pytz.utc).isoformat()
                
                # Only process readings within our date range
                reading_date = reading['startTime'].split('T')[0]
                if reading_date < start_date_str:
                    break

                fields = {}
                if 'averageHeartRate' in reading:
                    fields['averageHeartRate'] = reading['averageHeartRate']
                if 'leadNumber' in reading:
                    fields['leadNumber'] = reading['leadNumber']
                if 'samplingFrequencyHz' in reading:
                    fields['samplingFrequencyHz'] = reading['samplingFrequencyHz']
                if 'waveformSamples' in reading:
                    fields['numberOfSamples'] = len(reading['waveformSamples'])

                collected_records.append({
                    "measurement": "ECG",
                    "time": utc_time,
                    "tags": {
                        "Device": DEVICENAME,
                        "classification": reading.get('resultClassification', 'unknown')
                    },
                    "fields": fields
                })
            except Exception as e:
                logging.error(f"Error processing ECG reading: {e}")
                continue
            
        # If we got fewer results than the limit, we've reached the end
        if len(data['ecgReadings']) < limit:
            break
            
        offset += limit
        time.sleep(1)  # Rate limiting precaution
        
    if collected_records:
        logging.info(f"Recorded ECG data before date {end_date_str}")
    else:
        logging.warning(f"No ECG data found for date range {start_date_str} to {end_date_str}")

def get_water_logs(start_date_str, end_date_str):
    """Fetches water consumption logs"""
    data = request_data_from_fitbit(f'https://api.fitbit.com/1/user/-/foods/log/water/date/{start_date_str}/{end_date_str}.json')
    if data and 'foods-log-water' in data:
        for day in data['foods-log-water']:
            try:
                log_time = datetime.fromisoformat(day['dateTime'] + "T00:00:00")
                utc_time = LOCAL_TIMEZONE.localize(log_time).astimezone(pytz.utc).isoformat()
                collected_records.append({
                    "measurement": "WaterLog",
                    "time": utc_time,
                    "tags": {
                        "Device": DEVICENAME
                    },
                    "fields": {
                        "amount": float(day['value'])  # Amount in mL
                    }
                })
            except (KeyError, ValueError) as e:
                logging.error(f"Error processing water log entry: {e}")
                continue
                
        logging.info(f"Recorded Water Logs for date {start_date_str} to {end_date_str}")
    else:
        logging.warning(f"No water logs found for date {start_date_str} to {end_date_str}")

def get_food_logs(date_str):
    """Fetches food logs and nutrition data for a single day"""
    data = request_data_from_fitbit(f'https://api.fitbit.com/1/user/-/foods/log/date/{date_str}.json')
    if data and 'foods' in data:
        log_time = datetime.fromisoformat(date_str + "T00:00:00")
        utc_time = LOCAL_TIMEZONE.localize(log_time).astimezone(pytz.utc).isoformat()
        
        # Daily totals
        if 'summary' in data:
            collected_records.append({
                "measurement": "NutritionSummary",
                "time": utc_time,
                "tags": {
                    "Device": DEVICENAME
                },
                "fields": {
                    "calories": float(data['summary'].get('calories', 0)),
                    "carbs": float(data['summary'].get('carbs', 0)),
                    "fat": float(data['summary'].get('fat', 0)),
                    "fiber": float(data['summary'].get('fiber', 0)),
                    "protein": float(data['summary'].get('protein', 0)),
                    "sodium": float(data['summary'].get('sodium', 0))
                }
            })
        
        # Individual food logs
        for food in data['foods']:
            meal_time = datetime.fromisoformat(food['logDate'] + "T" + food.get('logTime', "00:00:00"))
            utc_meal_time = LOCAL_TIMEZONE.localize(meal_time).astimezone(pytz.utc).isoformat()
            collected_records.append({
                "measurement": "FoodLog",
                "time": utc_meal_time,
                "tags": {
                    "Device": DEVICENAME,
                    "mealType": food['loggedFood']['mealTypeId'],
                    "foodName": food['loggedFood']['name']
                },
                "fields": {
                    "calories": float(food['loggedFood']['calories']),
                    "amount": float(food['loggedFood']['amount']),
                    "unitId": int(food['loggedFood']['unit']['id'])
                }
            })
        logging.info(f"Recorded Food Logs for date {date_str}")
    else:
        logging.error(f"Recording failed: Food Logs for date {date_str}")

def get_body_measurements(start_date_str, end_date_str):
    """Fetches body measurements including weight, BMI, body fat, etc."""
    # Weight and BMI
    weight_data = request_data_from_fitbit(f'https://api.fitbit.com/1/user/-/body/log/weight/date/{start_date_str}/{end_date_str}.json')
    if weight_data and 'weight' in weight_data:
        for measurement in weight_data['weight']:
            log_time = datetime.fromisoformat(measurement['date'] + "T" + measurement['time'])
            utc_time = LOCAL_TIMEZONE.localize(log_time).astimezone(pytz.utc).isoformat()
            collected_records.append({
                "measurement": "BodyMeasurements",
                "time": utc_time,
                "tags": {
                    "Device": DEVICENAME,
                    "source": measurement['source']
                },
                "fields": {
                    "weight": float(measurement['weight']),
                    "bmi": float(measurement.get('bmi', 0))
                }
            })
    
    # Body Fat
    fat_data = request_data_from_fitbit(f'https://api.fitbit.com/1/user/-/body/log/fat/date/{start_date_str}/{end_date_str}.json')
    if fat_data and 'fat' in fat_data:
        for measurement in fat_data['fat']:
            log_time = datetime.fromisoformat(measurement['date'] + "T" + measurement.get('time', '00:00:00'))
            utc_time = LOCAL_TIMEZONE.localize(log_time).astimezone(pytz.utc).isoformat()
            collected_records.append({
                "measurement": "BodyFat",
                "time": utc_time,
                "tags": {
                    "Device": DEVICENAME,
                    "source": measurement['source']
                },
                "fields": {
                    "fat": float(measurement['fat'])
                }
            })
    
    logging.info(f"Recorded Body Measurements for date {start_date_str} to {end_date_str}")

def get_exercise_goals():
    """Fetches current exercise and activity goals"""
    try:
        # Get daily goals
        data = request_data_from_fitbit('https://api.fitbit.com/1/user/-/activities/goals/daily.json')
        logging.info(f"Received daily goals data: {data}")
        
        if data and 'goals' in data:  # Check for 'goals' key
            current_time = datetime.now(LOCAL_TIMEZONE)
            utc_time = current_time.astimezone(pytz.utc).isoformat()
            collected_records.append({
                "measurement": "ActivityGoals",
                "time": utc_time,
                "tags": {
                    "Device": DEVICENAME,
                    "type": "daily"
                },
                "fields": {
                    "caloriesOut": int(data['goals'].get('caloriesOut', 0)),
                    "distance": float(data['goals'].get('distance', 0)),
                    "floors": int(data['goals'].get('floors', 0)),
                    "steps": int(data['goals'].get('steps', 0)),
                    "activeMinutes": int(data['goals'].get('activeMinutes', 0)),
                    "activeZoneMinutes": int(data['goals'].get('activeZoneMinutes', 0))  # Added this new field
                }
            })
            
        # Get weekly goals
        weekly_data = request_data_from_fitbit('https://api.fitbit.com/1/user/-/activities/goals/weekly.json')
        logging.info(f"Received weekly goals data: {weekly_data}")
        
        if weekly_data and 'goals' in weekly_data:  # Check for 'goals' key
            current_time = datetime.now(LOCAL_TIMEZONE)
            utc_time = current_time.astimezone(pytz.utc).isoformat()
            collected_records.append({
                "measurement": "ActivityGoals",
                "time": utc_time,
                "tags": {
                    "Device": DEVICENAME,
                    "type": "weekly"
                },
                "fields": {
                    "distance": float(weekly_data['goals'].get('distance', 0)),
                    "floors": int(weekly_data['goals'].get('floors', 0)),
                    "steps": int(weekly_data['goals'].get('steps', 0)),
                    "activeMinutes": int(weekly_data['goals'].get('activeMinutes', 0))
                }
            })
            
        logging.info("Recorded Activity Goals")
    except Exception as e:
        logging.error(f"Error fetching activity goals: {e}")

def get_activity_summary(date_str):
    """Fetches activity summary for a specific date"""
    try:
        data = request_data_from_fitbit(f'https://api.fitbit.com/1/user/-/activities/date/{date_str}.json')
        if data and 'summary' in data:
            log_time = datetime.fromisoformat(date_str + "T00:00:00")
            utc_time = LOCAL_TIMEZONE.localize(log_time).astimezone(pytz.utc).isoformat()
            
            summary = data['summary']
            collected_records.append({
                "measurement": "ActivitySummary",
                "time": utc_time,
                "tags": {
                    "Device": DEVICENAME
                },
                "fields": {
                    "caloriesOut": summary.get('caloriesOut', 0),
                    "activityCalories": summary.get('activityCalories', 0),
                    "steps": summary.get('steps', 0),
                    "floors": summary.get('floors', 0),
                    "sedentaryMinutes": summary.get('sedentaryMinutes', 0),
                    "lightlyActiveMinutes": summary.get('lightlyActiveMinutes', 0),
                    "fairlyActiveMinutes": summary.get('fairlyActiveMinutes', 0),
                    "veryActiveMinutes": summary.get('veryActiveMinutes', 0)
                }
            })
            logging.info(f"Recorded Activity Summary for date {date_str}")
    except Exception as e:
        logging.error(f"Error fetching activity summary for {date_str}: {e}")

def fetch_latest_activities(end_date_str):
    """Fetches the last 50 activities including today's activities
    
    Args:
        end_date_str: Current date in YYYY-MM-DD format
    """
    tomorrow = (datetime.strptime(end_date_str, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    
    params = {
        'beforeDate': tomorrow,
        'sort': 'desc',
        'limit': 50,
        'offset': 0
    }
    
    logging.info(f"Fetching last 50 activities up through {end_date_str}")
    recent_activities_data = request_data_from_fitbit('https://api.fitbit.com/1/user/-/activities/list.json', params=params)
    
    if recent_activities_data and 'activities' in recent_activities_data:
        activity_count = len(recent_activities_data['activities'])
        logging.info(f"Retrieved {activity_count} activities")
        
        for activity in recent_activities_data['activities']:
            starttime = datetime.fromisoformat(activity['startTime'].strip("Z"))
            utc_time = starttime.astimezone(pytz.utc).isoformat()
            
            # Create a single record per activity with all relevant metrics
            fields = {
                # Duration metrics
                'duration_minutes': round(float(activity.get('duration', 0)) / (1000 * 60), 2),  # Convert ms to minutes
                'active_duration_minutes': round(float(activity.get('activeDuration', 0)) / (1000 * 60), 2),
                
                # Performance metrics
                'average_heart_rate': activity.get('averageHeartRate', 0),
                'calories': activity.get('calories', 0),
                'steps': activity.get('steps', 0),
                'distance_km': round(float(activity.get('distance', 0)), 3),
            }
            
            # Add speed if available (converted to km/h)
            if 'speed' in activity:
                fields['speed_kmh'] = round(float(activity['speed']) * 3.6, 2)  # Convert m/s to km/h
            
            # Add pace if available (in min/km)
            if 'pace' in activity:
                fields['pace_min_km'] = round(float(activity['pace']) / 60, 2)
            
            # Add elevation data if available
            if 'elevationGain' in activity:
                fields['elevation_gain_meters'] = float(activity['elevationGain'])
            
            # Heart rate zones as percentage of total active time
            if 'heartRateZones' in activity:
                total_zone_minutes = sum(zone.get('minutes', 0) for zone in activity['heartRateZones'])
                if total_zone_minutes > 0:
                    for zone in activity['heartRateZones']:
                        zone_name = zone.get('name', '').lower().replace(' ', '_')
                        fields[f'hr_zone_{zone_name}_pct'] = round((zone.get('minutes', 0) / total_zone_minutes) * 100, 1)
            
            collected_records.append({
                "measurement": "Activities",
                "time": utc_time,
                "tags": {
                    "activity_name": activity.get('activityName', 'Unknown-Activity'),
                    "device": DEVICENAME,
                    "log_type": activity.get('logType', 'automatic'),  # Track if manually logged
                    "activity_date": starttime.strftime("%Y-%m-%d")    # Add date as tag for easier daily queries
                },
                "fields": fields
            })
            
        if activity_count > 0:
            first_activity_date = recent_activities_data['activities'][-1]['startTime'].split('T')[0]
            last_activity_date = recent_activities_data['activities'][0]['startTime'].split('T')[0]
            logging.info(f"Activities range from {first_activity_date} to {last_activity_date}")
    else:
        logging.warning(f"No activities found through {end_date_str}")

# Add these new functions after your existing functions but before the scheduling section

def get_sleep_score(start_date_str, end_date_str):
    """Fetches sleep score data including detailed score breakdown"""
    data = request_data_from_fitbit(f'https://api.fitbit.com/1.2/user/-/sleep/score/date/{start_date_str}/{end_date_str}.json')
    if data and 'sleepScores' in data:
        for day_score in data['sleepScores']:
            log_time = datetime.fromisoformat(day_score['dateTime'] + "T00:00:00")
            utc_time = LOCAL_TIMEZONE.localize(log_time).astimezone(pytz.utc).isoformat()
            
            # Overall sleep score
            fields = {
                'overall_score': day_score.get('overallScore', 0)
            }
            
            # Add detailed breakdown if available
            if 'scoreComponents' in day_score:
                components = day_score['scoreComponents']
                fields.update({
                    'composition_score': components.get('composition', 0),
                    'revitalization_score': components.get('revitalization', 0),
                    'duration_score': components.get('duration', 0),
                    'deep_sleep_score': components.get('deepSleep', 0),
                    'quality_score': components.get('qualityOfSleep', 0)
                })
            
            collected_records.append({
                "measurement": "SleepScore",
                "time": utc_time,
                "tags": {
                    "Device": DEVICENAME
                },
                "fields": fields
            })
        logging.info(f"Recorded Sleep Scores for date range {start_date_str} to {end_date_str}")
    else:
        logging.warning(f"No sleep score data found for date range {start_date_str} to {end_date_str}")

def get_lifetime_stats():
    """Fetches lifetime activity statistics"""
    data = request_data_from_fitbit('https://api.fitbit.com/1/user/-/activities.json')
    if data and 'lifetime' in data:
        current_time = datetime.now(LOCAL_TIMEZONE)
        utc_time = current_time.astimezone(pytz.utc).isoformat()
        
        lifetime = data['lifetime']
        
        # Process tracker data
        if 'tracker' in lifetime:
            tracker = lifetime['tracker']
            collected_records.append({
                "measurement": "LifetimeStats",
                "time": utc_time,
                "tags": {
                    "Device": DEVICENAME,
                    "source": "tracker"
                },
                "fields": {
                    "distance": float(tracker.get('distance', 0)),
                    "floors": int(tracker.get('floors', 0)),
                    "steps": int(tracker.get('steps', 0))
                }
            })
        
        # Process total data (includes manual entries)
        if 'total' in lifetime:
            total = lifetime['total']
            collected_records.append({
                "measurement": "LifetimeStats",
                "time": utc_time,
                "tags": {
                    "Device": DEVICENAME,
                    "source": "total"  # includes both tracker and manual entries
                },
                "fields": {
                    "distance": float(total.get('distance', 0)),
                    "floors": int(total.get('floors', 0)),
                    "steps": int(total.get('steps', 0))
                }
            })
        
        logging.info("Recorded Lifetime Stats")
    else:
        logging.warning("No lifetime stats data found")

# %% [markdown]
# ## Call the functions one time as a startup update OR do switch to bulk update mode

# %%
if AUTO_DATE_RANGE:
    date_list = [(start_date + timedelta(days=i)).strftime("%Y-%m-%d") for i in range((end_date - start_date).days + 1)]
    
    if len(date_list) > 3:
        logging.warn("Auto schedule update is not meant for more than 3 days at a time...")
    for date_str in date_list:
        get_intraday_data_limit_1d(date_str, [('heart','HeartRate_Intraday','1sec'),('steps','Steps_Intraday','1min')]) # 2 queries x number of dates ( default 2)
    get_daily_data_limit_30d(start_date_str, end_date_str) # 3 queries
    get_daily_data_limit_100d(start_date_str, end_date_str) # 1 query
    get_daily_data_limit_365d(start_date_str, end_date_str) # 8 queries
    get_daily_data_limit_none(start_date_str, end_date_str) # 1 query
    get_cardio_score(start_date_str, end_date_str) # 1 query
    get_temperature_data(start_date_str, end_date_str) # 1 query
    get_ecg_data(start_date_str, end_date_str) # 1 query
    get_water_logs(start_date_str, end_date_str) # 1 query
    get_food_logs(start_date_str) # 1 query
    get_body_measurements(start_date_str, end_date_str) # 1 query
    get_exercise_goals() # 1 query
    # Get activity summaries for each day
    for date in date_list:
        get_activity_summary(date)
    get_battery_level() # 1 query
    fetch_latest_activities(end_date_str) # 1 query
    get_lifetime_stats()
    write_points_to_influxdb(collected_records) 
    collected_records = []
else:
    # Do Bulk update----------------------------------------------------------------------------------------------------------------------------
    schedule.every(1).hours.do(lambda : Get_New_Access_Token(client_id,client_secret)) # Auto-refresh tokens every 1 hour
    
    date_list = [(start_date + timedelta(days=i)).strftime("%Y-%m-%d") for i in range((end_date - start_date).days + 1)]

    def yield_dates_with_gap(date_list, gap):
        start_index = -1*gap
        while start_index < len(date_list)-1:
            start_index  = start_index + gap
            end_index = start_index+gap
            if end_index > len(date_list) - 1:
                end_index = len(date_list) - 1
            if start_index > len(date_list) - 1:
                break
            yield (date_list[start_index],date_list[end_index])

    def do_bulk_update(funcname, start_date, end_date):
        global collected_records
        funcname(start_date, end_date)
        schedule.run_pending()
        write_points_to_influxdb(collected_records)
        collected_records = []

    fetch_latest_activities(date_list[-1])
    write_points_to_influxdb(collected_records)
    do_bulk_update(get_daily_data_limit_none, date_list[0], date_list[-1])
    for date_range in yield_dates_with_gap(date_list, 360):
        do_bulk_update(get_daily_data_limit_365d, date_range[0], date_range[1])
    for date_range in yield_dates_with_gap(date_list, 98):
        do_bulk_update(get_daily_data_limit_100d, date_range[0], date_range[1])
    for date_range in yield_dates_with_gap(date_list, 28):
        do_bulk_update(get_daily_data_limit_30d, date_range[0], date_range[1])
    for single_day in date_list:
        do_bulk_update(get_intraday_data_limit_1d, single_day, [('heart','HeartRate_Intraday','1sec'),('steps','Steps_Intraday','1min')])

    logging.info("Success : Bulk update complete for " + start_date_str + " to " + end_date_str)
    print("Bulk update complete!")

# %% [markdown]
# ## Schedule functions at specific intervals (Ongoing continuous update)

# %%
# Ongoing continuous update of data
if SCHEDULE_AUTO_UPDATE:
    
    schedule.every(1).hours.do(lambda : Get_New_Access_Token(client_id,client_secret)) # Auto-refresh tokens every 1 hour
    schedule.every(3).minutes.do( lambda : get_intraday_data_limit_1d(end_date_str, [('heart','HeartRate_Intraday','1sec'),('steps','Steps_Intraday','1min')] )) # Auto-refresh detailed HR and steps
    schedule.every(1).hours.do( lambda : get_intraday_data_limit_1d((datetime.strptime(end_date_str, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d"), [('heart','HeartRate_Intraday','1sec'),('steps','Steps_Intraday','1min')] )) # Refilling any missing data on previous day end of night due to fitbit sync delay ( see issue #10 )
    schedule.every(20).minutes.do(get_battery_level) # Auto-refresh battery level
    schedule.every(3).hours.do(lambda : get_daily_data_limit_30d(start_date_str, end_date_str))
    schedule.every(4).hours.do(lambda : get_daily_data_limit_100d(start_date_str, end_date_str))
    schedule.every(6).hours.do( lambda : get_daily_data_limit_365d(start_date_str, end_date_str))
    schedule.every(6).hours.do(lambda : get_daily_data_limit_none(start_date_str, end_date_str))
    schedule.every(1).hours.do( lambda : fetch_latest_activities(end_date_str))
    schedule.every(6).hours.do(lambda : get_cardio_score(start_date_str, end_date_str))
    schedule.every(6).hours.do(lambda : get_temperature_data(start_date_str, end_date_str))
    schedule.every(1).hours.do(lambda : get_ecg_data(start_date_str, end_date_str))
    schedule.every(1).hours.do(lambda : get_water_logs(start_date_str, end_date_str))
    schedule.every(1).hours.do(lambda : get_food_logs(start_date_str))
    schedule.every(1).hours.do(lambda : get_body_measurements(start_date_str, end_date_str))
    schedule.every(1).hours.do(lambda : get_exercise_goals())
    schedule.every(1).days.do(lambda: get_activity_summary(end_date_str))
    schedule.every(12).hours.do(get_lifetime_stats)  # Lifetime stats don't change frequently
    while True:
        schedule.run_pending()
        if len(collected_records) != 0:
            write_points_to_influxdb(collected_records)
            collected_records = []
        time.sleep(30)
        update_working_dates()
