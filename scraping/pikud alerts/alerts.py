from infra.etl import ETL
import requests
import json
import time


HOST_ARGS = {
            'host' : 'localhost',
            'user' : 'root',
            'password' : 'root',
            'database' : 'pikud'
            }

SQL_DIR = 'scraping/pikud alerts/sql'
CITIES_TABLE = 'cities'
ALERTS_TABLE = 'alerts'

DAYS = ['24.01.2024']  ## ------------------------------------------


def check_valid_field(row, tag):
    try:
        row_value = row[tag]
    except:
        return ''
    else:
        return row_value


## cities API request 
# url = 'https://www.oref.org.il/Shared/Ajax/GetDistricts.aspx?lang=en'
# cities_data = requests.get(url=url).json()


## import cities data from file
cities_file = 'scraping/pikud alerts/cities.txt'
read_cities_file = open(cities_file, "r").read()
cities_data = json.loads(read_cities_file)

cities_data_list = []
for row in cities_data:
    cities_row_data = [row['label'], row['value'], row['id'], row['areaid'], row['areaname'], row['label_he'], row['migun_time']]
    cities_data_list.append(cities_row_data)

etl = ETL(**HOST_ARGS)

## initiliaze tables
# etl.exec_script(script_dir=f"{SQL_DIR}/init.sql")

## insert cities data
# etl.insert_bulk(table=CITIES_TABLE, truncate='y', data=cities_data_list)

city_names = set(c[0] for c in cities_data_list)

## read alerts data from file
# alerts_file = 'scraping/pikud alerts/alerts.txt'
# read_alerts_file = open(alerts_file, "r").read()
# alerts_data = json.loads(read_alerts_file)


# etl.exec_script(script_dir=f"{SQL_DIR}/1_trunc_alerts.sql")

for day in DAYS:
    print(f"Day: {day}")
    for city in city_names:
        city_name = city
        # api for alerts
        alerts_url = f"https://www.oref.org.il//Shared/Ajax/GetAlarmsHistory.aspx?lang=en&fromDate={day}&toDate={day}&mode=0&city_0={city_name}"
        alerts_data = requests.get(url=alerts_url).json()
        
        if alerts_data:
            print(f"city: {city_name}")
            alerts_data_list = []
            
            for row in alerts_data:            
                try:
                    name_he = row['NAME_HE']
                    name_en = row['NAME_EN']
                    name_ar = row['NAME_AR']
                    name_ru = row['NAME_RU']
                except:
                    name_he = name_ar = name_en = name_ru = ''

                alerts_row_data = [row['data'], row['date'], row['time'], row['alertDate'], row['category'], 
                            row['category_desc'], row['matrix_id'], row['rid'],
                            name_he, name_en, name_ar, name_ru]
            
                alerts_data_list.append(alerts_row_data)

            # insert cities data
            etl.insert_bulk(table=ALERTS_TABLE, truncate='n', data=alerts_data_list)

            