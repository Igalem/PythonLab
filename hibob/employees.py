import requests
import pandas as pd
from google.cloud import bigquery


API_KEY = "XXXXX"
API_URL = "https://api.hibob.com/v1/people"


client = bigquery.Client()
job_config = bigquery.QueryJobConfig()
job_config.use_query_cache = False

HEADERS = {"Authorization" : API_KEY,
           "Content-Type": "application/json"
           }

PARAMS = {
        "humanReadable": "true",
        "showInactive": "true"
    }


response = requests.get(API_URL, headers=HEADERS, params=PARAMS).json()


employees = response['employees']
#emp struc '''{'fullName': 'Aaron Aben Danan', 'lifecycle': {'custom': {'field_1661756019053': None, 'field_1649236050695': None, 'field_1645345338858': None, 'field_1643800909778': None}}, 'displayName': 'Aaron Aben Danan', 'personal': {'shortBirthDate': '02-07', 'pronouns': 'He / Him', 'communication': {'skypeUsername': None, 'slackUsername': '@aaben'}, 'custom': {'field_1653984843038': '232723893', 'field_1645012920403': 'אהרון אבן דנן'}, 'honorific': 'Mr', 'nationality': ['Israeli']}, 'creationDateTime': '2022-01-25T13:48:53.804919', 'employee': {'recentLeaveEndDate': None, 'veteranStatus': [], 'disabilityStatus': None, 'recentLeaveStartDate': None}, 'work': {'shortStartDate': '08-08', 'startDate': '2021-08-08', 'manager': '3239480338522570765', 'workPhone': None, 'tenureDuration': {'periodISO': 'P2Y7M18D', 'sortFactor': 948, 'humanize': '2 years, 7 months and 18 days'}, 'custom': {'field_1643116986890': '43'}, 'durationOfEmployment': {'periodISO': 'P2Y7M18D', 'sortFactor': 948, 'humanize': '2 years, 7 months and 18 days'}, 'reportsToIdInCompany': 144, 'employeeIdInCompany': 43, 'reportsTo': {'displayName': 'Benny Lifschitz', 'email': 'blifschitz@tango.me', 'surname': 'Lifschitz', 'firstName': 'Benny', 'id': '3239480338522570765'}, 'workMobile': None, 'indirectReports': 2, 'department': 'Revenue', 'siteId': 1956949, 'tenureDurationYears': 2.628, 'tenureYears': 3, 'customColumns': {'column_1643117142262': '2879918890144497715', 'column_1643117109214': None, 'column_1643117083783': '240471831', 'column_1644490082589': '215510126', 'column_1642587074361': '208071991', 'column_1674121820160': None, 'column_1644489912875': '3038612987645526785', 'column_1643117184778': '2749483652147577088'}, 'isManager': True, 'title': '255800564', 'site': 'Israel', 'originalStartDate': '2021-08-08', 'activeEffectiveDate': '2024-01-30', 'directReports': 2, 'secondLevelManager': '3038612987645526785', 'yearsOfService': 2.628, 'daysOfPreviousService': 0}, 'internal': {'periodSinceTermination': None, 'yearsSinceTermination': None, 'terminationReason': None, 'probationEndDate': None, 'currentActiveStatusStartDate': '2021-08-08', 'terminationDate': None, 'status': 'Active', 'terminationType': None, 'notice': None, 'lifecycleStatus': 'employed'}, 'secondName': None, 'workContact': {'custom': {'field_1643279751515': '2706202342', 'field_1650198591504': '2759000674371896182'}}, 'avatarUrl': 'https://media-process.hibob.com/image/upload/v1/hibob/avatar/509113/images/0ace973e-3725-462d-83f3-46e1d1aeac25?token=X19jbGRfdG9rZW5fXz1leHA9MTcxMjQ1MjMzNX5hY2w9KiUyZmhpYm9iJTJmYXZhdGFyJTJmNTA5MTEzJTJmaW1hZ2VzJTJmMGFjZTk3M2UtMzcyNS00NjJkLTgzZjMtNDZlMWQxYWVhYzI1Kn5obWFjPTYyOWFhMTk3NmQyM2NiY2FmNDZjYWQ0NzY1YTk2MzkwMDFlM2Q0OTRjYjM3NGYxOTFmOTAyMGJmZTdhNWZlNjg=&vendor=cloudinary', 'about': {'foodPreferences': ['224300275'], 'custom': {'field_1645609171364': '217407513', 'field_1646144777184': [], 'field_1657005963559': None, 'field_1675668965257': None}, 'socialData': {'linkedin': 'https://www.linkedin.com/in/aaron-ad/', 'twitter': None, 'facebook': None}, 'superpowers': [], 'hobbies': ['vv', 'running', 'coding', 'movies', 'learning'], 'about': None, 'avatar': 'https://media-process.hibob.com/image/upload/v1/hibob/avatar/509113/images/0ace973e-3725-462d-83f3-46e1d1aeac25?token=X19jbGRfdG9rZW5fXz1leHA9MTcxMjQ1MjMzNX5hY2w9KiUyZmhpYm9iJTJmYXZhdGFyJTJmNTA5MTEzJTJmaW1hZ2VzJTJmMGFjZTk3M2UtMzcyNS00NjJkLTgzZjMtNDZlMWQxYWVhYzI1Kn5obWFjPTYyOWFhMTk3NmQyM2NiY2FmNDZjYWQ0NzY1YTk2MzkwMDFlM2Q0OTRjYjM3NGYxOTFmOTAyMGJmZTdhNWZlNjg=&vendor=cloudinary'}, 'companyId': 509113, 'email': 'aaben@tango.me', 'surname': 'Aben Danan', 'home': {'mobilePhone': '(+972) 532207021', 'privateEmail': 'aarondanan@gmail.com', 'privatePhone': None}, 'coverImageUrl': 'https://media-process.hibob.com/image/upload/v1/hibob/public-image/cover/white_default_cover_image.png?token=X19jbGRfdG9rZW5fXz1leHA9MTczMDgwMTQ2NX5hY2w9KiUyZmhpYm9iJTJmcHVibGljLWltYWdlJTJmY292ZXIlMmZ3aGl0ZV9kZWZhdWx0X2NvdmVyX2ltYWdlLnBuZyp+aG1hYz0wMDQ5MGJkNjQ5N2Y2ZDQ5OGY5ZTVmZTMxYTYwNTI1NDA4ZTVhOGE4YWFmMWU3ZTZjYzVkNzlmNzVjODA3YTkw&vendor=cloudinary', 'id': '2759000689655939974', 'firstName': 'Aaron'}'''

employees_data = []
for emp in employees:
    employees_data.append({
        'displayName': emp.get('displayName'),
        'email': emp.get('email'),
        'title': emp.get('work', {}).get('title'),
        'department': emp.get('work', {}).get('department'),
        'startDate': emp.get('work', {}).get('startDate'),
        'site': emp.get('work', {}).get('site'),
        'manager_name': emp.get('work', {}).get('reportsTo'),
        'tango_id': emp.get('workContact', {}).get('custom', {}).get('field_1643279751515'),
        'team': emp.get('work', {}).get('customColumns', {}).get('column_1643117109214'),
        'internal_status': emp.get('internal', {}).get('status'),
        'life_cycle_status': emp.get('internal', {}).get('lifecycleStatus'),
        'id': emp.get('id'),        
    })

df = pd.DataFrame(employees_data)

table_id = "XXXXX.mrr.dim_employee"

job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("displayName", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("email", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("title", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("department", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("startDate", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("site", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("manager_name", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("tango_id", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("team", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("internal_status", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("life_cycle_status", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("id", bigquery.enums.SqlTypeNames.STRING)
        ],
        write_disposition="WRITE_TRUNCATE", )

job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
job.result()  # Wait for the job to complete.
table = client.get_table(table_id)  # Make an API request.




