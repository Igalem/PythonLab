from simple_salesforce import Salesforce
# from pyspark.sql import SparkSession
# import pandas as pd


FIELD_LIST = [
    "LastActivityDate",
    "Opportunity_Owner_Formula__c",
    "Pricebook2Id",
    "Low_ACV_Approval_Details__c",
    "ForecastCategory",
    "X1LM_Forecast__c",
    "X2LM_Forecast__c",
    "Contract_in_hand__c",
    "Executive_Alignment__c",
    "Timeline_Alignment__c",
    "Risk_mitigation__c",
    "Commercials_Agreed_Upon__c",
    "Closed_Lost_No_Opp_Description__c",
    "Inside_Sales_Executive__c"
]

OBJECT_TYPE = 'Opportunity'

FIELD_TYPE_DOUBLE = ['double', 'currency', 'percent', ]

def get_salesforce_object_list(sf_instance):
    # Retrieve the list of all objects
    objects_list = sf_instance.describe()["sobjects"]

    # Extract object names and print them
    object_names = [obj["name"] for obj in objects_list]
    return object_names


def get_salesforce_field_api_name(sf_instance, object_type):
    error_exit = 0
    # Fetch object description
    describe = sf_instance.__getattr__(object_type).describe()

    # Find field by label
    print(f'Entity name: {object_type}\n')

    # print(describe['fields'], '\n')

    SF_FIELDS = {}
    for f in describe['fields']:
        f_type = f['type'].lower()
        if f_type in FIELD_TYPE_DOUBLE:
            field_type = 'double'
        elif (f_type == 'boolean' or f_type == 'checkbox'):
            field_type = 'boolean'
        elif (f_type == 'number'):
            field_type = f_type
        else:
            field_type = 'varchar'

        field_name = f['name']
        # print(field_name, field_name.lower())
        SF_FIELDS[field_name.lower()] = [f['label'], field_type, f['length']]

    fields_status = []
    field_status=()

    # print(SF_FIELDS)
    for field in FIELD_LIST:
        check_field = field.lower()
        if check_field in SF_FIELDS:
            field_name = SF_FIELDS.get(check_field)[0]
            field_api_type = SF_FIELDS.get(check_field)[1]
            field_api_size = SF_FIELDS.get(check_field)[2]

            field_status = (field_name, field, field_api_type, field_api_size)
        else:
            field_status = ('N/A', field)
            print(f'Error: N/A API field name: {field}')
            error_exit += 1

        fields_status.append(field_status)

    if error_exit>0:
        exit()

    return fields_status
    # return SF_FIELDS


def generate_alter_sql(field_list, table_name):
    for index, field in enumerate(field_list):
        field_name = field[1].lower()
        field_type = field[2].lower()
        size = field[3]

        if field_type == 'varchar' and size == 0:
            field_size = "(255)"
        elif field_type != 'varchar' and size == 0:
            field_size = ''
        else:
            size_rounded = -(-size // 10) * 10
            field_size = f"({size_rounded})"
        
        if index == 0:
            sql_alter_query = f"alter table {table_name} add {field_name} {field_type}{field_size}"
        else:
            sql_alter_query += f", {field_name} {field_type}{field_size}"
        
    print(sql_alter_query)


if __name__ == "__main__":
    # Salesforce credentials
    username = "xxxxx"
    password = "xxxxx"
    security_token = "xxxxxxxxxxxxxxxxxxxxx"

    ### Connect to Salesforce
    sf = Salesforce(username=username, password=password, security_token=security_token, domain='login')
    # print(get_salesforce_object_list(sf_instance=sf))

    ### get salesforce fields by object
    api_data = get_salesforce_field_api_name(sf, object_type=OBJECT_TYPE)
    # print(api_data)

    ## generate alter SQL
    table_name = 'DWH_PROD.SALESFORCE_OPPORTUNITY'
    generate_alter_sql(field_list=api_data, table_name=table_name)

    # spark = SparkSession.builder.appName('api').getOrCreate()
    # df = spark.createDataFrame(data=api_data, schema=['field name', 'api field name', 'field type', 'field size'])

    # df.show(100)

    # pdf = df.toPandas()
    # pdf.to_excel(f"/Users/igale/Downloads/{OBJECT_TYPE}.xlsx", index=False, engine='openpyxl')

    # # # # # Stop the Spark session
    # spark.stop()


