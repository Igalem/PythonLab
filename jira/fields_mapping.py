from jira import JIRA
from datetime import datetime

# --- search for your requiered field in Jira -----------------
FIELD_SEARCH = 'Story Points'
# ------------------------------------------------------------

jira_domain = 'XXXXX'
jira_username = 'XXXXX'
jira_token = 'XXXXX'

options = {
    'server': jira_domain
}

jira = JIRA(
            jira_domain,
            basic_auth=(jira_username, jira_token)
            )

## retrive all jira fields
all_fields = jira.fields()

## get fields definithion
for field in all_fields:
    if field['name'] == FIELD_SEARCH:
        # print(field)
        jira_field_id = field['id']

ticket_no = 'DATA-20186'
ticket = jira.issue(ticket_no)
ticket_field_and_values = ticket.raw['fields']

print(f'Serching Jira field: {FIELD_SEARCH}')
print(f"Jira field id = {jira_field_id}")
print("\nField definition:")
print(ticket_field_and_values.get(jira_field_id))

