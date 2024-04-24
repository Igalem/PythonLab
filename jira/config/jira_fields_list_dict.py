jira_fields_list = {
    'created': None,
    'updated': None,
    'resolutiondate': None, ## Resolved
    'duedate': None, 
    'summary': None,
    'issuetype': 'name',
    'issuelinks': None, ## Linked Issues
    'status': 'statusCategory',
    'fixVersions': 'name', 
    'project': 'name',
    'reporter': 'displayName',
    'creator': 'displayName',
    'assignee': 'displayName',
    'priority': 'name',
    'labels': None,
    'comment': 'total',
    'watches': 'watchCount',
    'votes': 'votes',
    'timeestimate': None,
    'timetracking': 'remainingEstimate',
    'progress': 'total',
    'subtasks': 'key',
    'components': 'name',
    'customfield_10193': 'value',  ## Severity
    'customfield_10222': None,  ## Time to detect (h)
    'customfield_10205': None,  ## Duration (h)
    'customfield_10196': None,  ## Root Causes
    'customfield_10195': 'value',  ## Root Causes Category    
    'customfield_10221': 'value',  ## Feature Team
    'customfield_10184': 'value',  ## Team Location
    'customfield_10202': 'value',  ## High level Side
    'customfield_10090': 'value', ## Test Phase
    'customfield_10020': 'name',  ## Sprint
    'customfield_10026': None,  ## Story Points
    'assignee__email': 'emailAddress',
    'creator__email': 'emailAddress',
    'reporter__email': 'emailAddress',
    'customfield_10200': None

}

jira_mapping_fields = {
    'created': 'created',
    'updated': 'updated',
    'resolutiondate': 'resolved',
    'duedate': 'duedate', 
    'summary': 'summary',
    'issuetype': 'issue_type',
    'status': 'status_category',
    'issuelinks': 'linked_issues',
    'fixVersions': 'fix_versions',
    'project': 'project',
    'reporter': 'reporter',
    'creator': 'creator',
    'assignee': 'assignee',
    'priority': 'priority',
    'labels': 'labels',
    'comment': 'total_comments',
    'watches': 'total_watches',
    'votes': 'total_votes',
    'timeestimate': 'time_estimate',
    'timetracking': 'time_tracking',
    'progress': 'progress',
    'subtasks': 'subtasks',
    'components': 'components',
    'customfield_10193': 'severity',
    'customfield_10222': 'time_to_detect',
    'customfield_10205': 'duration', 
    'customfield_10196': 'root_causes',
    'customfield_10195': 'root_causes_category', 
    'customfield_10221': 'feature_team',
    'customfield_10184': 'team_location',
    'customfield_10202': 'high_level_side',
    'customfield_10090': 'test_phase',
    'customfield_10020': 'sprint',
    'customfield_10026': 'story_points',
    'creator__email': 'creator_email',    
    'assignee__email': 'assignee_email',
    'reporter__email': 'reporter_email',
    'customfield_10200': 'lost_money'
}


# def listToString(list_values):
#     return ','.join(list_values)


# def generate_jira_field_names():
#     jiraFieldNames = []
#     for i, jfield in enumerate(jira_fields_list):
#         try:
#             jiraFieldNames.append(f"${i + 2} as {jira_mapping_fields[jfield]}")
#         except:
#             raise Exception(F"Mapping is missing for Jira field name: [{jfield}]")
#             sys.exit(1)


#     return listToString(jiraFieldNames)