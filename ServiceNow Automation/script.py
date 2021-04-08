import requests, json, datetime, getpass, re, pysnow, smtplib, ssl, keyring

srv ='servicenow_instance'
creds = keyring.get_password('instance.service-now.com', 'user_account')   # secure credential retrieval
s = pysnow.Client(instance=srv, user='user_account', password=creds, request_params={'sysparm_display_value': 'true'})
min_ago = 15

'''
--- SCRIPT LOGIC ---

1. Get the tickets that were updated in the last 15 minutes by "svc_account_usr" using the "sys_audit" table
2. Compare the "newvalue" and "oldvalue" to see if the "Select Include/Exclude" parts of the description have changed
    A. If the description has changed:
        I. Query the "task" table to see if all subtasks (i.e. "numbers") for the parent ticket are closed (i.e. the "closed_at" fields are not null)
            1. If all subtasks are closed:
                A. Duplicate the subtasks
            2. If not all subtasks are closed:
                A. Do nothing and exit loop
    B. If the select has not changed:
        I. Do nothing and exit loop
'''

# Function to detect if parts of a ticket's description have changed
def DescriptionChanged(newvalue, oldvalue, parent_sys_id):

  for newselect in re.findall(r'(?<=Select Include: ).*?(?=\\r)', newvalue):
    new_select_include = newselect
  for newselect in re.findall(r'(?<=Select Exclude: ).*?(?=\\r)', newvalue):
    new_select_exclude = newselect
  for oldselect in re.findall(r'(?<=Select Include: ).*?(?=\\r)', oldvalue):
    old_select_include = oldselect
  for oldselect in re.findall(r'(?<=Select Exclude: ).*?(?=\\r)', oldvalue):
    old_select_exclude = oldselect

  if new_select_include != old_select_include or new_select_exclude != old_select_exclude:
    description_changes = f'\nOld Select Include: {old_select_include} \
                       \nNew Select Include: {new_select_include} \
                       \nOld Select Exclude: {old_select_exclude} \
                       \nNew Select Exclude: {new_select_exclude}\n'
    return description_changes
  else:
    description_changes = ''
    return False


# Build query string to find all tickets that have had their description updated by 'svc_account_usr' in the past 15 min
parent_tkts = '^fieldname=description'
parent_tkts += '^user=svc_account_usr'
parent_tkts += '^tablename=sc_req_item'
parent_tkts += f'^sys_created_on>=javascript:gs.minutesAgoStart({min_ago})'
parent_tkts += '^ORDERBYsys_created_on'

tickets = s.query(table = 'sys_audit', query = parent_tkts)   # Build API request for the above query
new_subtasks = []

try:
  for ticket in tickets.get_multiple():
    parent_sys_id = ticket['documentkey']
    description_changes = DescriptionChanged(repr(ticket['newvalue']), repr(ticket['oldvalue']), parent_sys_id)  # check if description has changed
    if description_changes:
      subtasks = s.query(table = 'task', query = f'^parent={parent_sys_id}^sys_class_name=sc_task')   # build new query and API request to get subtask details
      allSubtasksClosed = False
      old_subtask = {}
      try:
        print('\n------------------------------')
        print(description_changes)
        for subtask in subtasks.get_multiple():   # for each subtask, check if it is closed
          state = subtask['state']
          print(subtask['parent']['display_value'])
          print(state + ': ' + subtask['number'])
          if state == 'Closed' or state == 'Closed Incomplete':   # if it is closed, duplicate its information
            allSubtasksClosed = True
            old_subtask = {
              'parent': subtask['parent'],
              'cmdb_ci': subtask['cmdb_ci'],
              'assigned_to': subtask['assigned_to'],
              'assignment_group': subtask['assignment_group'],
              'due_date': subtask['due_date'],
              'start_date': subtask['start_date'],
              'end_date': subtask['end_date'],
              'priority': subtask['priority'],
              'short_description': subtask['short_description'],
              'description': description_changes
            }
            new_subtasks.append(dict(old_subtask))
          elif state == 'Cancelled':
            allSubtasksClosed = True
          else:
            allSubtasksClosed = False
            break
      except Exception as error:
        print(error)

      if allSubtasksClosed == True:                   # duplicate subtasks
        print('\n***** ALL SUBTASKS CLOSED *****')
        for subtask in new_subtasks:
          print(json.dumps(subtask, indent=4, sort_keys=True))
          subtask.create(payload=subtask)
except Exception as error:
  print(error)