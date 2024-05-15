from jira import JIRA 
import pandas as pd
from google.cloud import bigquery

#---------------------------------------------------------------------
# Update data release and jira authentication parameters

data_release = 'Release 5.0'
email = "<email>"
api_token = "<api-token>" # https://id.atlassian.com/manage-profile/security/api-tokens

#---------------------------------------------------------------------

# instantiate JIRA client
server = "https://sagebionetworks.jira.com/"
jira = JIRA(basic_auth = (email, api_token), 
  options={'server': server})

# instantiate BigQuery client
client = bigquery.Client()

# pull in error lists from bigquery
errors = client.query("""
  SELECT * FROM `htan-dcc.data_release.errors`
""").result().to_dataframe()
cb_errors = client.query("""
  SELECT * FROM `htan-dcc.data_release.clin_bio_errors`
""").result().to_dataframe()

# group errors by manifest
group_errors = errors.groupby(['HTAN_Center', 'Component', 'Manifest_Id'])


def create_jira_request(title, description):
  data = {
    "serviceDeskId": "1",
    "requestTypeId": "Other questions",
    "requestFieldValues": {
      "summary": title,
      "description": description
    }
  }

  new_ticket = jira.create_customer_request(fields = data)
  return new_ticket


all_tickets = {}

for group, df in group_errors:
  #print(group[0], group[1], group[2])
  df = df.drop(columns = {
    'Manifest_Id','Manifest_Version','Id','HTAN_Center','Component'})
  title = "[%s Errors] %s %s %s" % (data_release, group[0], group[1], group[2])

  # convert dataframe to markdown
  table = df.to_markdown(index=False,tablefmt='pipe')
  table = table.split('\n')
  table.pop(1)  
  table = '\n'.join(line.replace('|:', '|') for line in table)

  # 32,767 character limit for descriptions and comments
  # attach table as csv if character limit exceeded
  if len(table) > 32000:
    description = "Errors pertaining to manifest %s attached as csv" % (group[2])
    res = create_jira_request(title, description)
    csv_path = './tmp/release5_%s_%s.csv' % (group[0].replace(' ',''),group[2])
    df.to_csv(csv_path, index = False)
    jira.add_attachment(issue = res.key, attachment = csv_path)
  
  else:
    description = "Errors pertaining to manifest %s \n %s" % (group[2], table)
    res = create_jira_request(title, description)
  
  # group tickets by center to create master ticket 
  if group[0] in all_tickets:
    all_tickets[group[0]]['issues'] = all_tickets[group[0]]['issues'].append(res.key)
  
  else:
    all_tickets[group[0]] = {"issues": [res.key]}


# create master issues grouped by center
for key,value in all_tickets.items():

  title = '[%s] %s Master Ticket' % (data_release, key)
  description = 'Master issue to manage %s errors from %s pre-release checks' % (
    key, data_release)

  res = create_jira_request(title, description)

  for v in value['issues']:
    jira.create_issue_link(type = "split to", inwardIssue = res.key, outwardIssue = v)


# create tickets for clinical/biospecimen validation
cb_errors = cb_errors.groupby('HTAN Center')

for group, df in cb_errors:
  title = "[HTAN Biospecimen/Participant ID Validation] %s" % group
  
  # convert dataframe to markdown
  table = df.to_markdown(index=False,tablefmt='pipe')
  table = table.split('\n')
  table.pop(1)  
  table = '\n'.join(line.replace('|:', '|') for line in table)
  
  description = "The table below contains biospecimen and participant IDs that were identified in multiple rows within %s's Biospecimen and/or Demographics manifests respectively \n %s" % (group, table)
  res = create_jira_request(title, description)
