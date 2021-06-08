#
# SCRIPT:      migrate-dremio-local-users.py
#
# DESCRIPTION: A python script that creates users in a new Dremio cluster from users referenced
#              in a Dremio Cloner backup file.
#
# USAGE:       Run this script BEFORE you run the Dremio Cloner PUT operation.
#                   $ python3 migrate-dremio-local-users.py
#
# FEEDBACK:    greg@dremio.com
#

# Imports
import json
import requests
import os, os.path, sys
import getpass

auth_header = ''
headers = {'content-type':'application/json'}

print('\nRunning migrate-dremio-local-users.py - a python script that creates users in a new Dremio cluster \nfrom users referenced in a Dremio Cloner backup file.\n')

# Prompt for required arguments

# Prompt for the new Dremio cluster's coordinator URL
dremio_server_url = input('Enter New Dremio Cluster\'s HTTP URL: [http://localhost:9047] ')
if dremio_server_url == '':
  dremio_server_url = 'http://localhost:9047'

# Prompt for the admin user id and password to use with the new cluster's REST API
admin_username = input('Enter New Dremio Cluster\'s Admin User ID: ')
admin_password = getpass.getpass(prompt='Enter Admin Password: ')

# Prompt for the name of the Dremio Cloner backup file
cloner_backup_file = input('Enter the name of the Dremio Cloner backup file: [dremio-cloner.backup] ')
if cloner_backup_file == '':
  cloner_backup_file = 'dremio-cloner.backup'

# login function
def login():
    loginData = {'userName': admin_username, 'password': admin_password}
    response = requests.post(dremio_server_url + '/apiv2/login', headers=headers, data=json.dumps(loginData), verify=False)

    if response.status_code == 200:
    
        print ('  Successfully authenticated.')
    
        data = json.loads(response.text)

        # retrieve the login token
        token = data['token']

        return {'content-type':'application/json', 'authorization':'_dremio{authToken}'.format(authToken=token)}
    else:
        if response.status_code == 401:
          print(' FATAL ERROR: Admin user authentication failed. Incorrect user id or password. ')
        else:
          print(' FATAL  ERROR: Admin user authentication failed. Error: ', str(response))

        sys.exit(-1)

    # end of function

# Check if user exists function
def user_exists(user_name):
    
    endpoint = dremio_server_url + '/api/v3/user/' + user_name

    response = requests.get( endpoint, headers=auth_header, verify=False)

    if response.status_code == 200:
        return (True)
    elif response.status_code == 404 or 'not found' in response.content:
        return (False)
    else:
      print(' FATAL ERROR making REST API call: ', endpoint)
      sys.exit(-1)    

    # end of function

# Create User Function
def create_user (user_name, first_name, last_name, email):
    
    endpoint = dremio_server_url + '/api/v3/user/' 

    default_user_password = "changeme123"

    data = ''' 
    { 
      "name": "%s", 
      "firstName": "%s", 
      "lastName": "%s", 
      "email": "%s", 
      "password": "%s" 
    }
    ''' % (user_name, first_name, last_name, email, default_user_password)

    response = requests.post( endpoint, headers=auth_header, data=data, verify=False)

    if response.status_code == 200:
      print('   Successfully created user \"' + user_name + "\"" )
    else:
      if 'already exists' in str(response.content):
        print('   User \"' + user_name + '\" already exists. ')
      else:
        print('   ERROR creating user \"' + user_name + '\". Error: ', response.content)

    # end of function

# Login to Dremio and get auth token
auth_header = login()

# Check if the Cloner backup file exists.
if not os.path.isfile(cloner_backup_file):
  print(' FATAL ERROR: Dremio Cloner backup file \"' + cloner_backup_file + '\" does not exist. ')
  sys.exit(-1)    

# Read the Cloner backup file and loop through the "referenced_users"
# For each referenced user, create the user on the new cluster

with open(cloner_backup_file) as file:
  dremio_backup_json = json.load(file)

# Dump backup data
#print("====================")
#print(json.dumps(dremio_backup_json, indent = 4, sort_keys=True))

for key, value  in dremio_backup_json.items():

  if key == 'data':
    json_data = value

    for json_item in json_data:

      for item_key in json_item:
        if item_key == 'referenced_users':

          for k2, users in json_item.items():

            for user_key in users:

              for (user_field_key, user_field_value) in user_key.items():

                if user_field_key == 'id':
                  user_id = user_field_value
                elif user_field_key == 'name':
                  user_name = user_field_value
                elif user_field_key == 'firstName':
                  user_firstName = user_field_value
                elif user_field_key == 'lastName':
                  user_lastName = user_field_value
                elif user_field_key == 'email':
                  user_email = user_field_value
                elif user_field_key == 'tag':
                  user_tag = user_field_value

              print("\n  Processing user: ", user_firstName, user_lastName, " - ", user_email)

              if user_exists(user_name): 
                print("    User " + user_name + " already exists in new cluster, skipping create user.")
              else:
                if user_name == admin_username:
                  print("   Skiping this admin user \"" + user_name + "\".")
                else:
                  create_user(user_name, user_firstName, user_lastName, user_email)


# end of script
