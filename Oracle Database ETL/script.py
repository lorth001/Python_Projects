import pysftp
import csv
import ast
import cx_Oracle
import keyring
import os
import io
import codecs
import shutil
import glob
from itertools import islice
from datetime import datetime, timedelta


# DEFINITION FOR INSERTING CSV CONTENTS TO ORACLE DATABASE
def insert(localfile, connection, delimiter=None):
    print('\nSyncing contents of the new CSV file to the Oracle database...')
    with codecs.open(localfile, 'r', encoding='cp1252', errors='ignore') as csvfile:    # open the file as csvfile
        if (delimiter is None):
            reader = csv.reader(csvfile)
        else:
            reader = csv.reader(csvfile, delimiter=delimiter)
        counter = 0
        for row in reader:
            if counter >= 6:                                                            # skip the header (first 6 lines) of the csv
                connection.execute("INSERT INTO ['''TABLE_NAME'''] (COLUMN_NAME_1,COLUMN_NAME_2,COLUMN_NAME_3,COLUMN_NAME_4,COLUMN_NAME_5,COLUMN_NAME_6,COLUMN_NAME_7,COLUMN_NAME_8,COLUMN_NAME_9,COLUMN_NAME_10,COLUMN_NAME_11) values (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11)", [
                    row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10]])
                if counter == 100:                                                      # commit records to database every 100 rows
                    c.execute('commit')
            counter = counter + 1
            c.execute('commit')                                                         # commit remaining records to database


# DEFINITION FOR GETTING CSV FROM SFTP AND MANIPULATING IT IN LOCAL FOLDER
def process_files(directory_structure, local_folder):
    # REMOVE OLD EXCEL FILE FROM LOCAL DIR
    print('\nRemoving the old CSV file from the local directory...')
    file_to_remove = local_folder+'*'
    r = glob.glob(file_to_remove)
    for i in r:
        shutil.rmtree(i)

    # GET LATEST FILE FROM SFTP SERVER
    file_list = []                                                                      # initialize empty array to hold the last modified times for files in SFTP
    for attr in directory_structure:
        file_list.append(attr.st_mtime)                                                 # add modified times for each file in SFTP to file_list array
    file_list.sort(reverse=True)                                                        # sort last modified times
    latest_time = file_list[0]                                                          # grab latest modified time

    for attr in directory_structure:
        if latest_time == attr.st_mtime:                                                # grab file that matches latest modified time
            target_file = attr.filename
            print('\nRetrieving:')
            print(target_file)
            localfile = local_folder+target_file
            sftp.get(target_file, localfile)
    return localfile


# DEFINITION FOR REMOVING CSV FILES FROM SFTP
def remove_files(directory_structure):
    print('\nRemoving files that are at least 5 days old from the SFTP server...')
    for attr in directory_structure:
        file_date = datetime.fromtimestamp(attr.st_mtime).date()
        cutoff_date = datetime.today().date() - timedelta(days=5)                       # remove csv files only if they are at least 5 days old
        if file_date < cutoff_date:
            sftp.remove(attr.filename)


# DEFINITION FOR MAKING SURE THE CSV HASN'T ALREADY BEEN IMPORTED TO THE DATABASE
def last_processed(connection, localfile):
    print('\nChecking to see if the CSV file has already been inserted into the database...')
    file_name = connection.execute("SELECT file_name FROM ['''TABLE_NAME'''] WHERE trunc(imported_date) = trunc(sysdate) AND rownum = 1")
    if file_name == localfile:
        print('\nFile contents have already been inserted into the database.  Exiting script...')
        sys.exit(0)


user = ['''USER_NAME''']                                                                # username for Windows credential
cred_name = ['''CRED_NAME''']                                                           # Windows credential name
pwd = keyring.get_password(cred_name, user)                                             # get Windows credential password
myHostname = ['''www.example_host_name.com''']                                          # sftp host to connect to
cnopts = pysftp.CnOpts(knownhosts=None)                                                 # bypass 'hostkeys' error
cnopts.hostkeys = None                                                                  # bypass 'hostkeys' error
local_folder = ['''PATH/TO/LOCAL/FOLDER/''']

with pysftp.Connection(host=myHostname, username=user, password=pwd, cnopts=cnopts) as sftp:
    sftp.cwd('./uploads/')                                                              # shows current dir
    directory_structure = sftp.listdir_attr()                                           # shows directory of SFTP
    print("\nConnection successful... ")                                                # connection message

    localfile = process_files(directory_structure, local_folder)                        # process_files

    user = os.environ.get("USERNAME")                                                   # Creating connetion to DB (opening the session)
    cred_name = ['''CRED_NAME''']
    pwd = keyring.get_password(cred_name, user)
    conn_str = user+'/'+pwd+u'@['DATABASE_NAME']'
    conn = cx_Oracle.connect(conn_str)
    c = conn.cursor()

    # REMOVE OLD FILES IN SFTP SERVER
    remove_files(directory_structure)

    # DETERMINE IF FILE HAS ALREADY BEEN INSERTED INTO DATABASE
    last_processed(c, localfile)

    # UPLOAD FILE CONTENTS TO ORACLE DATABASE
    insert(localfile, c)

    conn.close()

    print('\n********************\nPROCESS COMPLETE\n********************\n')
