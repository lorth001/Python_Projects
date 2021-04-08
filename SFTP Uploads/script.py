import pysftp
import keyring
import os
import io
import glob
import sys
from itertools import islice
from datetime import datetime, timedelta


username = "EXAMPLE_USERNAME"                               # username for Windows credential
cred_name = "EXAMPLE_CRED"                                  # Windows credential name
password = keyring.get_password(cred_name, username)        # get Windows credential password
hostname = "www.example.com"                                # host to connect to
cnopts = pysftp.CnOpts(knownhosts=None)                     # bypass 'hostkeys' error
cnopts.hostkeys = None                                      # bypass 'hostkeys' error


def process_files(directory_structure):
    files_in_folder = glob.glob("Y:\\FOLDER_NAME\\*")       # get LiveRamp files from local folder
    archive_folder = "Y:\\FOLDER_NAME\\SUB_FOLDER_NAME\\"   # get archive folder
    if len(files_in_folder) == 0:                           # if folder contains no files, then pass
        pass
    else:
        local_file_path = max(files_in_folder, key=os.path.getctime)    # file to upload to sftp
        filename = os.path.basename(local_file_path)                    # extract just the filename for the last uploaded file
        remote_dir = "dir_name_you_need_to_upload_to"                   # sftp directory to upload to
        remote_file_path = remote_dir+filename                          # path to upload file to in sftp
        sftp.put(local_file_path)                                       # upload file to SFTP
        print('Uploaded ' + filename + ' to '+ remote_dir + ' ... ')
        os.replace(local_file_path, archive_folder+filename)            # move the file to the archive folder


with pysftp.Connection(host=hostname, username=username, password=password, cnopts=cnopts) as sftp:
    print("Connection successful ... ")
    sftp.cwd('./uploads/')                                              # go to the 'uploads' directory
    with sftp.cd("dir_name_you_need_to_upload_to"):
        directory_structure = sftp.listdir_attr()
        process_files(directory_structure)                              
