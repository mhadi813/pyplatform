import os
import json
import logging
import re


def list_files_in_folder(folderpath="./", file_extenion="."):
    """Return filepath in a local folder and its subfolders, filters filepaths by file extension.

    Keyword Arguments:
        folderpath {str} -- relative or absolute path of directory (default: {"./"})
        file_extenion {str} -- e.g .py, .ipynb, .txt, .pdf etc (default: all filetype)

    Returns:
        list -- list of filepath matching the criteria
    """

    f = []

    if folderpath == "./":
        folderpath = os.path.curdir
        logging.debug("searching in current working dir and it's subfolders")
    else:
        logging.debug(f"searching in {folderpath} and it's subfolders")

    if file_extenion == ".":
        logging.debug("searching for all files")
    else:
        logging.debug(f"searching for files with {file_extenion} extension")
        file_extenion = file_extenion.lower()+"$"

    for (dirpath, dirnames, filenames) in os.walk(folderpath):
        for file in filenames:
            if re.search(r'{}'.format(file_extenion), file):
                f.append(os.path.join(dirpath, file))
    logging.debug(f"found {len(f)} files in")
    return f


def read_json_file(filepath):
    """ Reads json file

    Arguments:
        filepath {str} -- filepath

    Returns:
        dict
    """
    with open(filepath, mode='r') as file:
        content = json.loads(file.read())
    return content


def _update_json_file(filepath, newfilepath=None, **kwargs):
    """Update or create newfile json file from existing one.

    Arguments:
        filepath {str} -- json filepath to be updated

    Keyword Arguments:
        newfilepath {str} -- new filepath so as not modify original file (default: {overwrites original file})
        kwargs:
        key = value pairs or dict to be inserted or updated in the original file

    Returns:
        newfilepath {str} -- modified filepath
    """
    if not newfilepath:
        newfilepath = filepath
    with open(filepath, mode='r') as file:
        dict_content = json.loads(file.read())

    for key in kwargs:
        dict_content[key] = kwargs[key]

    with open(newfilepath, mode='w') as file:
        file.write(json.dumps(dict_content))

    return newfilepath


def _delete_temp_files():
    """Delete tempory files.
    """
    if os.path.isfile(temp_azure_cred):
        os.remove(temp_azure_cred)

    if os.path.isfile(temp_tableau_cred):
        os.remove(temp_tableau_cred)

    if os.path.isfile(temp_gcp_cred):
        os.remove(temp_gcp_cred)


def _insert_into_namespace(key, value, name_space=globals()):
    """Insert key, values to global namesapce

    Arguments:
        key {str} 
        value {any} 

    Keyword Arguments:
        name_space -- (default: {globals()})
    """
    name_space[key] = value
