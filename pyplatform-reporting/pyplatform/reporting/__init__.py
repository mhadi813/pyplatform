
from pkg_resources import get_distribution

__version__ = get_distribution("pyplatform-reporting").version

from .reporting import tableau_server_list_resources, tableau_server_get_credentials, tableau_server_upload_hyper, tableau_server_download_hyper, df_to_hyper, hyper_to_df, unzip_tdsx


def show_me(module=None, docstring=False):
    """
    Print functions contained in a module or script.

    Keyword Arguments:
        module {str,os.path,types.ModuleType} -- module, submodule or scriptpath (default=None)
        doctring {bool} -- if true, prints doctring of functions as well (default=False)

    Example:
    import pyplatform.common as pyp
    pyp.show_me() # prints functions names contained in pyplatfrom

    from pyplatform.common import udf
    pyp.show_me(udf, docstring=True) # prints functions names contained in udf submodule with docstring

    pyp.show_me('path_to_script/main.py') #prints funcstion contained in main.py

    """
    import re
    import os
    from types import ModuleType

    module_pattern = re.compile(r'^([a-z0-9]*.py)')
    functions = re.compile('^def[^__]{2}.*')
    all_content = re.compile('^def[^__]{2}.*|^[\s]*\"\"\"[\s]*.{1}')

    if not module:
        module_dir = os.path.dirname(__file__)
        script_path = [os.path.join(module_dir, script)
                       for script in os.listdir(module_dir) if re.search(module_pattern, script)]

    elif isinstance(module, ModuleType):
        if re.search(r'(namespace)', repr(module)):
            module_dir = module.__path__._path[0]
            script_path = [os.path.join(path, name) for path, subdirs, files in os.walk(
                module_dir) for name in files if re.search(r'[^__].py$', name)]

        else:
            if os.path.basename(module.__file__) != '__init__.py':
                script_path = [module.__file__]
            else:
                module_dir = os.path.dirname(module.__file__)
                script_path = [os.path.join(module_dir, script)
                               for script in os.listdir(module_dir) if re.search(module_pattern, script)]

    elif os.path.isfile(module):
        script_path = [module]

    for file_path in script_path:
        print('\n')

        print("***", "==="*5, '      ', os.path.basename(file_path).split('.')[0],
              '       ', "==="*5, "***", '\n')
        if docstring:
            print(f'{os.path.basename(file_path)} at {file_path} contains: \n')
        lines = None
        with open(file_path, mode='r') as file:
            lines = file.readlines()

        for line in lines:
            if docstring:
                if re.search(all_content, line):
                    print(line)
            else:
                if re.search(functions, line):
                    print(line)
        print('\n')
