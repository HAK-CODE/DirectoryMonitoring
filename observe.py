'''
author: HAK
time  : 10:00 PM, 28/10/2017
'''

import argparse
import re
from directoryInfo import PATH_INFO_PROVIDER
from Watch import Watcher


parser = argparse.ArgumentParser(prog='WATCHER',
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                 description='Watch directory with a path specified.')
parser.add_argument('--path'  , '-p', type=str, default='.', help="Specify full path to a directory.")
parser.add_argument('-s', action='store_true')
args = parser.parse_args()
DIRECTORY_NAME = re.sub('[\'\"]','',args.path)
DIR_INFO = PATH_INFO_PROVIDER(DIRECTORY_NAME)


if DIR_INFO.ISDIR() == True:
    if(args.s):
        print("Binding Server...")
        Watcher(DIR_INFO.DIRNAME(), True).run()
    else:
        Watcher(DIR_INFO.DIRNAME(), False).run()
else:
    print('Defined directory', DIRECTORY_NAME, "does not exist.")