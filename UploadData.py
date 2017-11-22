from SCHEMAS.SCHEMA import SCHEMA_PROVIDER

import sys
import time



#df_list=sys.argv[1:sys.argv.index(']')+1]
if sys.argv[1] == 'SENSOR':
    print('Sensor')
elif sys.argv[1] == 'INVERTER':
    print('Inverter')
else:
    print('Not valid')
    time.sleep(5)
    sys.exit(0)

'''
SCHEMA = SCHEMA_PROVIDER(True)
DICTIONARY = SCHEMA.GET_SCHEMA()

if SCHEMA.VALIDATE(DICTIONARY):
    print('valid')
    print(DICTIONARY)
else:
    print('error')
'''