#!/home/pablo/Spymovil/python/pyenv/ml/bin/python3

import configparser
import os
import sys

current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
pparent = os.path.dirname(parent)
sys.path.append(pparent)

# Cuando se ejecuta el current dir es:
# /home/pablo/Spymovil/www/cgi-bin/spcommsV3/FUNCAUX/UTILS
# Le debemos indicar le PATH hacia el directorio config.
#
Config = configparser.ConfigParser()
Config.read( os.path.join(pparent, 'config/spcomms.conf'))

if __name__ == '__main__':
    print(f'CWD={os.getcwd()}')
    print(Config['REDIS']['host'])

