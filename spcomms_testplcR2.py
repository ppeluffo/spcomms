#!/opt/anaconda3/envs/mlearn/bin/python3

'''
Script que envia en ráfaga todos los frames manejables por el SPY.py
Acepta argumentos ( ver spyplc_test_xframe.py -h )
'''

import time
from FUNCAUX.UTILS.spc_sendframes import SENDFRAMES
# from FUNCAUX.UTILS.spc_log import config_logger

if __name__ == '__main__':

    # config_logger('XFRAME')
    start = time.perf_counter()

    print('Testing SPYPLCR2....')
    sendframes = SENDFRAMES()
    sendframes.set_type('PLCR2')
    #sendframes.set_dlgid('PPOTKIYU')
    sendframes.set_dlgid('PLCTEST')
    sendframes.set_cgi_type('POST')
    #sendframes.set_server('192.168.0.9')
    #sendframes.set_path('COMMS')
    #sendframes.prepare_random_payload()
    sendframes.send()
    # Summary:
    print('SUMMARY:')
    start = time.perf_counter()
    sendframes.get_params()
    elapsed = time.perf_counter() - start
    print('Total time = {0}.'.format(elapsed))
    print('\n------------------------------------------')
