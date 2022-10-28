#!/opt/anaconda3/envs/mlearn/bin/python3

'''
Script que envia en ráfaga todos los frames manejables por el SPY.py
Acepta argumentos ( ver spyplc_test_xframe.py -h )
'''

import time
from FUNCAUX.UTILS.spc_sendframes import SENDFRAMES
from FUNCAUX.UTILS.spc_log import config_logger

if __name__ == '__main__':

    config_logger('XFRAME')
    start = time.perf_counter()

    print('Testing SPYPLC....')
    sendframes = SENDFRAMES()

    #
    #for frame_type in ['SPX', 'SP5K', 'PLC', 'PLCPAY', 'OCEANUS', 'GENERICO']:
    for frame_type in ['PLCR2']:
        print('\n------------------------------------------')
        sendframes.set_type(frame_type)
        sendframes.set_dlgid('DLGTEST')
        sendframes.set_verbose(True)
        if frame_type in ['OCEANUS', 'GENERICO', 'PLCR2']:
            sendframes.set_cgi_type('POST')
        else:
            sendframes.set_cgi_type('GET')
        sendframes.prepare_random_payload()
        sendframes.send()
        # Summary:
        print('SUMMARY:')
        start = time.perf_counter()
        sendframes.get_params()
        elapsed = time.perf_counter() - start
        print('Total time = {0}.'.format(elapsed))

    print('\n------------------------------------------')
