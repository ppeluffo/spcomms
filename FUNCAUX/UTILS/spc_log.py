#!/home/pablo/Spymovil/www/cgi-bin/spcommsV3/venv/bin/python3
"""
Created on Wed Aug 14 09:19:01 2019

@author: pablo
"""

import logging
import logging.handlers
from datetime import datetime

#from FUNCAUX.UTILS.spc_config import Config
#
# from spc_config import Config

# Variable global que indica donde logueo.
# La configuro al inciar los programas

#syslogmode = 'SYSLOG','CONSOLA'
global SYSLOG_MODE
SYSLOG_MODE = 'SYSLOG'

global DEBUG_DLGID
DEBUG_DLGID = '00000'

# https://stackoverflow.com/questions/11029717/how-do-i-disable-log-messages-from-the-requests-library
urllib3_logger = logging.getLogger('urllib3')
urllib3_logger.setLevel(logging.CRITICAL)

def set_debug_dlgid(debug_dlgid='00000'):
    global DEBUG_DLGID
    DEBUG_DLGID = debug_dlgid

def config_logger( modo='SYSLOG'):
    # logging.basicConfig(filename='log1.log', filemode='a', format='%(asctime)s %(name)s %(levelname)s %(message)s', level = logging.DEBUG, datefmt = '%d/%m/%Y %H:%M:%S' )

    global SYSLOG_MODE
    SYSLOG_MODE = modo
 
    logging.basicConfig(level=logging.DEBUG)

    # formatter = logging.Formatter('SPX %(asctime)s  [%(levelname)s] [%(name)s] %(message)s', datefmt='%Y-%m-%d %T')
    # formatter = logging.Formatter('SPX [%(levelname)s] [%(name)s] %(message)s')
    formatter = logging.Formatter('SPCOMMSV3 [%(levelname)s] [%(name)s] %(message)s')
    handler = logging.handlers.SysLogHandler('/dev/log')
    handler.setFormatter(formatter)
    # Creo un logger derivado del root para usarlo en los modulos
    logger1 = logging.getLogger()
    # Le leo el handler de consola para deshabilitarlo
    lhStdout = logger1.handlers[0]  # stdout is the only handler initially
    logger1.removeHandler(lhStdout)
    # y le agrego el handler del syslog.
    logger1.addHandler(handler)
    # Creo ahora un logger child local.
    LOG = logging.getLogger('spy')
    LOG.addHandler(handler)
    #
 
def log2(d_log):
    '''
    Se encarga de mandar la logfile el mensaje.
    Si el level es SELECT, dependiendo del dlgid se muestra o no
    Si console es ON se hace un print del mensaje
    El flush al final de print es necesario para acelerar. !!!
    '''
    module = d_log.get('MODULE','NO_MODULE')
    function = d_log.get('FUNCTION', 'NO_FUNTION')
    dlgid = d_log.get('DLGID','00000')
    level = d_log.get('LEVEL','ERROR')
    msg = d_log.get('MSG','NO_MSG')
        
    global DEBUG_DLGID
    global SYSLOG_MODE

    #if syslogmode != 'SYSLOG':
    #    print('SPY.py TEST[level={0}, debug_level={1}, dlgid={2}, debug_dlgid={3}'.format(level, debug_level, dlgid, debug_dlgid))

    #print(f'DEBUG_DLGID: {DEBUG_DLGID}')
    #print(f'LEVEL:{level}')
    #print(f'dlgid:{dlgid}')

    if level in ['INFO','ERROR']:
        # Logueo siempre:
        if SYSLOG_MODE == 'SYSLOG':
            #logging.info(f'SPCOMMS [{dlgid}][{module}][{function}]:[{msg}]')
            logging.info(f'[{dlgid}][{msg}]')
        else:
            print('{0}:{1}::[{2}][{3}][{4}]:[{5}]'.format( datetime.now(), SYSLOG_MODE, dlgid, module, function, msg), flush=True)
        return
    
    if ( level == 'SELECT') and ( dlgid == DEBUG_DLGID):
        if SYSLOG_MODE == 'SYSLOG':
            logging.info('[{0}][{1}][{2}]:[{3}]'.format( dlgid,module,function,msg))
        else:
            print('{0}:{1}::[{2}][{3}][{4}]:[{5}]'.format( datetime.now(), SYSLOG_MODE, dlgid, module, function, msg), flush=True)
        return
    #
    if (dlgid == '00000'):
        if SYSLOG_MODE == 'SYSLOG':
            #logging.info(f'SPCOMMS [{dlgid}][{module}][{function}]:[{msg}]')
            logging.info(f'[{dlgid}][{msg}]')
        else:
            print('{0}:{1}::[{2}][{3}][{4}]:[{5}]'.format( datetime.now(), SYSLOG_MODE, dlgid, module, function, msg), flush=True)
        return
    #
    return

if __name__ == '__main__':
    #from spy import Config
    #list_dlg = ast.literal_eval(Config['SELECT']['list_dlg'])
    pass
