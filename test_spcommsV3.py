#!/home/pablo/Spymovil/www/cgi-bin/spcommsV3/venv/bin/python3

from urllib.parse import urlencode
from urllib.request import urlopen
import pprint
import datetime as dt
import argparse
import timeit
import sys
import time

#URL_BASE = 'http://localhost/cgi-bin/spcommsV3/spcomms.py'
URL_BASE = 'http://localhost/spcomms.py'

class TESTS:

    def __init__(self):
        self.params = {}
        self.url = ''
        self.callback_functions = { 'SPXR2': {
                                        'PING': self.spxR2_ping,
                                        'BASE': self.spxR2_base,
                                        'AINPUTS':self.spxR2_ainputs,
                                        'COUNTERS':self.spxR2_counters,
                                        'DATA':self.spxR2_data 
                                        }, 
                                    'SPXR3': {
                                        'PING': self.spxR3_ping,
                                        'BASE': self.spxR3_base,
                                        'AINPUTS':self.spxR3_ainputs,
                                        'COUNTERS':self.spxR3_counters,
                                        'MODBUS': self.spxR3_modbus,
                                        'DATA':self.spxR3_data 
                                        }           
                                    }

    def POST_process_test(self):
        '''
        La diferencia entre transmitir POST o GET es que el POST
        debe colocar los datos en el data= del urlopen en cambio en el GET
        todos los datos van en el URL.
        '''
        query_string = 'ID=PABLO&TYPE=PLCR2&VER=1.0.0;'
        self.url = URL_BASE + '?' + query_string
        print('Test PLCR2...')
        post_data = b'f\xe6\xf6Bdx\x00\xcd\xcc\x01B\x14(\x8dU'
        #
        print(f'URL={self.url}')
        print(f'POST_DATA={post_data}')
        #
        with urlopen(url=self.url, data=post_data) as response:
            response_text=response.read()
            print(f'POST_RESPONSE={response_text}')

    def GET_process_test(self):
        print('PARAMS=', end='')
        pprint.pprint(self.params)
        print(f'URL={self.url}')
        #
        with urlopen(self.url) as response:
            response_text=response.read()
            print(f'GET RESPONSE={response_text}')

    def spxR2_ping(self):
        query_string = 'ID:PABLO;TYPE:SPXR2;VER:1.0.0;CLASS:PING;'
        self.url = URL_BASE + '?' + query_string
        print('Test SPXR2 PING...')
        self.GET_process_test()

    def spxR2_base(self):
        query_string = 'ID:PABLO;TYPE:SPXR2;VER:1.0.0;CLASS:CONF_BASE;UID:42125128300065090117010400000000;HASH:0x11;'
        self.url = URL_BASE + '?' + query_string
        print('Test SPXR2 BASE...')
        self.GET_process_test()

    def spxR2_ainputs(self):
        query_string = 'ID:PABLO;TYPE:SPXR2;VER:1.0.0;CLASS:CONF_AINPUTS;HASH:0x01;'
        self.url = URL_BASE + '?' + query_string
        print('Test SPXR2 AINPUTS...')
        self.GET_process_test()

    def spxR2_counters(self):
        query_string = 'ID:PABLO;TYPE:SPXR2;VER:1.0.0;CLASS:CONF_COUNTERS;HASH:0x86;'
        self.url = URL_BASE + '?' + query_string
        print('Test SPXR2 COUNTERS...')
        self.GET_process_test()

    def spxR2_data(self):
        query_string = 'ID:PABLO;TYPE:SPXR2;VER:1.0.0;CLASS:DATA;A0:0.00;A1:0.00;A2:0.00;C0:0.000;C1:0.000;'
        self.url = URL_BASE + '?' + query_string
        print('Test SPXR2 DATA...')
        self.GET_process_test()

    def spxR3_ping(self):

        self.params = {
            'ID': 'PABLO',
            'TYPE': 'SPXR3',
            'VER': '1.0.0',
            'CLASS': 'PING',
        }
    
        query_string = urlencode(self.params)
        self.url = URL_BASE + '?' + query_string
        #
        print('Test SPXR3 PING...')
        self.GET_process_test()
        
    def spxR3_base(self):

        self.params = {
            'ID': 'PABLO',
            'TYPE': 'SPXR3',
            'VER': '1.0.0',
            'CLASS': 'CONF_BASE',
            'UID':'42125128300065090117010400000000',
            'HASH':'0x11'
        }
    
        query_string = urlencode(self.params)
        self.url = URL_BASE + '?' + query_string
        #
        print('Test SPXR3 BASE...')
        self.GET_process_test()

    def spxR3_ainputs(self):

        self.params = {
            'ID': 'PABLO',
            'TYPE': 'SPXR3',
            'VER': '1.0.0',
            'CLASS': 'CONF_AINPUTS',
            'HASH':'0x11'
        }
    
        query_string = urlencode(self.params)
        self.url = URL_BASE + '?' + query_string
        #
        print('Test SPXR3 AINPUTS...')
        self.GET_process_test()

    def spxR3_counters(self):

        self.params = {
            'ID': 'PABLO',
            'TYPE': 'SPXR3',
            'VER': '1.0.0',
            'CLASS': 'CONF_COUNTERS',
            'HASH':'0x11'
        }
    
        query_string = urlencode(self.params)
        self.url = URL_BASE + '?' + query_string
        #
        print('Test SPXR3 COUNTERS...')
        self.GET_process_test()

    def spxR3_modbus(self):

        self.params = {
            'ID': 'PABLO',
            'TYPE': 'SPXR3',
            'VER': '1.0.0',
            'CLASS': 'CONF_MODBUS',
            'HASH':'0x11'
        }
    
        query_string = urlencode(self.params)
        self.url = URL_BASE + '?' + query_string
        #
        print('Test SPXR3 MODBUS...')
        self.GET_process_test()

    def spxR3_data(self):

        now = dt.datetime.now()

        self.params = {
            'ID': 'PABLO',
            'TYPE': 'SPXR3',
            'VER': '1.0.0',
            'CLASS': 'DATA',
            'DATE':now.strftime('%y%m%d'),
            'TIME':now.strftime('%H%M%S'),
            'A0':0.00,
            'A1':0.00,
            'A2':0.00,
            'C0':0.000,
            'C1':0.000,
            'bt':12.496
        }
    
        query_string = urlencode(self.params)
        self.url = URL_BASE + '?' + query_string
        #
        print('Test SPXR3 DATA...')
        self.GET_process_test()

def process_arguments():
    '''
    Proceso la linea de comandos.
    d_vp es un diccionario donde guardamos todas las variables del programa
    Corrijo los parametros que sean necesarios
    '''

    parser = argparse.ArgumentParser(description='Prueba de frames al servidor SPCOMMS')
    parser.add_argument('-m,', '--method', dest='method', action='store', default='GET',
                        help='Metodo de envio de datos: GET,POST')
    parser.add_argument('-p', '--protocol', dest='protocol', action='store', default='SPXR3',
                        help='Protocolo de datos: SPXR2,SPXR3, ALL')
    parser.add_argument('-f', '--frame', dest='frame', action='store', default='PING',
                        help='Tipo de frame a enviar: PING,BASE,AINPUTS,COUNTERS,MODBUS,DATA,ALL')

    args = parser.parse_args()
    d_args = vars(args)
    return d_args

if __name__ == '__main__':

    # Diccionario con variables generales para intercambiar entre funciones
    test = TESTS()
    d_args = process_arguments()
    protocol = d_args['protocol']
    frame = d_args['frame']
    method = d_args['method']
    start_time = timeit.default_timer()

    if method == 'POST':
        test.POST_process_test()
        elapsed = timeit.default_timer() - start_time
        print(f'Method={method},elapsed time: {elapsed}')
        sys.exit(0)
    else:
        # GET Protocol
        if protocol == 'ALL' or frame == 'ALL':
            for kp in test.callback_functions.keys():
                for kf in test.callback_functions[kp]:
                    test.callback_functions[kp][kf]()
                    print()
                    time.sleep(2)
            sys.exit(0)
        #
        test.callback_functions[protocol.upper()][frame.upper()]()
        elapsed = timeit.default_timer() - start_time
        print(f'Method={method},Protocol={protocol},Frame={frame},elapsed time: {elapsed}')
