#!/opt/anaconda3/envs/mlearn/bin/python3
#!/usr/bin/python3 -u
'''
Clase que genera frames con valore aleatorios para testing.
Lee las magnitudes definidas para el datalogger de modo que los frames no den error al insertarlos en GDA
'''

import numpy as np
import urllib.request
import requests
from FUNCAUX.BD.spc_bd_gda import BD_GDA
from FUNCAUX.UTILS.spc_memblocks import MEMBLOCK
import re

class SENDFRAMES:

    def __init__(self):
        self.url = None
        self.response = None
        self.dlgid = 'DEFAULT'
        self.server = '127.0.0.1'
        self.script = 'spcomms.py'
        self.path = 'spcomms'
        self.port = 80
        self.fw_ver = '4.0.4b'
        self.verbose = True
        self.payload_template = ''
        self.payload = ''
        self.type = ''
        self.frame_list = None
        self.cgi_type = 'GET'

    def set_cgi_type(self, cgi_type):
        self.cgi_type = cgi_type

    def set_type(self, type):
        self.type = type

    def set_dlgid(self,dlgid):
        self.dlgid = dlgid

    def set_server(self,server):
        self.server = server

    def set_port(self, port):
        self.port = port

    def set_fw_ver(self, fw_ver):
        self.fw_ver = fw_ver

    def set_script(self, script):
        self.script = script

    def set_path(self, path):
        self.path = path

    def set_verbose(self, verbose):
        self.verbose = verbose

    def set_frame_list(self, frame_list):
        self.frame_list = frame_list

    def get_params(self):
        print('TYPE: {}'.format(self.type))
        print('CGI: {}'.format(self.cgi_type))
        print('DLGID = {}'.format(self.dlgid))
        print('SERVER = {}'.format(self.server))
        print('PORT = {}'.format(self.port))
        print('FW_VER: {}'.format(self.fw_ver))
        print('SCRIPT: {}'.format(self.script))
        print('VERBOSE: {}'.format(self.verbose))

    def prepare_random_payload(self):
        # Payload: Genero una al azar
        l_tags = ['PA', 'PB', 'Q0', 'H1', 'H2', 'SEN1', 'BMB0', 'BMB1', 'FS', 'CAU0', 'CAU1', 'IO0', 'IO1', 'IO2','AIN0', 'AIN1']
        nro_tags = np.random.randint(2, len(l_tags))
        tags = np.random.choice( l_tags, nro_tags, replace=False)
        for tag in tags:
            self.payload_template += '%s:REPLACE_VAL;' % tag
        self.payload_template += 'bt:12.01;'
        return self.payload_template

    def prepare_payload_template(self):
        '''
        Prepara un payload pero con nombres que el datalogger tenga configurado para que no de error al insertarlo
        en GDA.
        Solo genera el template
        '''
        gda = BD_GDA()
        d = gda.read_dlg_conf(self.dlgid)
        mag_names=[ d[(ch,tag)] for (ch,tag) in d if tag =='NAME' and d[(ch,tag)] not in ['bt','X'] ]
        if len(mag_names) == 0:
            print('ERROR: El dlgid {0} no tienen en la BDGDA magnitudes usables. EXIT !!'.format(self.dlgid))
            exit(1)

        for mag in mag_names:
            self.payload_template += '%s:REPLACE_VAL;' % mag
        self.payload_template += 'bt:12.01;'
        return self.payload_template

    def fill_payload(self):
        # EL payload es un template. Asigno los valores de las magnitudes.
        self.payload = self.payload_template
        # Rellena el template con valores aleatorios.
        while self.payload.find('REPLACE_VAL') > 0:
            new_val = '%0.3f' % (np.random.random() * 100)
            self.payload = self.payload.replace('REPLACE_VAL', new_val, 1)
        return self.payload

    def send_cgi_GET(self):
        # Header
        self.url = 'http://{0}:{1}/cgi-bin/{2}/{3}?ID:{4};TYPE:{5};VER:{6};'.format(self.server, self.port, self.path, self.script, self.dlgid, self.type, self.fw_ver)
        #
        # Calculo los valores instantaneos y transmito
        self.fill_payload()
        self.url += self.payload
        #
        if self.verbose:
            print('SENT: {0}'.format(self.url))
        # Envio
        try:
            req = urllib.request.Request(self.url)
        except:
            print('\nERROR al enviar frame {0}'.format(self.url))

        print('.',end='', flush=True)

        with urllib.request.urlopen(req) as response:
            self.response = response.read()

        if self.verbose:
            print('RESP: {0}'.format(self.response))
        return self.response

    def send_cgi_POST(self):
        # La primer parte es un GET
        self.url = 'http://{0}:{1}/cgi-bin/{2}/{3}?ID:{4};TYPE:{5};VER:{6};'.format(self.server, self.port, self.path, self.script, self.dlgid, self.type, self.fw_ver)
        if self.verbose:
            print('SENT GETpart: {0}'.format(self.url))
        # Envio
        # Los datos van en un POST
        if self.type == 'PLCR2':
            payload = b'd\x00\x00\x00\x00\x00\n\x14\x1e(2<F\xc8\x00,\x01e\x00\x00\x00\x00\x00\x0b\x15\x1f)3=G\xc9\x00-\x01f\x00\x00\x00\x00\x00\x0c\x16 *4>H\xca\x00.\x01g\x00\x00\x00J\x01\r\x17!+5?IS\x00\x00\x00\x00\xc8\xc8\xc8\xc8\xc8\xc8\xc8\xc8\xc8\xc8\xc8\xc8\xc8\xc8\xc8\xc8\xc9\xc8\xc8\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xc2G'
        else:
            payload = {'key1': 'value1', 'key2': 'value2'}
        r = requests.post( self.url , data=payload)

        # Response
        # La respuesta viene en un header HTML !!!
        if self.verbose:
            print('RESP: {0}'.format(r.text))

        if self.type == 'PLCR2':
            # <html><body><h1>b' \x00\x00\x00\xd7\xa3\xcaB\x02\x00\x00\x00\xecq\xb2Cf\xa6!F'</h1></body></html>
            # Decodifico ( fixed mode ) el frame recibido de PLCR2 para ver que ande todo bien
            # as per recommendation from @freylis, compile once only
            raw_html = r.text
            CLEANR = re.compile('<.*?>')
            cleantext = re.sub(CLEANR, '', raw_html)
            print('CLEANTEXT: {0}'.format(cleantext))
            #rx_payload = str.encode(cleantext)
            #print('RX_PAYLOAD: {0}'.format(rx_payload))
            #mbk = MEMBLOCK()
            #d_mbk = mbk.convert_bytes2dict(rx_payload)
            #print('D_MEMBLOCK: {0}'.format(d_mbk))

    def send(self):
        if self.verbose:
            print('\nFrame:')
        #
        if self.cgi_type == 'GET':
            self.send_cgi_GET()
        elif self.cgi_type == 'POST':
            self.send_cgi_POST()
        else:
            print('SENT ERROR: {0}'.format(self.cgi_type))
        #
        return