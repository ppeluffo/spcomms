#!/home/pablo/Spymovil/www/cgi-bin/spcommsV3/venv/bin/python3
'''
Implemento el procesamiento del protocolo SPXV2
Los PLC transmiten parte de sus datos en un GET y el bloque de memoria con las
variables con POST.
Se transmiten ida y vuelta bloques de memoria.
En la configuracion tenemos como armar y desarmar esos bloques, es decir que variables
representan, cuantos bytes ( protocolo orietado al byte) son cada variable y el nombre.

'''
import datetime as dt
from urllib.parse import parse_qs
import os
import sys
import random

current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
pparent = os.path.dirname(parent)
sys.path.append(pparent)

from FUNCAUX.UTILS.spc_log import log2
from FUNCAUX.UTILS import spc_memblocks
from FUNCAUX.SERVICIOS import servicios
from FUNCAUX.UTILS import spc_stats

class ProtocoloPLCR2:

    def __init__(self):
        self.dlgid = '00000'
        self.d_wrk = {}
        self.tag = random.randint(0,1000)
        self.mbk = spc_memblocks.Memblock()
        self.servicios = servicios.Servicios()
        self.d_local_conf = None
        self.d_response = {}

    def process_protocol(self, d_in:dict):
        '''
        d_in es el leido de la entrada: tiene 2 claves: GET y POST
        '''
        # Parseo el query string para obtener el ID y la VERSION
        query_string = d_in.get('GET',{}).get('QS', '')
        d_log = { 'MODULE':__name__, 'FUNCTION':'process', 'LEVEL':'INFO',  
                 'MSG':f'({self.tag}) QS={query_string}'}
        log2(d_log)
        #
        d_qs = parse_qs(query_string)
        #
        self.d_wrk['QUERY_STRING'] = query_string
        self.d_wrk['D_QS'] = d_qs
        #
        self.d_wrk['ID'] = d_qs.get('ID',['00000'])[0]
        self.d_wrk['UID'] = d_qs.get('UID',['0123456789'])[0]
        self.d_wrk['TYPE'] = d_qs.get('TYPE',['ERROR'])[0]
        self.d_wrk['VER'] =  d_qs.get('VER',['0.0.0'])[0]
        self.d_wrk['CLASS'] = d_qs.get('CLASS',['ERROR'])[0]
        self.d_wrk['RX_PAYLOAD'] =  d_in.get('POST',{})
        #
        self.__process__()
        #
        self.d_response['METHOD'] = 'POST'
        return self.d_response
        
    def __process__(self):
        #
        # Verificamos tener una configuracion local valida. Leemos la misma solicitandola al servicio
        self.dlgid = self.d_wrk['ID']
        endpoint = 'READ_CONFIG'
        params = { 'DLGID': self.dlgid }
        response = self.servicios.process(params=params, endpoint=endpoint)
        if response.status_code() == 200:
            # Tengo una configuracion valida
            self.d_local_conf = response.json().get('DCONFIG',{})
        else:
            # Salgo:
            self.d_response = {'DLGID':self.dlgid, 'RSP_PAYLOAD': 'ERROR:NO DLGID CONF','TAG':self.tag}
            log2 ({ 'MODULE':__name__, 'FUNCTION':'process', 'LEVEL':'INFO',
                   'DLGID':self.dlgid, 'MSG':f'({self.tag}) ERROR:NO DLGID CONF'})
            return
        #
        spc_stats.inc_count_frame_data()
        #
        # Leo de la configuracion la definicion del memblock y la cargo en el objeto mbk
        d_mbk = self.d_local_conf.get('MEMBLOCK',{})
        self.mbk.load_configuration(self.dlgid, d_mbk)
        #
        self.__process_reception__()
        #
        self.__process_response__()
        #

    def __process_reception__(self):    
        # RECEPCION
        # Cargo los datos (memblock) recibidos del PLC (post) al objeto mbk
        d_rx_payload = self.d_wrk.get('RX_PAYLOAD',b'')
        self.mbk.load_rx_payload(d_rx_payload)
        #
        # Proceso el bloque de datos recibidos de acuerdo a la definicion de RCVD MBK.
        if self.mbk.convert_rxbytes2dict():
            # Guardo los datos en las BD (redis y SQL)
            d_payload= self.mbk.get_d_rx_payload()
            # Agrego los campos DATE y TIME para normalizarlos al protocolo SPXV3
            # 'DATE': '230417', 'TIME': '161057'
            now = dt.datetime.now()
            d_payload['DATE'] = now.strftime('%y%m%d')
            d_payload['TIME'] = now.strftime('%H%M%S')
            log2 ({ 'MODULE':__name__, 'FUNCTION':'process', 'LEVEL':'SELECT',
                       'DLGID':self.dlgid, 'MSG':f'({self.tag}) RX:D_PAYLOAD={d_payload}'})
            endpoint = 'SAVE_DATA_LINE'
            params = { 'DLGID':self.dlgid, 'D_LINE':d_payload }
            response = self.servicios.process(params=params, endpoint=endpoint)
            if response.status_code() != 200:
                # ERROR No pude salvar los datos, pero igual sigo y le respondo
                self.d_response = {'DLGID':self.dlgid, 'RSP_PAYLOAD': 'ERROR: UNABLE TO SAVE DATA','TAG':self.tag}
                log2 ({ 'MODULE':__name__, 'FUNCTION':'process', 'LEVEL':'INFO',
                       'DLGID':self.dlgid, 'MSG':f'({self.tag}) ERROR: UNABLE TO SAVE DATA'})

    def __process_response__(self):
        # RESPUESTA
        # Armo la salida por medio de un diccionario que luego voy a convertir en un memblock
        # Primero veo cuales son los dlg_remtos y que variables de estos debo transmitir.
        d_responses = {}
        '''
        Leo el diccionario con las variables remotas ( dlgid: [ (var_name_remoto, var_name_local),  ] )
        var_name_remoto es el nombre de la variable en el equipo remoto (pA)
        var_name_local es el nombre definido en el mbk. ( ALTURA_TANQUE_KIYU )
        REMVARS = { 'KIYU001':[('HTQ1', 'ALTURA_TANQUE_KIYU_1'), ('HTQ2', 'ALTURA_TANQUE_KIYU_2')],
                    'SJOSE001' : [ ('PA', 'PRESION_ALTA_SJ1'), ('PB', 'PRESION_BAJA_SQ1')]
                  }
        dlgid: { [(Nombre_en_equipo_remoto, Nombre_en_equipo_destino ),...], }
        '''
        d_rem_vars = self.d_local_conf.get('REMVARS',{})
        log2 ({ 'MODULE':__name__, 'FUNCTION':'process', 'LEVEL':'SELECT','DLGID':self.dlgid, 
               'MSG':f'({self.tag}) d_rem_vars={d_rem_vars}'})
        for id in d_rem_vars:
            list_tvars = d_rem_vars[id]
            for var_name_remoto, var_name_destino in list_tvars:
                # Leo el valor correspondiente al equipo remoto
                endpoint = 'READ_VALUE'
                params = { 'DLGID': id, 'VAR_NAME':var_name_remoto }
                response = self.servicios.process(params=params, endpoint=endpoint)
                if response.status_code() == 200:
                    rem_value = response.json().get('VALUE', -99)
                    d_responses[var_name_destino] = rem_value
                else:
                     d_responses[var_name_destino] = -99
                #
                log2 ({ 'MODULE':__name__, 'FUNCTION':'process', 'LEVEL':'INFO',
                       'DLGID':self.dlgid, 'MSG':f'({self.tag}) read_remote_value={var_name_remoto}:{d_responses[var_name_destino]}'})
        #
        # Agrego una nueva variable TIMESTAMP que es el timestamp HHMM que sirve para que el PLC pueda tener
        # contol si el enlace cayo y los valores de las variables caducaron.
        now = dt.datetime.now()
        now_str = now.strftime("%H%M")
        d_responses['TIMESTAMP'] = int(now_str)
        #
        log2 ({ 'MODULE':__name__, 'FUNCTION':'process', 'LEVEL':'SELECT',
                       'DLGID':self.dlgid, 'MSG':f'({self.tag}) D_RESPONSES={d_responses}'})
        #
        # Leo las variables de la API del ATVISE y las agrego al diccionario de respuestas
        endpoint = 'READ_ATVISE'
        params = { 'DLGID': self.dlgid }
        response = self.servicios.process(params=params, endpoint=endpoint)
        if response.status_code() == 200:
            d_atvise_responses = response.json().get('D_ATVISE', {})
        else:
            d_atvise_responses = {}
        log2 ({ 'MODULE':__name__, 'FUNCTION':'process', 'LEVEL':'SELECT',
                       'DLGID':self.dlgid, 'MSG':f'({self.tag}) D_ATVISE_ORDERS={d_atvise_responses}'})
        #
        # Junto todas las variables de la respuesta en un solo diccionario
        d_responses.update(d_atvise_responses)
        log2 ({ 'MODULE':__name__, 'FUNCTION':'process', 'LEVEL':'SELECT',
                       'DLGID':self.dlgid, 'MSG':f'({self.tag}) D_ALL_RESPONSES={d_responses}'})
        #
        # Armo el bloque de respuestas a enviar apareando el d_responses con el mbk dando como resultado un rsp_dict.
        stream_payload = self.mbk.convert_dict2bytes( self.dlgid, d_responses)
        log2 ({ 'MODULE':__name__, 'FUNCTION':'process', 'LEVEL':'SELECT',
                       'DLGID':self.dlgid, 'MSG':f'({self.tag}) TX_STREAM_PAYLOAD={stream_payload}'})
        #
        self.d_response = {'DLGID':self.dlgid, 'TAG':self.tag, 'RSP_PAYLOAD': stream_payload}









    