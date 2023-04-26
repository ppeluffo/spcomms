#!/home/pablo/Spymovil/www/cgi-bin/spcommsV3/venv/bin/python3
'''
Implemento el procesamiento del protocolo SPXV3
Los frames que pueden llegar son:

 PING:          ID=PABLO&TYPE=SPXR3&VER=1.0.0&CLASS=PING
 RECOVER:       ID=DEFAULT&TYPE=SPXR3&VER=1.0.0&CLASS=RECOVER&UID=42125128300065090117010400000000
 CONF_BASE:     ID=PABLO&TYPE=SPXR3&VER=1.0.0&CLASS=CONF_BASE&UID=42125128300065090117010400000000&HASH=0x11
 CONF_AINPUTS:  ID=PABLO&TYPE=SPXR3&VER=1.0.0&CLASS=CONF_AINPUTS&HASH=0x01           
 CONF_COUNTERS: ID=PABLO&TYPE=SPXR3&VER=1.0.0&CLASS=CONF_COUNTERS&HASH=0x86
 CONF_MODBUS:   ID=PABLO&TYPE=SPXR3&VER=1.0.0&CLASS=CONF_MODBUS&HASH=0x86
 DATA:          ID=PABLO&TYPE=SPXR3&VER=1.0.0&CLASS=DATA&DATE=230321&TIME=094504&A0=0.00&A1=0.00&A2=0.00&C0=0.000&C1=0.000&bt=12.496

 Todos utilizan GET.

 Recibe un diccionario con el formato:
 d{ 'GET':{ QS', 'SIZE'}, 
       'POST': {'STREAM', 'BYTES', 'SIZE' } 
    }

self.d_wrk = {
    'CLASS': 'CONF_BASE',
    'D_QS': {'CLASS': ['CONF_BASE'], 'HASH': ['0x11'], 'ID': ['PABLO'], 'TYPE': ['SPXR3'],
                'UID': ['42125128300065090117010400000000'], 'VER': ['1.0.0']},
    'ID': 'PABLO',
    'QUERY_STRING': 'ID=PABLO&TYPE=SPXR3&VER=1.0.0&CLASS=CONF_BASE&UID=42125128300065090117010400000000&HASH=0x11',
    'TYPE': 'SPXR3',
    'VER': '1.0.0'
}

'''
import datetime as dt
import os
import sys
import random
from urllib.parse import parse_qs

current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
pparent = os.path.dirname(parent)
sys.path.append(pparent)

from FUNCAUX.SERVICIOS import servicios
from FUNCAUX.UTILS.spc_utils import u_hash, version2int, normalize_querystring, translate_rsp_payload, normalize_frame_data
from FUNCAUX.UTILS.spc_log import log2, config_logger, set_debug_dlgid
from FUNCAUX.UTILS import spc_stats

class ProtocoloSPXR3:
    '''
    Funciones que atienden al protocolo SPXR3.
    Todas retornan un diccionario del tipo dict('DLGID':, 'RSP_PAYLOAD': )

    self.d_wrk = {
        'CLASS': 'CONF_BASE',
        'D_QS': {'CLASS': ['CONF_BASE'], 'HASH': ['0x11'], 'ID': ['PABLO'], 'TYPE': ['SPXR3'],
                'UID': ['42125128300065090117010400000000'], 'VER': ['1.0.0']},
        'ID': 'PABLO',
        'QUERY_STRING': 'ID=PABLO&TYPE=SPXR3&VER=1.0.0&CLASS=CONF_BASE&UID=42125128300065090117010400000000&HASH=0x11',
        'TYPE': 'SPXR3',
        'VER': '1.0.0'
    }
    '''
    def __init__(self):
        self.d_wrk = {}
        self.d_response = {}
        self.response = ''
        self.d_local_conf = None
        self.servicios = servicios.Servicios()
        self.callback_functions =  { 'PING': self.__process_frame_ping__,
                                     'RECOVER': self.__process_frame_recover__,
                                     'CONF_BASE': self.__process_frame_config_base__,
                                     'CONF_AINPUTS': self.__process_frame_config_ainputs__,
                                     'CONF_COUNTERS': self.__process_frame_config_counters__,
                                     'CONF_MODBUS': self.__process_frame_config_modbus__,
                                     'DATA': self.__process_frame_data__
                                    }
        self.tag = random.randint(0,1000)

    def process_protocol(self, d_in:dict):
        '''
        Normaliza el query string si es necesario
        Procesa el protocolo con datos normalizados
        Des-normaliza el payload para adecuarlo al protocolo
        '''
        # Primero parseamos el query_string para obtener todos los campos.
        # El protocolo SPXR3 solo maneja datos por GET.
        query_string = d_in.get('GET',{}).get('QS', '')
        d_log = { 'MODULE':__name__, 'FUNCTION':'process', 'LEVEL':'INFO',  
                 'MSG':f'({self.tag}) QS={query_string}'}
        log2(d_log)
        #
        # Se usa para normalizar protocolos anteriores a SPXR3
        query_string = normalize_querystring(query_string)
        d_qs = parse_qs(query_string)
        #
        # Armamos el dict de trabajo.
        self.d_wrk['QUERY_STRING'] = query_string
        self.d_wrk['D_QS'] = d_qs
        #
        self.d_wrk['ID'] = d_qs.get('ID',['00000'])[0]
        self.d_wrk['UID'] = d_qs.get('UID',['0123456789'])[0]
        self.d_wrk['TYPE'] = d_qs.get('TYPE',['ERROR'])[0]
        self.d_wrk['VER'] =  d_qs.get('VER',['0.0.0'])[0]
        self.d_wrk['CLASS'] = d_qs.get('CLASS',['ERROR'])[0]
        #
        self.__process__()
        #
        # Se usa para convertir las respuestas de SPXR3 a protocolos anteriores.
        protocol = self.d_wrk['TYPE']
        self.d_response['RSP_PAYLOAD'] = translate_rsp_payload(protocol, self.d_response['RSP_PAYLOAD'])
        self.d_response['RSP_PAYLOAD'] = bytes(self.d_response['RSP_PAYLOAD'], encoding="UTF-8")
        self.d_response['METHOD'] = 'GET'
        #
        return self.d_response

     # PROCESS -------------------------------------------------------------

    def __process__(self):
        '''
        Metodo que procesa todos los frames del protocolo SPXR3.
        Prepara el diccionario de entrada y luego utiliza un selector
        de funciones para procesar cada clase de request.
        SALIDA: d_output.
        '''
        #
        dlgid = self.d_wrk['ID']
        clase = self.d_wrk['CLASS']
        version = self.d_wrk['VER']
        #
        log2 ({ 'MODULE':__name__, 'FUNCTION':'process', 'LEVEL':'SELECT',
                 'DLGID':dlgid, 'MSG':f'({self.tag}) D_WRK={self.d_wrk}'} )
        #
        log2 ({ 'MODULE':__name__, 'FUNCTION':'process', 'LEVEL':'INFO',
                 'DLGID':dlgid, 'MSG':f'({self.tag}) CLASS={clase},DLGID={dlgid},VERSION={version}'})
        #
        # Verificamos tener una configuracion local valida. Leemos la misma solicitandola al servicio
        endpoint = 'READ_CONFIG'
        params = { 'DLGID': dlgid }
        response = self.servicios.process(params=params, endpoint=endpoint)
        if response.status_code() == 200:
            # Tengo una configuracion valida
            self.d_local_conf = response.json().get('D_CONFIG',{})
        else:
            # Salgo:
            self.d_response = {'DLGID':dlgid, 'RSP_PAYLOAD': 'ERROR:NO DLGID CONF','TAG':self.tag}
            log2 ({ 'MODULE':__name__, 'FUNCTION':'process', 'LEVEL':'INFO',
                   'DLGID':dlgid, 'MSG':f'({self.tag}) ERROR:NO DLGID CONF'})
            return
        #
        # Proceso el REQUEST: lo define la clase: DATA, RECOVER, CONF_nnn, etc     
        if clase in self.callback_functions:
            self.d_response = self.callback_functions[clase]()
        else:
            # Frame no reconocido
            self.d_response = {'DLGID':dlgid, 'RSP_PAYLOAD':'ERROR:UNKNOWN FRAME TYPE','TAG':self.tag}
            log2 ({ 'MODULE':__name__, 'FUNCTION':'process', 'LEVEL':'INFO',
                   'DLGID':dlgid, 'MSG':f'({self.tag}) ERROR:UNKNOWN FRAME TYPE'})
        #
        return

    def __process_frame_ping__(self)->dict:
        '''
        Funcion usada para indicar que el enlace esta activo
        El QS del frame de PING es: ID=PABLO&TYPE=SPXR3&VER=1.0.0&CLASS=PING

        Testing:
        GET /cgi-bin/spcommsV3/spcommsV3.py?ID=PABLO&TYPE=SPXR3&VER=1.0.0&CLASS=PING
        HTTP/1.1
        Host: www.spymovil.com
        '''
        dlgid = self.d_wrk.get('ID','00000')
        return {'DLGID':dlgid,'RSP_PAYLOAD': 'CLASS=PONG','TAG':self.tag }
        
    def __process_frame_recover__(self):
        '''
        Funcion usada para configurar los parametros base.
        El QS es: ID=DEFAULT&TYPE=SPXR3&VER=1.0.0&CLASS=RECOVER&UID=42125128300065090117010400000000

        Testing:
        GET /cgi-bin/spcommsV3/spcommsV3.py?ID=DEFAULT&TYPE=SPXR3&VER=1.0.0&CLASS=RECOVER&UID=42125128300065090117010400000000
        HTTP/1.1
        Host: www.spymovil.com

        '''
        uid = self.d_wrk.get('UID','00000')
        #
        endpoint = 'READ_DLGID_FROM_UID'
        params = { 'UID': uid }
        response = self.servicios.process(params=params, endpoint=endpoint)
        if response.status_code() == 200:
            new_dlgid = response.json().get('DLGID','00000')
            if new_dlgid != '00000':
                self.d_response = {'DLGID':new_dlgid,'RSP_PAYLOAD':f'CLASS=RECOVER&DLGID={new_dlgid}' }
                return
        #
        # ERROR:
        self.d_response = {'DLGID':'00000','RSP_PAYLOAD':'CLASS=RECOVER&CONFIG=ERROR' }
        #
            
    def __process_frame_config_base__(self):
        '''
        Funcion usada para configurar los parametros base.
        El QS es: ID=PABLO&TYPE=SPXR3&VER=1.0.0&CLASS=CONF_BASE&UID=42125128300065090117010400000000&HASH=0x11

        Testing:
        GET /cgi-bin/spcommsV3/spcommsV3.py?ID=PABLO&TYPE=SPXR3&VER=1.0.0&CLASS=CONF_BASE&UID=42125128300065090117010400000000&HASH=0x11
        HTTP/1.1
        Host: www.spymovil.com
        '''
        spc_stats.inc_count_frame_config_base()

        dlgid = self.d_wrk.get('ID','00000')
        uid = self.d_wrk.get('UID','00000')

        if dlgid == 'DEFAULT':
            return {'DLGID':dlgid,'RSP_PAYLOAD': 'CLASS=CONF_BASE&CONFIG=ERROR','TAG':self.tag }
        #
        # Chequeo tener una configuracion valida
        if self.d_local_conf is None:
            return {'DLGID':dlgid,'RSP_PAYLOAD':'CLASS=CONF_BASE&CONFIG=ERROR','TAG':self.tag }
        #
        # Proceso el frame
        bd_hash = self.__get_hash_config_base__()
        # Calculo el hash ( viene en hex por eso lo convierto a decimal )
        dlg_hash_str = self.d_wrk.get('D_QS',{}).get('HASH',[0])[0]
        dlg_hash =  int( dlg_hash_str, 16)
        #
        d_log = { 'MODULE':__name__, 'FUNCTION':'process_frame_config_base', 'LEVEL':'SELECT',
                 'DLGID':dlgid, 'MSG':f'BASE: dlg_hash={dlg_hash}, bd_hash={bd_hash}' }
        log2(d_log)
        #
        d_response = {}
        if dlg_hash == bd_hash:
            d_response = {'DLGID':dlgid,'RSP_PAYLOAD':'CLASS=CONF_BASE&CONFIG=OK','TAG':self.tag }
        else:
            resp = self.__get_response_base__()
            d_response = {'DLGID':dlgid,'RSP_PAYLOAD':f'{resp}','TAG':self.tag }
        #
        # Confirmacion de credenciales:
        endpoint = 'READ_DLGID_FROM_UID'
        params = { 'UID':uid }
        response = self.servicios.process(params=params, endpoint=endpoint)
        if response.status_code() != 200:
            # No pude leer las credenciales
            d_response = {'DLGID':dlgid, 'RSP_PAYLOAD': 'ERROR: UPDATE CREDENCALES','TAG':self.tag}
            log2 ({ 'MODULE':__name__, 'FUNCTION':'process', 'LEVEL':'INFO',
                   'DLGID':dlgid, 'MSG':f'({self.tag}) ERROR: UPDATE CREDENCALES'})
            return d_response
        #
        c_dlgid = response.json().get('DLGID','00000')
        if dlgid != c_dlgid:
            # No estan actualizadas: las actualizo. No chequeo errores
            endpoint = 'SAVE_DLGID_UID'
            params = { 'DLGID':dlgid, 'UID':uid }
            _= self.servicios.process(params=params, endpoint=endpoint)
        #
        return d_response

    def __process_frame_config_ainputs__(self)->dict:
        '''
        Funcion usada para configurar los parametros de las entradas analogicas.
        El QS es: ID=PABLO&TYPE=SPXR3&VER=1.0.0&CLASS=CONF_AINPUTS&HASH=0x01

        Testing:
        GET /cgi-bin/spcommsV3/spcommsV3.py?ID=PABLO&TYPE=SPXR3&VER=1.0.0&CLASS=CONF_AINPUTS&HASH=0x01
        HTTP/1.1
        Host: www.spymovil.com
        '''
        dlgid = self.d_wrk.get('ID','00000')
        if dlgid == 'DEFAULT':
            return {'DLGID':dlgid,'RSP_PAYLOAD':'CLASS=CONF_AINPUTS&CONFIG=ERROR','TAG':self.tag }
        #
        # Chequeo tener una configuracion valida
        if self.d_local_conf is None:
            return {'DLGID':dlgid,'RSP_PAYLOAD':'CLASS=CONF_AINPUTS&CONFIG=ERROR','TAG':self.tag }
        #
        # Proceso el frame
        bd_hash = self.__get_hash_config_ainputs__()
        dlg_hash_str = self.d_wrk.get('D_QS',{}).get('HASH',[0])[0]
        dlg_hash =  int( dlg_hash_str, 16)
        #
        d_log = { 'MODULE':__name__, 'FUNCTION':'process_frame_config_ainputs', 'LEVEL':'SELECT',
                 'DLGID':dlgid, 'MSG':f'BASE: dlg_hash={dlg_hash}, bd_hash={bd_hash}' }
        log2(d_log)
        #
        if dlg_hash == bd_hash:
            return {'DLGID':dlgid,'RSP_PAYLOAD':'CLASS=CONF_AINPUTS&CONFIG=OK','TAG':self.tag }
        else:
            resp = self.__get_response_ainputs__()
            return {'DLGID':dlgid,'RSP_PAYLOAD':f'{resp}','TAG':self.tag }
        #

    def __process_frame_config_counters__(self)->dict:
        '''
        Funcion usada para configurar los parametros de las entradas de contadores.
        El QS es: ID=PABLO&TYPE=SPXR3&VER=1.0.0&CLASS=CONF_COUNTERS&HASH=0x86

        Testing:
        GET /cgi-bin/spcommsV3/spcommsV3.py?ID=PABLO&TYPE=SPXR3&VER=1.0.0&CLASS=CONF_COUNTERS&HASH=0x86
        HTTP/1.1
        Host: www.spymovil.com
        '''
        dlgid = self.d_wrk.get('ID','00000')
        if dlgid == 'DEFAULT':
            return {'DLGID':dlgid,'RSP_PAYLOAD':'CLASS=CONF_COUNTERS&CONFIG=ERROR','TAG':self.tag }
        #
        # Chequeo tener una configuracion valida
        if self.d_local_conf is None:
            return {'DLGID':dlgid,'RSP_PAYLOAD':'CLASS=CONF_COUNTERS&CONFIG=ERROR','TAG':self.tag }
        #
        # Proceso el frame
        bd_hash = self.__get_hash_config_counters__()
        # Calculo el hash ( viene en hex por eso lo convierto a decimal )
        dlg_hash_str = self.d_wrk.get('D_QS',{}).get('HASH',[0])[0]
        dlg_hash =  int( dlg_hash_str, 16)
        #
        d_log = { 'MODULE':__name__, 'FUNCTION':'process_frame_config_base', 'LEVEL':'SELECT',
                 'DLGID':dlgid, 'MSG':f'BASE: dlg_hash={dlg_hash}, bd_hash={bd_hash}' }
        log2(d_log)
        #
        if dlg_hash == bd_hash:
            return {'DLGID':dlgid,'RSP_PAYLOAD':'CLASS=CONF_COUNTERS&CONFIG=OK','TAG':self.tag }
        else:
            resp = self.__get_response_counters__()
            return {'DLGID':dlgid,'RSP_PAYLOAD':f'{resp}','TAG':self.tag }
        #

    def __process_frame_config_modbus__(self)->dict:
        '''
        Funcion usada para configurar los parametros de los canales modbus.
        El QS es: ID=PABLO&TYPE=SPXR3&VER=1.0.0&CLASS=CONF_MODBUS&HASH=0x86

        Testing:
        GET /cgi-bin/spcommsV3/spcommsV3.py?ID=PABLO&TYPE=SPXR3&VER=1.0.0&CLASS=CONF_MODBUS&HASH=0x86
        HTTP/1.1
        Host: www.spymovil.com
        '''
        dlgid = self.d_wrk.get('ID','00000')
        if dlgid == 'DEFAULT':
            return {'DLGID':dlgid,'RSP_PAYLOAD':'CLASS=CONF_MODBUS&CONFIG=ERROR','TAG':self.tag }
        #
        # Chequeo tener una configuracion valida
        if self.d_local_conf is None:
            return {'DLGID':dlgid,'RSP_PAYLOAD':'CLASS=CONF_MODBUS&CONFIG=ERROR','TAG':self.tag }
        #
        # Proceso el frame
        bd_hash = self.__get_hash_config_modbus__()
        # Calculo el hash ( viene en hex por eso lo convierto a decimal )
        dlg_hash_str = self.d_wrk.get('D_QS',{}).get('HASH',[0])[0]
        dlg_hash =  int( dlg_hash_str, 16)
        #
        d_log = { 'MODULE':__name__, 'FUNCTION':'process_frame_config_base', 'LEVEL':'SELECT',
                 'DLGID':dlgid, 'MSG':f'BASE: dlg_hash={dlg_hash}, bd_hash={bd_hash}' }
        log2(d_log)
        #
        if dlg_hash == bd_hash:
            return {'DLGID':dlgid,'RSP_PAYLOAD':'CLASS=CONF_MODBUS&CONFIG=OK','TAG':self.tag }
        else:
            resp = self.__get_response_modbus__()
            return {'DLGID':dlgid,'RSP_PAYLOAD':f'{resp}','TAG':self.tag }
        #

    def __process_frame_data__(self):
        '''
        Funcion usada para procesar los frames con datos
        El QS es: ID=PABLO&TYPE=SPXR3&VER=1.0.0&CLASS=DATA&DATE=230321&TIME=094504&A0=0.00&A1=0.00&A2=0.00&C0=0.000&C1=0.000&bt=12.496

        Testing:
        GET /cgi-bin/spcommsV3/spcommsV3.py?ID=PABLO&TYPE=SPXR3&VER=1.0.0&CLASS=DATA&DATE=230321&TIME=094504&A0=0.00&A1=0.00&A2=0.00&C0=0.000&C1=0.000&bt=12.496
        HTTP/1.1
        Host: www.spymovil.com
        '''
        d_tmp = self.d_wrk.get('D_QS', {})
        dlgid = d_tmp.get('ID', '00000')[0]     # el parse_qs devuleve listas !!
        protocol = d_tmp.get('TYPE','ERR')
        # 1) Elimino las claves que no son necesarias
        d_tmp.pop('ID',None)
        d_tmp.pop('VER',None)
        d_tmp.pop('CLASS',None)
        d_tmp.pop('TYPE',None)
        # 2) Convierto el resto de valores de listas a elementos individuales
        d_payload = { k:d_tmp[k][0] for k in d_tmp }
        #
        # 3) Normalizo el d_payload completando campos faltantes de otros protocolos ( SPXR2 )
        d_payload = normalize_frame_data( protocol, d_payload)
        #
        spc_stats.inc_count_frame_data()
        #
        # 4) Guardo los datos en las BD (redis y SQL)
        endpoint = 'SAVE_DATA_LINE'
        params = { 'DLGID':dlgid, 'D_LINE':d_payload }
        response = self.servicios.process(params=params, endpoint=endpoint)
        if response.status_code() != 200:
            # ERROR No pude salvar los datos, pero igual sigo y le respondo
            self.d_response = {'DLGID':dlgid, 'RSP_PAYLOAD': 'ERROR: UNABLE TO SAVE DATA','TAG':self.tag}
            log2 ({ 'MODULE':__name__, 'FUNCTION':'process', 'LEVEL':'INFO',
                 'DLGID':dlgid, 'MSG':f'({self.tag}) ERROR: UNABLE TO SAVE DATA'})
        #
        # 5) Armo la salida
        now=dt.datetime.now().strftime('%y%m%d%H%M')
        frame_rsp = f'CLASS=DATA&CLOCK={now}'
        #
        # 6) Agrego ordenes que leo del redis local
        endpoint = 'READ_ORDENES'
        params = { 'DLGID':dlgid }
        response = self.servicios.process(params=params, endpoint=endpoint)
        # Puede no haber una key ORDENES y esto no seria un error. Asi que no chequeo errores
        if response.status_code() == 200:
            ordenes = response.json().get('ORDENES','')
            if ordenes:
                frame_rsp += f';{ordenes}'
                # Si el equipo lo estoy mandando a resetearse, borro las entradas a la bd redis.
                if 'RESET' in ordenes:
                    # Debo borrar la entrada de configuracion para que se rehaga. No controlo errores
                    endpoint = 'DELETE_ENTRY'
                    params = { 'DLGID':dlgid }
                    _ = self.servicios.process(params=params, endpoint=endpoint)
                #
            #
        #
        # 7) Actualizo los datos para el monitoreo.
        endpoint = 'SAVE_FRAME_TIMESTAMP'
        params = { 'DLGID':dlgid }
        _ = self.servicios.process(params=params, endpoint=endpoint)
        #
        return {'DLGID':dlgid,'RSP_PAYLOAD':frame_rsp,'TAG':self.tag }

    
     # HASHES -------------------------------------------------------------

    def __get_hash_config_base__(self)->int:
        '''
        Calcula el hash de la configuracion base.
        RETURN: int
        '''
        xhash = 0
        timerpoll = int(self.d_local_conf.get(('BASE', 'TPOLL'),'0'))
        timerdial = int(self.d_local_conf.get(('BASE', 'TDIAL'),'0'))
        pwr_modo = int(self.d_local_conf.get(('BASE', 'PWRS_MODO'),'0'))
        pwr_hhmm_on = int(self.d_local_conf.get(('BASE', 'PWRS_HHMM1'),'601'))
        pwr_hhmm_off = int(self.d_local_conf.get(('BASE', 'PWRS_HHMM2'),'2201'))
        #
        hash_str = f'[TIMERPOLL:{timerpoll:03d}]'
        xhash = u_hash(xhash, hash_str)
        #d_log = { 'MODULE':__name__, 'FUNCTION':'calcular_hash_config_base', 'LEVEL':'ERROR',
        #        'DLGID': d_params['DLGID'], 'MSG':f'DEBUG: HASH_BASE: hash_str=<{hash_str}>, hash={xhash}' }
        #log2(d_log)
        #
        hash_str = f'[TIMERDIAL:{timerdial:03d}]'
        xhash = u_hash(xhash, hash_str)
        #d_log = { 'MODULE':__name__, 'FUNCTION':'calcular_hash_config_base', 'LEVEL':'ERROR',
        #        'DLGID': d_params['DLGID'], 'MSG':f'DEBUG: HASH_BASE: hash_str=<{hash_str}>, hash={xhash}' }
        #log2(d_log)
        #
        hash_str = f'[PWRMODO:{pwr_modo}]'
        xhash = u_hash(xhash, hash_str)
        #d_log = { 'MODULE':__name__, 'FUNCTION':'calcular_hash_config_base', 'LEVEL':'ERROR',
        #        'DLGID': d_params['DLGID'], 'MSG':f'DEBUG: HASH_BASE: hash_str=<{hash_str}>, hash={xhash}' }
        #log2(d_log)
        #
        hash_str = f'[PWRON:{pwr_hhmm_on:04}]'
        xhash = u_hash(xhash, hash_str)
        #d_log = { 'MODULE':__name__, 'FUNCTION':'calcular_hash_config_base', 'LEVEL':'ERROR',
        #        'DLGID': d_params['DLGID'], 'MSG':f'DEBUG: HASH_BASE: hash_str=<{hash_str}>, hash={xhash}' }
        #log2(d_log)
        #
        hash_str = f'[PWROFF:{pwr_hhmm_off:04}]'
        xhash = u_hash(xhash, hash_str)
        #d_log = { 'MODULE':__name__, 'FUNCTION':'calcular_hash_config_base', 'LEVEL':'ERROR',
        #        'DLGID': d_params['DLGID'], 'MSG':f'DEBUG: HASH_BASE: hash_str=<{hash_str}>, hash={xhash}' }
        #log2(d_log)
        #
        # A partir de la version 105 incorporamos 'samples'y almlevel'
        fw_version = version2int( self.d_wrk.get('VER','1.0.0'))
        if fw_version >= 105:
            samples = int(self.d_local_conf.get(('BASE', 'SAMPLES'),'1'))
            almlevel = int(self.d_local_conf.get(('BASE', 'ALMLEVEL'),'0'))
            hash_str = f'[SAMPLES:{samples:02}]'
            xhash = u_hash(xhash, hash_str)
            #d_log = { 'MODULE':__name__, 'FUNCTION':'calcular_hash_config_base', 'LEVEL':'ERROR',
            #    'DLGID': d_params['DLGID'], 'MSG':f'DEBUG: HASH_BASE: hash_str=<{hash_str}>, hash={xhash}' }
            #log2(d_log)
            #
            hash_str = f'[ALMLEVEL:{almlevel:02}]'
            xhash = u_hash(xhash, hash_str)
            #d_log = { 'MODULE':__name__, 'FUNCTION':'calcular_hash_config_base', 'LEVEL':'ERROR',
            #    'DLGID': d_params['DLGID'], 'MSG':f'DEBUG: HASH_BASE: hash_str=<{hash_str}>, hash={xhash}' }
            #log2(d_log)
        #
        return xhash

    def __get_hash_config_ainputs__(self)->int:
        '''
        Calcula el hash de la configuracion de canales analoggicos.
        RETURN: int
        '''
        #
        xhash = 0
        for channel in ['A0','A1','A2']:
            if (channel,'NAME') in self.d_local_conf:      # ( 'A1', 'NAME' ) in  d.keys()
                name=self.d_local_conf.get((channel, 'NAME'),'X')
                imin=int( self.d_local_conf.get((channel, 'IMIN'),'0'))
                imax=int( self.d_local_conf.get((channel, 'IMAX'),'0'))
                mmin=float( self.d_local_conf.get((channel, 'MMIN'),'0'))
                mmax=float( self.d_local_conf.get((channel, 'MMAX'),'0'))
                offset=float( self.d_local_conf.get((channel, 'OFFSET'),'0'))
                hash_str = f'[{channel}:{name},{imin},{imax},{mmin:.02f},{mmax:.02f},{offset:.02f}]'
            else:
                hash_str = f'[{channel}:X,4,20,0.00,10.00,0.00]'
            #
            xhash = u_hash(xhash, hash_str)
        return xhash

    def __get_hash_config_counters__(self)->int:
        '''
        Calcula el hash de la configuracion de canales digitales ( contadores ).
        RETURN: int
        '''
        #
        xhash = 0
        for channel in ['C0','C1']:
            if (channel,'NAME') in self.d_local_conf:      # ( 'C0', 'NAME' ) in  d.keys()
                name=self.d_local_conf.get((channel, 'NAME'),'X')
                str_modo = self.d_local_conf.get((channel, 'MODO'),'CAUDAL')
                if str_modo == 'CAUDAL':
                    modo = 0    # caudal
                else:
                    modo = 1    # pulsos
                magpp=float(self.d_local_conf.get((channel, 'MAGPP'),'0'))
                hash_str = f'[{channel}:{name},{magpp:.03f},{modo}]'
            else:
                hash_str = f'[{channel}:X,1.000,0]'
            #
            xhash = u_hash(xhash, hash_str)
        return xhash

    def __get_hash_config_modbus__(self)->int:
        '''
        Calcula el hash de la configuracion de canales modbus.
        RETURN: int
        '''
        #
        xhash = 0
        for channel in ['M0','M1','M2','M3','M4']:
            if (channel,'NAME') in self.d_local_conf:      # ( 'A1', 'NAME' ) in  d.keys()
                name=self.d_local_conf.get((channel, 'NAME'),'X')
                sla_addr=int(self.d_local_conf.get((channel, 'SLA_ADDR'),'0'))
                reg_addr=int(self.d_local_conf.get((channel, 'ADDR'),'0'))
                nro_regs=int(self.d_local_conf.get((channel, 'NRO_RECS'),'0'))
                fcode=int(self.d_local_conf.get((channel, 'FCODE'),'0'))
                mtype=self.d_local_conf.get((channel, 'TYPE'),'U16')
                codec=self.d_local_conf.get((channel, 'CODEC'),'C0123')
                pow10=int(self.d_local_conf.get((channel, 'POW10'),'0'))
                #
                hash_str = f'[{channel}:{name},{sla_addr:02d},{reg_addr:04d},{nro_regs:02d},{fcode:02d},{mtype},{codec},{pow10:02d}]'
            else:
                hash_str = f'[{channel}:X,00,0000,00,00,U16,C0123,00]'
            #
            xhash = u_hash(xhash, hash_str)
        return xhash

    # RESPONSES -----------------------------------------------------------
    
    def __get_response_base__(self)->str:
        '''
        Calcula la respuesta de configuracion base
        RETURN: string
        '''
        #
        timerpoll = int( self.d_local_conf.get(('BASE', 'TPOLL'),'0'))
        timerdial = int(self.d_local_conf.get(('BASE', 'TDIAL'),'0'))
        pwr_modo = int(self.d_local_conf.get(('BASE', 'PWRS_MODO'),'0'))
        pwr_hhmm_on = int(self.d_local_conf.get(('BASE', 'PWRS_HHMM1'),'600'))
        pwr_hhmm_off = int(self.d_local_conf.get(('BASE', 'PWRS_HHMM2'),'2200'))
        if pwr_modo == 0:
            s_pwrmodo = 'CONTINUO'
        elif pwr_modo == 1:
            s_pwrmodo = 'DISCRETO'
        else:
            s_pwrmodo = 'MIXTO'
        #
        response = 'CLASS=CONF_BASE&'
        response += f'TPOLL={timerpoll}&TDIAL={timerdial}&PWRMODO={s_pwrmodo}&PWRON={pwr_hhmm_on:04}&PWROFF={pwr_hhmm_off:04}'
        #
        fw_version = version2int( self.d_wrk.get('VER','1.0.0'))
        if fw_version >= 105:
            samples = int( self.d_local_conf.get(('BASE', 'SAMPLES'),'1'))
            almlevel = int( self.d_local_conf.get(('BASE', 'ALMLEVEL'),'0'))
            response += f'&SAMPLES={samples}&ALMLEVEL={almlevel}'
        #    
        return response
    
    def __get_response_ainputs__(self)->str:
        '''
        Calcula la respuesta de configuracion de canales analogicos
        '''
        #
        response = 'CLASS=CONF_AINPUTS&'
        for channel in ('A0','A1','A2'):
            name = self.d_local_conf.get((channel, 'NAME'), 'X')
            imin = int(self.d_local_conf.get((channel, 'IMIN'), 4))
            imax = int(self.d_local_conf.get((channel, 'IMAX'), 20))
            mmin = float(self.d_local_conf.get((channel, 'MMIN'), 0.00))
            mmax = float(self.d_local_conf.get((channel, 'MMAX'), 10.00))
            offset = float(self.d_local_conf.get((channel, 'OFFSET'), 0.00))
            response += f'{channel}={name},{imin},{imax},{mmin},{mmax},{offset}&'
        #
        response = response[:-1]
        return response

    def __get_response_counters__(self)->str:
        '''
        Calcula la respuesta de configuracion de canales contadores
        '''
        response = 'CLASS=CONF_COUNTERS&'
        for channel in ('C0','C1'):
            name = self.d_local_conf.get((channel, 'NAME'), 'X')
            magpp = float(self.d_local_conf.get((channel, 'MAGPP'), 1.00))
            str_modo = self.d_local_conf.get((channel, 'MODO'),'CAUDAL')
            response += f'{channel}={name},{magpp},{str_modo}&'
        #
        response = response[:-1]
        return response

    def __get_response_modbus__(self)->str:
        '''
        Calcula la respuesta de configuracion de canales modbus
        '''
        response = 'CLASS=CONF_MODBUS&'
        for channel in ['M0','M1','M2','M3','M4']:
            name=self.d_local_conf.get((channel, 'NAME'),'X')
            sla_addr=int(self.d_local_conf.get((channel, 'SLA_ADDR'),'0'))
            reg_addr=int(self.d_local_conf.get((channel, 'ADDR'),'0'))
            nro_regs=int(self.d_local_conf.get((channel, 'NRO_RECS'),'0'))
            fcode=int(self.d_local_conf.get((channel, 'FCODE'),'0'))
            mtype=self.d_local_conf.get((channel, 'TYPE'),'U16')
            codec=self.d_local_conf.get((channel, 'CODEC'),'C0123')
            pow10=int(self.d_local_conf.get((channel, 'POW10'),'0'))
            response += f'{channel}={name},{sla_addr},{reg_addr},{nro_regs},{fcode},{mtype},{codec},{pow10}&'
        #
        response = response[:-1]
        return response

     # ---------------------------------------------------------------------


class TestProtocoloSPXV3:

    def __init__(self):
        self.dlgid = ''
        self.d_input = { 'GET': { 'QS':'', 'SIZE':0 },  'POST': {'STREAM':'', 'BYTES':b'', 'SIZE':1 } }
        self.p_spxr3 = ProtocoloSPXR3()

    def test_ping(self):

        print('TEST PING Start...')
        self.dlgid = 'PABLO'
        set_debug_dlgid(self.dlgid)
        qs = 'ID=PABLO&TYPE=SPXR3&VER=1.0.0&CLASS=PING'
        self.d_input['GET']['QS'] = qs
        _ = self.p_spxr3.process(self.d_input)
        print('TEST PING Stop...')

    def test_config_base(self):

        print('TEST CONFIG_BASE Start...')
        self.dlgid = 'PABLO'
        set_debug_dlgid(self.dlgid)
        qs = 'ID=PABLO&TYPE=SPXR3&VER=1.0.0&CLASS=CONF_BASE&UID=42125128300065090117010400000000&HASH=0x11'
        self.d_input['GET']['QS'] = qs
        _ = self.p_spxr3.process(self.d_input)
        print('TEST CONFIG_BASE Stop...')

    def test_config_ainputs(self):

        print('TEST CONFIG_AINPUTS Start...')
        self.dlgid = 'PABLO'
        set_debug_dlgid(self.dlgid)
        qs = 'ID=PABLO&TYPE=SPXR3&VER=1.0.0&CLASS=CONF_AINPUTS&HASH=0x01' 
        self.d_input['GET']['QS'] = qs
        _ = self.p_spxr3.process(self.d_input)
        print('TEST CONFIG_AINPUTS Stop...')

    def test_config_counters(self):

        print('TEST CONFIG_COUNTERS Start...')
        self.dlgid = 'PABLO'
        set_debug_dlgid(self.dlgid)
        qs = 'ID=PABLO&TYPE=SPXR3&VER=1.0.0&CLASS=CONF_COUNTERS&HASH=0x86'
        self.d_input['GET']['QS'] = qs
        _ = self.p_spxr3.process(self.d_input)
        print('TEST CONFIG_COUNTERS Stop...')

    def test_config_modbus(self):

        print('TEST CONFIG_MODBUS Start...')
        self.dlgid = 'PABLO'
        set_debug_dlgid(self.dlgid)
        qs = 'ID=PABLO&TYPE=SPXR3&VER=1.0.0&CLASS=CONF_MODBUS&HASH=0x86'
        self.d_input['GET']['QS'] = qs
        _ = self.p_spxr3.process(self.d_input)
        print('TEST CONFIG_MODBUS Stop...')

    def test_data(self):

        print('TEST DATA Start...')
        self.dlgid = 'PABLO'
        set_debug_dlgid(self.dlgid)
        qs = 'ID=PABLO&TYPE=SPXR3&VER=1.0.0&CLASS=DATA&DATE=230321&TIME=094504&A0=0.00&A1=0.00&A2=0.00&C0=0.000&C1=0.000&bt=12.496'
        self.d_input['GET']['QS'] = qs
        _ = self.p_spxr3.process(self.d_input)
        print('TEST DATA Stop...')

    def test_recover(self):

        print('TEST RECCOVER Start...')
        self.dlgid = 'PABLO'
        set_debug_dlgid(self.dlgid)
        qs = ' ID=DEFAULT&TYPE=SPXR3&VER=1.0.0&CLASS=RECOVER&UID=42125128300065090117010400000000'
        self.d_input['GET']['QS'] = qs
        _ = self.p_spxr3.process(self.d_input)
        print('TEST RECOVER Stop...')

if __name__ == '__main__':
    
    config_logger('CONSOLA')

    test = TestProtocoloSPXV3()
    test.test_ping()
    #test.test_recover()
    #test.test_config_base()
    #test.test_config_ainputs()
    #test.test_config_counters()
    #test.test_config_modbus()
    #test.test_data()
    sys.exit(0)


    
  
