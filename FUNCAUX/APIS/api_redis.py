#!/home/pablo/Spymovil/www/cgi-bin/spcommsV3/venv/bin/python3

'''
Funciones de API de configuracion.
Se utilizan para leer configuracion de equipos de una BD persistente (GDA)
Las claves utilzadas son:

'idNNN' : { 'PKCONFIG':{ {CONF}, 'MEMBLOCK':{}, 'REMVARS':{} }, 
            'ORDENES',
            'ATVISE', 
            'PKLINE', 
            'PKTIMESTAMP', 
            'ATVISE_ORDERS' 
          }
'SPCOMMS': {'DEBUG_DLGID' }
'UID2DLGID'
'STATS_QUEUE'
'RXDATA_QUEUE'

D_CONFIG = {
            ('BASE', 'ALMLEVEL'): '10', ('BASE', 'BAT'): 'ON', ('BASE', 'COMMITED_CONF'): '0', ('BASE', 'DIST'): '0',('BASE', 'FIRMWARE'): '4.0.0a',
            ('BASE', 'HW_CONTADORES'): 'OPTO',('BASE', 'IMEI'): '860585007136848',('BASE', 'IPADDRESS'): '0.0.0.0',('BASE', 'PWRS_HHMM1'): '1800',
            ('BASE', 'PWRS_HHMM2'): '1440', ('BASE', 'PWRS_MODO'): '0',('BASE', 'RESET'): '0',('BASE', 'SAMPLES'): '1',('BASE', 'SIMID'): '8959801615239182186F',
            ('BASE', 'TDIAL'): '900',('BASE', 'TIMEPWRSENSOR'): '5',('BASE', 'TPOLL'): '30',('BASE', 'UID'): '42125128300065090117010400000000',('BAT', 'NAME'): 'bt',
            ('BAT', 'OFFSET'): '0',('C0', 'EDGE'): 'RISE',('C0', 'MAGPP'): '0.01',('C0', 'NAME'): 'q0',('C0', 'OFFSET'): '0',('C0', 'PERIOD'): '100',
            ('C0', 'PWIDTH'): '10',('C0', 'SPEED'): 'LS',('A0', 'IMAX'): '20',('A0', 'IMIN'): '4',('A0', 'MMAX'): '10',('A0', 'MMIN'): '0',('A0', 'NAME'): 'HTQ',
            ('A0', 'OFFSET'): '0',('A1', 'IMAX'): '20',('A1', 'IMIN'): '4',('A1', 'MMAX'): '10',('A1', 'MMIN'): '0',('A1', 'NAME'): 'X',('A1', 'OFFSET'): '0',
            ('M2', 'ADDR'): '2070',('M2', 'FCODE'): '3',('M2', 'NAME'): 'QS',('M2', 'SIZE'): '2',('M2', 'TIPO'): 'float',('M0', 'ADDR'): '2069',('M0', 'CODEC'): 'C1032',
            ('M0', 'FCODE'): '3',('M0', 'MMAX'): '10',('M0', 'MMIN'): '0',('M0', 'NAME'): 'AI0',('M0', 'NRO_RECS'): '2',('M0', 'POW10'): '0',('M0', 'REG_ADDR'): '2069',
            ('M0', 'SIZE'): '2',('M0', 'SLA_ADDR'): '2',('M0', 'TIPO'): 'FLOAT',('M0', 'TYPE'): 'FLOAT',('M1', 'ADDR'): '2069',('M1', 'CODEC'): 'C1032',
            ('M1', 'FCODE'): '3',('M1', 'MMAX'): '10',('M1', 'MMIN'): '0',('M1', 'NAME'): 'AI0',('M1', 'NRO_RECS'): '2',('M1', 'POW10'): '0',('M1', 'REG_ADDR'): '2069',
            ('M1', 'SIZE'): '2',('M1', 'SLA_ADDR'): '2',('M1', 'TIPO'): 'FLOAT',('M1', 'TYPE'): 'FLOAT',
    'MEMBLOCK': {
        'RCVD_MBK_LENGTH': 21, 'RCVD_MBK_DEF': [('var1', 'uchar', 0),('var2', 'uchar', 1),('var3', 'float', 2),('var4', 'short', 3),('var5', 'short', 5)],
        'SEND_MBK_LENGTH': 21,'SEND_MBK_DEF': [('var1', 'uchar', 0),('var2', 'uchar', 1),('var3', 'float', 2),('var4', 'short', 3),('var5', 'short', 5)]},
    'REMVARS': {
        'KYTQ003': [['HTQ1', 'NIVEL_TQ_KIYU']]
                }
            }
PKCONFIG = 
b'\x80\x04\x95\r\x05\x00\x00\x00\x00\x00\x00}\x94(\x8c\x04BASE\x94\x8c\x08ALMLEVEL\x94\x86\x94\x8c\x0210\x94h
\x01\x8c\x03BAT\x94\x86\x94\x8c\x02ON\x94h\x01\x8c\rCOMMITED_CONF\x94\x86\x94\x8c\x010\x94h\x01\x8c\x04DIST\x94
\x86\x94h\nh\x01\x8c\x08FIRMWARE\x94\x86\x94\x8c\x064.0.0a\x94h\x01\x8c\rHW_CONTADORES\x94\x86\x94\x8c\x04OPTO
\x94h\x01\x8c\x04IMEI\x94\x86\x94\x8c\x0f860585007136848\x94h\x01\x8c\tIPADDRESS\x94\x86\x94\x8c\x070.0.0.0\x94h
\x01\x8c\nPWRS_HHMM1\x94\x86\x94\x8c\x041800\x94h\x01\x8c\nPWRS_HHMM2\x94\x86\x94\x8c\x041440\x94h\x01\x8c
\tPWRS_MODO\x94\x86\x94h\nh\x01\x8c\x05RESET\x94\x86\x94h\nh\x01\x8c\x07SAMPLES\x94\x86\x94\x8c\x011\x94h\x01
\x8c\x05SIMID\x94\x86\x94\x8c\x148959801615239182186F\x94h\x01\x8c\x05TDIAL\x94\x86\x94\x8c\x03900\x94h\x01\x8c
\rTIMEPWRSENSOR\x94\x86\x94\x8c\x015\x94h\x01\x8c\x05TPOLL\x94\x86\x94\x8c\x0230\x94h\x01\x8c\x03UID\x94\x86\x94
\x8c 42125128300065090117010400000000\x94h\x05\x8c\x04NAME\x94\x86\x94\x8c\x02bt\x94h\x05\x8c\x06OFFSET\x94\x86\x94h
\n\x8c\x02C0\x94\x8c\x04EDGE\x94\x86\x94\x8c\x04RISE\x94h:\x8c\x05MAGPP\x94\x86\x94\x8c\x040.01\x94h:h5\x86\x94\x8c
\x02q0\x94h:h8\x86\x94h\nh:\x8c\x06PERIOD\x94\x86\x94\x8c\x03100\x94h:\x8c\x06PWIDTH\x94\x86\x94h\x04h:\x8c\x05SPEED
\x94\x86\x94\x8c\x02LS\x94\x8c\x02A0\x94\x8c\x04IMAX\x94\x86\x94\x8c\x0220\x94hL\x8c\x04IMIN\x94\x86\x94\x8c\x014\x94hL
\x8c\x04MMAX\x94\x86\x94h\x04hL\x8c\x04MMIN\x94\x86\x94h\nhLh5\x86\x94\x8c\x03HTQ\x94hLh8\x86\x94h\n\x8c\x02A1\x94hM\x86
\x94hOhZhP\x86\x94hRhZhS\x86\x94h\x04hZhU\x86\x94h\nhZh5\x86\x94\x8c\x01X\x94hZh8\x86\x94h\n\x8c\x02M2\x94\x8c\x04ADDR
\x94\x86\x94\x8c\x042070\x94hb\x8c\x05FCODE\x94\x86\x94\x8c\x013\x94hbh5\x86\x94\x8c\x02QS\x94hb\x8c\x04SIZE\x94\x86\x94
\x8c\x012\x94hb\x8c\x04TIPO\x94\x86\x94\x8c\x05float\x94\x8c\x02M0\x94hc\x86\x94\x8c\x042069\x94hq\x8c\x05CODEC\x94\x86
\x94\x8c\x05C1032\x94hqhf\x86\x94hhhqhS\x86\x94h\x04hqhU\x86\x94h\nhqh5\x86\x94\x8c\x03AI0\x94hq\x8c\x08NRO_RECS\x94\x86
\x94hmhq\x8c\x05POW10\x94\x86\x94h\nhq\x8c\x08REG_ADDR\x94\x86\x94hshqhk\x86\x94hmhq\x8c\x08SLA_ADDR\x94\x86\x94hmhqhn\x86
\x94\x8c\x05FLOAT\x94hq\x8c\x04TYPE\x94\x86\x94h\x86\x8c\x02M1\x94hc\x86\x94hsh\x89ht\x86\x94hvh\x89hf\x86\x94hhh\x89hS\x86
\x94h\x04h\x89hU\x86\x94h\nh\x89h5\x86\x94h{h\x89h|\x86\x94hmh\x89h~\x86\x94h\nh\x89h\x80\x86\x94hsh\x89hk\x86\x94hmh\x89h
\x83\x86\x94hmh\x89hn\x86\x94h\x86h\x89h\x87\x86\x94h\x86\x8c\x08MEMBLOCK\x94}\x94(\x8c\x0fRCVD_MBK_LENGTH\x94K\x15\x8c
\x0cRCVD_MBK_DEF\x94]\x94(\x8c\x04var1\x94\x8c\x05uchar\x94K\x00\x87\x94\x8c\x04var2\x94h\x9dK\x01\x87\x94\x8c\x04var3\x94hpK
\x02\x87\x94\x8c\x04var4\x94\x8c\x05short\x94K\x03\x87\x94\x8c\x04var5\x94h\xa4K\x05\x87\x94e\x8c\x0fSEND_MBK_LENGTH\x94K
\x15\x8c\x0cSEND_MBK_DEF\x94]\x94(h\x9eh\xa0h\xa2h\xa5h\xa7eu\x8c\x07REMVARS\x94}\x94\x8c\x07KYTQ003\x94]\x94]\x94(\x8c\x04HTQ1
\x94\x8c\rNIVEL_TQ_KIYU\x94easu.'

'''

import os
import sys
import random
import redis
import pickle

current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
pparent = os.path.dirname(parent)
sys.path.append(pparent)

from FUNCAUX.UTILS.spc_config import Config
from FUNCAUX.UTILS.spc_log import log2, config_logger, set_debug_dlgid
from FUNCAUX.UTILS.spc_utils import trace_request, trace_response
from FUNCAUX.UTILS import spc_stats
from FUNCAUX.UTILS import spc_responses

# ------------------------------------------------------------------------------

class ApiRedis:
    '''
    Interface con la BD redis.
    Todos los requirements los corremos como callbacks de modo de tener solo
    un metodo publico ( process ).
    Esto nos permite tener un diseño bien estructurado:
    - entradas:procesos:salidas 
    De este modo podemos debugear la entrada y salida.

    En response.json se arma un diccionario con los nombres que espera la capa superior. !!!
    '''
    def __init__(self):
        self.params = {}
        self.endpoint = ''
        self.response = spc_responses.Response()
        self.rh = BdRedis()
        self.callback_endpoints =  { 'READ_CONFIG': self.__read_config__,
                                    'READ_DEBUG_DLGID': self.__read_debug_dlgid__,
                                    'READ_ORDENES': self.__read_ordenes__,
                                    'READ_DLGID_FROM_UID': self.__read_dlgid_from_uid__,
                                    'READ_QUEUE_LENGTH': self.__read_queue_length__,
                                    'DELETE_ENTRY': self.__delete_entry__,
                                    'SAVE_DATA_LINE': self.__save_data_line__,
                                    'SAVE_CONFIG': self.__save_config__,
                                    'SAVE_DLGID_UID': self.__save_dlgid_uid__,
                                    'SAVE_STATS': self.__save_stats__, 
                                    'SAVE_FRAME_TIMESTAMP': self.__save_frame_timestamp__,
                                    'READ_PKLINE': self.__read_pkline__,
                                    'READ_PKATVISE': self.__read_pkatvise__,
                                    }
        self.tag = random.randint(0,1000)

    def process(self, endpoint='', params={}):
        '''
        Unica funcion publica que procesa los requests a la API.
        Permite poder hacer un debug de la entrada y salida.
        '''
        self.endpoint = endpoint
        self.params = params
        #
        trace_request( endpoint=self.endpoint, params=self.params, msg=f'Input API Redis ({self.tag})')
        # Ejecuto la funcion de callback
        if self.endpoint in self.callback_endpoints:
            # La response la fija la funcion de callback
            self.callback_endpoints[self.endpoint]()
        else:
            # ERROR: No existe el endpoint
            self.response.set_status_code(405)
            self.response.set_reason(f"API REDIS: No existe endpoint {endpoint}")
        #
        trace_response( response=self.response, msg=f'Output API Redis ({self.tag})')
        return self.response
      
    def __read_config__(self):
        ''' La api redis devuelve lo que hay en la BD o sea un PKCONF !!'''
        dlgid = self.params.get('DLGID','00000')
        d_response = self.rh.read_config(dlgid)
        #
        self.response.set_dlgid(dlgid)
        if d_response.get('STATUS',False):
            self.response.set_status_code(200)
            self.response.set_reason('OK')
            self.response.set_json( { 'PK_D_CONFIG': d_response.get('CONTENT',b'') } ) 
        else:
            self.response.set_status_code(400)
            self.response.set_reason(d_response.get('REASON','Err'))
            self.response.set_json( { 'PK_D_CONFIG':b'' } ) 
        #

    def __read_debug_dlgid__(self):
        # No quiero loguear las responses de estos entrypoints !!
        d_response = self.rh.read_debug_dlgid()
        if d_response.get('STATUS',False):
            self.response.set_status_code(200)
            self.response.set_reason('OK')
            self.response.set_json( { 'DEBUG_DLGID': d_response.get('CONTENT','00000'),'LOG':False} )
        else:
            self.response.set_status_code(400)
            self.response.set_reason(d_response.get('REASON','Err'))
            self.response.set_json( { 'DEBUG_DLGID':'00000' , 'LOG':False } )
        #

    def __read_ordenes__(self):
        dlgid = self.params.get('DLGID','00000')
        d_response = self.rh.read_ordenes(dlgid)
        #
        self.response.set_dlgid(dlgid)
        if d_response.get('STATUS',False):
            self.response.set_status_code(200)
            self.response.set_reason('OK')
            self.response.set_json( { 'ORDENES': d_response.get('CONTENT','')})
        else:
            self.response.set_status_code(400)
            self.response.set_reason(d_response.get('REASON','Err'))
            self.response.set_json( { 'ORDENES':'' } )
        #

    def __read_dlgid_from_uid__(self):
        uid = self.params.get('UID','0123456789')
        d_response = self.rh.read_dlgid_from_uid(uid)
        if d_response.get('STATUS',False):
            self.response.set_status_code(200)
            self.response.set_reason('OK')
            self.response.set_json( { 'DLGID': d_response.get('CONTENT','00000')})
        else:
            self.response.set_status_code(400)
            self.response.set_reason(d_response.get('REASON','Err'))
            self.response.set_json( { 'DLGID':'00000' } )
        #

    def __read_queue_length__(self):
        '''Leo el largo de una cola de Redis'''
        queue_name = self.params.get('QUEUE_NAME','queue')
        d_response = self.rh.read_queue_length(queue_name)
        if d_response.get('STATUS',False):
            self.response.set_status_code(200)
            self.response.set_reason('OK')
            self.response.set_json( { 'QUEUE_LENGTH': d_response.get('CONTENT', -1 )})
        else:
            self.response.set_status_code(400)
            self.response.set_reason(d_response.get('REASON','Err'))
            self.response.set_json( { 'QUEUE_LENGTH': None } )
        #

    def __delete_entry__(self):
        ''' Borra la entrada de un dlgid para que luego se regenere '''
        dlgid = self.params.get('DLGID','00000')
        dkey = self.params.get('KEY','000000')
        d_response = self.rh.delete_entry(dkey)
        #
        self.response.set_dlgid(dlgid)
        if d_response.get('STATUS',False):
            self.response.set_status_code(200)
            self.response.set_reason('OK')
            self.response.set_json( {} ) 
        else:
            self.response.set_status_code(400)
            self.response.set_reason(d_response.get('REASON','Err'))
            self.response.set_json( {} )
        #

    def __save_data_line__(self):
        ''' Guardo una linea serilizada (pkline) '''
        dlgid = self.params.get('DLGID','00000')
        pkline = self.params.get('PK_LINE','Empty Line')
        d_response = self.rh.save_data_line(dlgid,pkline)
        #
        self.response.set_dlgid(dlgid)
        if d_response.get('STATUS',False):
            self.response.set_status_code(200)
            self.response.set_reason('OK')
            self.response.set_json( {} ) 
        else:
            self.response.set_status_code(400)
            self.response.set_reason(d_response.get('REASON','Err'))
            self.response.set_json( {} )
        #

    def __save_config__(self):
        ''' Guardamos en redis una configuracion en formato serializado PKCONF !!'''
        dlgid = self.params.get('DLGID','00000')
        pkconf = self.params.get('PK_CONFIG','')
        d_response = self.rh.save_config(dlgid,pkconf)
        #
        self.response.set_dlgid(dlgid)
        if d_response.get('STATUS',False):
            self.response.set_status_code(200)
            self.response.set_reason('OK')
            self.response.set_json( {} ) 
        else:
            self.response.set_status_code(400)
            self.response.set_reason(d_response.get('REASON','Err'))
            self.response.set_json( {} ) 
        #

    def __save_dlgid_uid__(self):
        ''' Guardo el par dlgid,uid '''
        dlgid = self.params.get('DLGID','00000')
        uid = self.params.get('UID','0123456789')
        d_response = self.rh.save_dlgid_uid(dlgid,uid)
        #
        self.response.set_dlgid(dlgid)
        if d_response.get('STATUS',False):
            self.response.set_status_code(200)
            self.response.set_reason('OK')
            self.response.set_json( {} ) 
        else:
            self.response.set_status_code(400)
            self.response.set_reason(d_response.get('REASON','Err'))
            self.response.set_json( {} )

    def __save_stats__(self):
        ''' 
        Guardo el diccionario de stats serializado 
        en una cola para luego ser procesado por el exporter 
        '''
        pk_d_stats = self.params.get('PK_D_STATS','')
        dlgid = self.params.get('DLGID','00000')
        #
        d_response = self.rh.save_stats(pk_d_stats)
        #
        self.response.set_dlgid(dlgid)
        if d_response.get('STATUS',False):
            self.response.set_status_code(200)
            self.response.set_reason('OK')
            # Estos frames no lo quiero loguear
            self.response.set_json( {'LOG':False} ) 
        else:
            self.response.set_status_code(400)
            self.response.set_reason(d_response.get('REASON','Err'))
            self.response.set_json( {})
        #

    def __save_frame_timestamp__(self):
        '''
        Guardo el timestamp serializado del frame  para luego
        poder determinar cuando fue la ultima conexion y ver si esta caido
        '''
        dlgid = self.params.get('DLGID','00000')
        pk_timestamp = self.params.get('PK_TIMESTAMP','')
        #
        d_response = self.rh.save_timestamp(dlgid, pk_timestamp)
        #
        self.response.set_dlgid(dlgid)
        if d_response.get('STATUS',False):
            self.response.set_status_code(200)
            self.response.set_reason('OK')
            # Estos frames no lo quiero loguear
            #self.response.set_json( {'LOG':False} ) 
        else:
            self.response.set_status_code(400)
            self.response.set_reason(d_response.get('REASON','Err'))
            self.response.set_json( {} )
        #

    def __read_pkline__(self):
        dlgid = self.params.get('DLGID','00000')
        d_response = self.rh.read_pkline(dlgid)
        #
        self.response.set_dlgid(dlgid)
        if d_response.get('STATUS',False):
            self.response.set_status_code(200)
            self.response.set_reason('OK')
            self.response.set_json( { 'PK_LINE': d_response.get('CONTENT',b'')})
        else:
            self.response.set_status_code(400)
            self.response.set_reason(d_response.get('REASON','Err'))
            self.response.set_json( { 'PK_LINE':b'' } )       
        #
   
    def __read_pkatvise__(self):
        '''
        Leo la clave ATVISE.
        Este es un diccionario serializado con las ordenes ( respuestas ) hacia el PLC
        '''
        dlgid = self.params.get('DLGID','00000')
        d_response = self.rh.read_pkatvise(dlgid)
        #
        self.response.set_dlgid(dlgid)
        if d_response.get('STATUS',False):
            self.response.set_status_code(200)
            self.response.set_reason('OK')
            self.response.set_json( { 'PK_ATVISE': d_response.get('CONTENT',b'')})
        else:
            self.response.set_status_code(400)
            self.response.set_reason(d_response.get('REASON','Err'))
            self.response.set_json( { 'PK_ATVISE':b'' } )     


class BdRedis:
    '''
    Implemento los metodos para leer de la BD redis.
    Por ahora es la 192.168.0.6

    RESPUESTAS: 
        D_RESPONSE { 'STATUS': True/False,
                    'REASON':Text,
                    'CONTENT': None | valores en formato de c/metodo invocado 
                   }

    En CONTENT se devuelve lo que hay en la base: no se interpreta !!!        
    '''
    def __init__(self):
        self.connected = False
        self.rh = redis.Redis()

    def connect(self):
        '''
        Cada acceso primero verifica si esta conectada asi que aqui incrementamos el contador de accesos.
        '''
        spc_stats.inc_count_accesos_REDIS()

        if self.connected:
            return True

        host=Config['REDIS']['host']
        port=int(Config['REDIS']['port'])
        db=int( Config['REDIS']['db'], 10)
       
        try:
            self.rh = redis.Redis( host, port, db)
            self.connected = True
        except redis.RedisError as re:
            d_log = { 'MODULE':__name__, 'FUNCTION':'connect', 'LEVEL':'ERROR', 'MSG':'Bd redis init ERROR' }
            log2(d_log)
            d_log = { 'MODULE':__name__, 'FUNCTION':'connect', 'LEVEL':'ERROR', 'MSG':f'EXCEPTION={re}' }
            log2(d_log)
            self.connected = False

        return self.connected
    
    def read_config(self, dlgid='00000')->dict:
        ''' Lee de la REDIS la configuracion del dlgid '''
        if dlgid is None:
            d_log = { 'MODULE':__name__, 'FUNTION':'read_config', 'LEVEL':'ERROR', 'MSG':'DLGID None' }
            log2(d_log)
            return { 'STATUS':False,'REASON':'DLGID None','CONTENT':None }
        #
        if not self.connect():
            d_log = { 'MODULE':__name__, 'FUNTION':'get_config', 'LEVEL':'ERROR',
                     'DLGID':dlgid,'MSG':'REDIS LOCAL NOT CONNECTED !!' }
            log2(d_log)
            return { 'STATUS':False,'REASON':'REDIS NOT CONNECTED','CONTENT':None }
        #
        # Hay registro ( HASH ) del datalogger:
        pdict = self.rh.hget(dlgid, 'PKCONFIG')
        if pdict:
            return { 'STATUS':True,'REASON':'OK','CONTENT':pdict }
        #
        return { 'STATUS':False,'REASON':'NO CONFIG RCRD.','CONTENT':None }

    def read_debug_dlgid(self)->dict:
        ''' Retorna el ID del equipo configurado para el maximo log '''
        #
        if not self.connect():
            d_log = { 'MODULE':__name__, 'FUNTION':'read_debug_dlgid', 'LEVEL':'ERROR',
                     'MSG':'REDIS LOCAL NOT CONNECTED !!' }
            log2(d_log)
            return { 'STATUS':False,'REASON':'REDIS NOT CONNECTED','CONTENT':None }
        #
        # Hay registro ( HASH ) del datalogger:
        debug_dlgid = self.rh.hget('SPCOMMS', 'DEBUG_DLGID')
        if debug_dlgid is None:
            d_log = { 'MODULE':__name__, 'FUNTION':'read_debug_dlgid', 'LEVEL':'ERROR',
                     'MSG':'ERROR in HGET: No record. !!' }
            log2(d_log)
            return { 'STATUS':False,'REASON':'NO DEBUG_DLGID.','CONTENT':None }
        
        return { 'STATUS':True,'REASON':'OK','CONTENT':debug_dlgid.decode() }
    
    def read_ordenes(self, dlgid='00000')->dict:
        ''' Todas las ordenes se escriben en la key ORDERS, inclusive el RESET !! '''
        #
        if not self.connect():
            d_log = { 'MODULE':__name__, 'FUNTION':'read_ordenes', 'LEVEL':'ERROR',
                     'DLGID':dlgid,'MSG':'REDIS LOCAL NOT CONNECTED !!' }
            log2(d_log)
            return { 'STATUS':False,'REASON':'REDIS NOT CONNECTED','CONTENT':None }
        #
        ordenes = self.rh.hget(dlgid, 'ORDENES' )
        if ordenes is None:
            d_log = { 'MODULE':__name__, 'FUNTION':'read_ordenes', 'LEVEL':'SELECT',
                     'DLGID':dlgid,'MSG':'WARN in HGET: No record. !!' }
            log2(d_log)
            return { 'STATUS':False,'REASON':'NO ORDENES RCD.','CONTENT':None }
        
        return { 'STATUS':True,'REASON':'OK','CONTENT':ordenes.decode() }
 
    def read_dlgid_from_uid(self, uid='0123456789')->dict:
        ''' 
        Busca el DLGID asociado al UID
        En la redis tenemos un HASH llamado UID2DLGID donde cada clave es un
        uid y el valor asociado el dlgid
        '''
        if not self.connect():
            d_log = { 'MODULE':__name__, 'FUNTION':'read_dlgid_from_uid', 'LEVEL':'ERROR',
                     'MSG':'REDIS LOCAL NOT CONNECTED !!' }
            log2(d_log)
            return { 'STATUS':False,'REASON':'REDIS NOT CONNECTED','CONTENT':None }
        
        #
        # Hay registro ( HASH ) del datalogger:
        dlgid = self.rh.hget('UID2DLGID', uid)
        if dlgid is None:
            d_log = { 'MODULE':__name__, 'FUNTION':'read_dlgid_from_uid', 'LEVEL':'ERROR',
                     'MSG':'ERROR in HGET: No record. !!' }
            log2(d_log)
            return { 'STATUS':False,'REASON':'NO UID RCD.','CONTENT':None }
        
        return { 'STATUS':True,'REASON':'OK','CONTENT':dlgid.decode() }

    def read_queue_length(self, queue_name='STATS_QUEUE')->dict:
        ''' Retorna el largo de la cola '''
        #
        if not self.connect():
            d_log = { 'MODULE':__name__, 'FUNTION':'read_queue_length', 'LEVEL':'ERROR',
                     'MSG':'REDIS LOCAL NOT CONNECTED !!' }
            log2(d_log)
            return { 'STATUS':False,'REASON':'REDIS NOT CONNECTED','CONTENT':None }
        #
        queue_length = self.rh.llen(queue_name)
        if queue_length is None:
            d_log = { 'MODULE':__name__, 'FUNTION':'read_queue_length', 'LEVEL':'ERROR',
                     'MSG':'ERROR in HGET: No record. !!' }
            log2(d_log)
            return { 'STATUS':False,'REASON':'NO QUEUE.','CONTENT':None }
        
        return { 'STATUS':True,'REASON':'OK','CONTENT':queue_length }
       
    def delete_entry(self, dlgid='00000')->dict:
        ''' Borra el registro de la redis local !! '''
        if not self.connect():
            d_log = { 'MODULE':__name__, 'FUNTION':'delete_entry', 'LEVEL':'ERROR',
                     'MSG':'REDIS LOCAL NOT CONNECTED !!' }
            log2(d_log)
            return { 'STATUS':False,'REASON':'REDIS NOT CONNECTED','CONTENT':None }
        #
        _=self.rh.delete(dlgid)
        return { 'STATUS':True,'REASON':'OK','CONTENT':'' }

    def save_data_line(self, dlgid='00000', pkline='')->dict:
        '''
        Guarda la linea serializada con la clave PKLINE y 
        lo encola para procesar luego para la SQL
        '''
        #
        if not self.connect():
            d_log = { 'MODULE':__name__, 'FUNTION':'save_data_line', 'LEVEL':'ERROR',
                     'MSG':'REDIS LOCAL NOT CONNECTED !!' }
            log2(d_log)
            return { 'STATUS':False,'REASON':'REDIS NOT CONNECTED','CONTENT':None }
        #
        # La insercion nunca da errores porque si no existen las claves se crean
        _ = self.rh.hset(dlgid,'PKLINE', pkline)
        _ = self.rh.rpush( 'RXDATA_QUEUE', pkline)
        return { 'STATUS':True,'REASON':'OK','CONTENT':'' }
        #
    
    def save_config(self, dlgid='00000', pkconf='')->dict:
        ''' Guarda una configuracion serializada en la key CONFIG '''
        #
        if not self.connect():
            d_log = { 'MODULE':__name__, 'FUNTION':'save_config', 'LEVEL':'ERROR',
                     'MSG':'REDIS LOCAL NOT CONNECTED !!' }
            log2(d_log)
            return { 'STATUS':False,'REASON':'REDIS NOT CONNECTED','CONTENT':None }
        #
        # La insercion nunca da errores porque si no existen las claves se crean
        _ = self.rh.hset(dlgid,'PKCONFIG', pkconf)
        return { 'STATUS':True,'REASON':'OK','CONTENT':'' }

    def save_dlgid_uid(self, dlgid='00000', uid='0123456789')->dict:
        ''' Guarda (sobreescribe) el par dlgid,uid en la tabla UID2DLGID '''
        #
        if not self.connect():
            d_log = { 'MODULE':__name__, 'FUNTION':'save_dlgid_uid', 'LEVEL':'ERROR',
                     'MSG':'REDIS LOCAL NOT CONNECTED !!' }
            log2(d_log)
            return { 'STATUS':False,'REASON':'REDIS NOT CONNECTED','CONTENT':None }
        #
        # La insercion nunca da errores porque si no existen las claves se crean
        _ =  self.rh.hset('UID2DLGID', uid, dlgid)  
        return { 'STATUS':True,'REASON':'OK','CONTENT':'' }        
  
    def save_stats(self, pkstats='')->dict:
        '''
        Guarda la linea (serializada) con la clave PKLINE y 
        lo encola para procesar luego para la SQL
        '''
        #
        if not self.connect():
            d_log = { 'MODULE':__name__, 'FUNTION':'save_stats', 'LEVEL':'ERROR',
                     'MSG':'REDIS LOCAL NOT CONNECTED !!' }
            log2(d_log)
            return { 'STATUS':False,'REASON':'REDIS NOT CONNECTED','CONTENT':None }
        #
        # La insercion nunca da errores porque si no existen las claves se crean
        _ = self.rh.rpush('STATS_QUEUE', pkstats)
        return { 'STATUS':True,'REASON':'OK','CONTENT':'' }
        #

    def save_timestamp(self, dlgid='00000', pktimestamp='')->dict:
        '''
        Guarda la linea (serializada) con la clave PKTIMESTAMP y 
        lo encola para procesar luego para la SQL
        '''
        #
        if not self.connect():
            d_log = { 'MODULE':__name__, 'FUNTION':'save_timestamp', 'LEVEL':'ERROR',
                     'MSG':'REDIS LOCAL NOT CONNECTED !!' }
            log2(d_log)
            return { 'STATUS':False,'REASON':'REDIS NOT CONNECTED','CONTENT':None }
        #
        # La insercion nunca da errores porque si no existen las claves se crean
        _ = self.rh.hset('TIMESTAMP', dlgid, pktimestamp )
        return { 'STATUS':True,'REASON':'OK','CONTENT':'' }
        #
    
    def read_pkline(self, dlgid='00000')->dict:
        '''  Retorna la pkline de dlgid dado '''
        #
        if not self.connect():
            d_log = { 'MODULE':__name__, 'FUNTION':'read_pkline', 'LEVEL':'ERROR',
                     'DLGID':dlgid,'MSG':'REDIS LOCAL NOT CONNECTED !!' }
            log2(d_log)
            return { 'STATUS':False,'REASON':'REDIS NOT CONNECTED','CONTENT':None }
        #
        pk_line = self.rh.hget(dlgid,'PKLINE')
        if pk_line is None:
            d_log = { 'MODULE':__name__, 'FUNTION':'read_pkline', 'LEVEL':'SELECT',
                     'DLGID':dlgid,'MSG':'WARN in HGET PKLINE: No record. !!' }
            log2(d_log)
            return { 'STATUS':False,'REASON':'NO PKLINE RCD.','CONTENT':None }   
        #         
        if pk_line is None:  
            d_log = { 'MODULE':__name__, 'FUNTION':'read_pkline', 'LEVEL':'SELECT',
                     'DLGID':dlgid,'MSG':'WARN in HGET: No record. !!' }
            log2(d_log)
            return { 'STATUS':False,'REASON':'NO VALUE RCD.','CONTENT':None }
        #
        return { 'STATUS':True,'REASON':'OK','CONTENT': pk_line }

    def read_pkatvise(self,dlgid='00000')->dict:
        '''  Retorna las ordenes de atvise serializadas '''
        #
        if not self.connect():
            d_log = { 'MODULE':__name__, 'FUNTION':'read_pkatvise', 'LEVEL':'ERROR',
                     'DLGID':dlgid,'MSG':'REDIS LOCAL NOT CONNECTED !!' }
            log2(d_log)
            return { 'STATUS':False,'REASON':'REDIS NOT CONNECTED','CONTENT':None }
        #
        pk_atvise = self.rh.hget(dlgid,'PKATVISE')
        if pk_atvise is None:
            d_log = { 'MODULE':__name__, 'FUNTION':'read_pkatvise', 'LEVEL':'SELECT',
                     'DLGID':dlgid,'MSG':'WARN in HGET PKATVISE: No record. !!' }
            log2(d_log)
            return { 'STATUS':False,'REASON':'NO PKATVISE RCD.','CONTENT':None }   
        #         
        if pk_atvise is None:  
            d_log = { 'MODULE':__name__, 'FUNTION':'read_pkatvise', 'LEVEL':'SELECT',
                     'DLGID':dlgid,'MSG':'WARN in HGET: No record. !!' }
            log2(d_log)
            return { 'STATUS':False,'REASON':'NO VALUE RCD.','CONTENT':None }
        #
        return { 'STATUS':True,'REASON':'OK','CONTENT': pk_atvise }      


class TestApiRedis:

    def __init__(self):
        self.api = ApiRedis()
        self.dlgid = ''
        self.uid = ''

    def read_config(self):     
        self.dlgid = 'PABLO'
        set_debug_dlgid(self.dlgid)
        endpoint = 'READ_CONFIG'
        params = { 'DLGID':self.dlgid }
        print('* API_REDIS: TEST_READ_CONFIG Start...')  
        response = self.api.process(params=params, endpoint=endpoint)
        print(f'STATUS_CODE={response.status_code()}')
        print(f'REASON={response.reason()}')
        print(f'JSON={response.json()}')
        print('* API_REDIS: TEST_READ_CONFIG End...') 

    def read_debug_dlgid(self):
        endpoint = 'READ_DEBUG_DLGID'
        params = { }
        print('* API_REDIS: READ_DEBUG_DLGID Start...')  
        response = self.api.process(params=params, endpoint=endpoint)
        print(f'STATUS_CODE={response.status_code()}')
        print(f'REASON={response.reason()}')
        print(f'JSON={response.json()}')
        print('* API_REDIS: READ_DEBUG_DLGID End...') 

    def read_ordenes(self):
        self.dlgid = 'PABLO'
        set_debug_dlgid(self.dlgid)
        endpoint = 'READ_ORDENES'
        params = { 'DLGID':self.dlgid }
        print('* API_REDIS: TEST_READ_ORDENES Start...')  
        response = self.api.process(params=params, endpoint=endpoint)
        print(f'STATUS_CODE={response.status_code()}')
        print(f'REASON={response.reason()}')
        print(f'JSON={response.json()}')
        print('* API_REDIS: TEST_READ_ORDENES End...') 

    def read_dlgid_from_uid(self):
        self.uid = '0123456789'
        endpoint = 'READ_DLGID_FROM_UID'
        params = { 'UID':self.uid }
        print('* API_REDIS: TEST_READ_DLGID_FROM_UID Start...')  
        response = self.api.process(params=params, endpoint=endpoint)
        print(f'STATUS_CODE={response.status_code()}')
        print(f'REASON={response.reason()}')
        print(f'JSON={response.json()}')
        print('* API_REDIS: TEST_READ_DLGID_FROM_UID End...')

    def read_queue_length(self):
        endpoint = 'READ_QUEUE_LENGTH'
        params = { 'QUEUE_NAME':'STATS_QUEUE' }
        print('* API_REDIS: TEST_READ_QUEUE_LENGTH Start...')  
        response = self.api.process(params=params, endpoint=endpoint)
        print(f'STATUS_CODE={response.status_code()}')
        print(f'REASON={response.reason()}')
        print(f'JSON={response.json()}')
        print('* API_REDIS: TEST_READ_QUEUE_LENGTH End...')

    def delete_entry(self):
        endpoint = 'DELETE_ENTRY'
        params = { 'DLGID':'PABLO','DKEY':'PABLO_TEST' }
        print('* API_REDIS: TEST_DELETE_ENTRY Start...')  
        response = self.api.process(params=params, endpoint=endpoint)
        print(f'STATUS_CODE={response.status_code()}')
        print(f'REASON={response.reason()}')
        print(f'JSON={response.json()}')
        print('* API_REDIS: TEST_DELETE_ENTRY End...')

    def save_data_line(self):
        self.dlgid = 'PABLO'
        set_debug_dlgid(self.dlgid)
        d_payload =  { 'PA':1.23,'PB':4.56, 'bt':12.65 }
        pkline = pickle.dumps(d_payload)
        endpoint = 'SAVE_DATA_LINE'
        params = { 'DLGID':self.dlgid, 'PKLINE':pkline }
        print('* API_REDIS: TEST_SAVE_DATA_LINE Start...')  
        response = self.api.process(params=params, endpoint=endpoint)
        print(f'STATUS_CODE={response.status_code()}')
        print(f'REASON={response.reason()}')
        print(f'JSON={response.json()}')
        print('* API_REDIS: TEST_SAVE_DATA_LINE End...') 

    def save_config(self):
        d_conf = {('BASE', 'ALMLEVEL'): '10', ('BASE', 'BAT'): 'ON', ('BASE', 'COMMITED_CONF'): '0', ('BASE', 'DIST'): '0', 
                       ('BASE', 'FIRMWARE'): '4.0.0a', ('BASE', 'HW_CONTADORES'): 'OPTO', ('BASE', 'IMEI'): '860585007136848', 
                       ('BASE', 'IPADDRESS'): '0.0.0.0', ('BASE', 'PWRS_HHMM1'): '1800', ('BASE', 'PWRS_HHMM2'): '1440', 
                       ('BASE', 'PWRS_MODO'): '0', ('BASE', 'RESET'): '0', ('BASE', 'SAMPLES'): '1', 
                       ('BASE', 'SIMID'): '8959801615239182186F', ('BASE', 'TDIAL'): '900', ('BASE', 'TIMEPWRSENSOR'): '5', 
                       ('BASE', 'TPOLL'): '30', ('BASE', 'UID'): '42125128300065090117010400000000', ('BAT', 'NAME'): 'bt', 
                       ('BAT', 'OFFSET'): '0', ('C0', 'EDGE'): 'RISE', ('C0', 'MAGPP'): '0.01', ('C0', 'NAME'): 'q0', 
                       ('C0', 'OFFSET'): '0', ('C0', 'PERIOD'): '100', ('C0', 'PWIDTH'): '10', ('C0', 'SPEED'): 'LS', 
                       ('A0', 'IMAX'): '20', ('A0', 'IMIN'): '4', ('A0', 'MMAX'): '10', ('A0', 'MMIN'): '0', 
                       ('A0', 'NAME'): 'HTQ', ('A0', 'OFFSET'): '0', ('A1', 'IMAX'): '20', ('A1', 'IMIN'): '4', 
                       ('A1', 'MMAX'): '10', ('A1', 'MMIN'): '0', ('A1', 'NAME'): 'X', ('A1', 'OFFSET'): '0', 
                       ('M2', 'ADDR'): '2070', ('M2', 'FCODE'): '3', ('M2', 'NAME'): 'QS', ('M2', 'SIZE'): '2', 
                       ('M2', 'TIPO'): 'float', ('M0', 'ADDR'): '2069', ('M0', 'CODEC'): 'C1032', ('M0', 'FCODE'): '3', 
                       ('M0', 'MMAX'): '10', ('M0', 'MMIN'): '0', ('M0', 'NAME'): 'AI0', ('M0', 'NRO_RECS'): '2', 
                       ('M0', 'POW10'): '0', ('M0', 'REG_ADDR'): '2069', ('M0', 'SIZE'): '2', ('M0', 'SLA_ADDR'): '2', 
                       ('M0', 'TIPO'): 'FLOAT', ('M0', 'TYPE'): 'FLOAT', ('M1', 'ADDR'): '2069', ('M1', 'CODEC'): 'C1032', 
                       ('M1', 'FCODE'): '3', ('M1', 'MMAX'): '10', ('M1', 'MMIN'): '0', ('M1', 'NAME'): 'AI0', ('M1', 'NRO_RECS'): '2', 
                       ('M1', 'POW10'): '0', ('M1', 'REG_ADDR'): '2069', ('M1', 'SIZE'): '2', ('M1', 'SLA_ADDR'): '2', 
                       ('M1', 'TIPO'): 'FLOAT', ('M1', 'TYPE'): 'FLOAT'}
        #
        self.dlgid = 'PABLO_TEST'
        set_debug_dlgid(self.dlgid)
        pkconf = pickle.dumps(d_conf)
        endpoint = 'SAVE_CONFIG'
        params = { 'DLGID':self.dlgid, 'PKCONFIG':pkconf }
        print('* API_REDIS: TEST_SAVE_CONFIG Start...')  
        response = self.api.process(params=params, endpoint=endpoint)
        print(f'STATUS_CODE={response.status_code()}')
        print(f'REASON={response.reason()}')
        print(f'JSON={response.json()}')
        print('* API_REDIS: TEST_SAVE_CONFIG End...') 

    def save_timestamp(self):
        self.dlgid = 'PABLO'
        set_debug_dlgid(self.dlgid)
        timestamp = datetime.datetime.now()
        pktimestamp = pickle.dumps(timestamp)
        endpoint = 'SAVE_FRAME_TIMESTAMP'
        params = { 'DLGID':self.dlgid, 'PKTIMESTAMP':pktimestamp }
        print('* API_REDIS: TEST_SAVE_FRAME_TIMESTAMP Start...')  
        response = self.api.process(params=params, endpoint=endpoint)
        print(f'STATUS_CODE={response.status_code()}')
        print(f'REASON={response.reason()}')
        print(f'JSON={response.json()}')
        print('* API_REDIS: TEST_SAVE_FRAME_TIMESTAMP End...') 

if __name__ == "__main__":

    config_logger('CONSOLA')
    import pickle
    import datetime

    # Test api_redis
    print('TESTING API REDIS...')
    test = TestApiRedis()
    #test.read_config()
    #test.read_debug_dlgid()
    #test.read_ordenes()
    #test.read_dlgid_from_uid()
    #test.read_queue_length()
    #test.save_data_line()
    #test.delete_queue()
    #test.delete_entry()
    #test.save_config()
    test.save_timestamp()
    sys.exit(0)
