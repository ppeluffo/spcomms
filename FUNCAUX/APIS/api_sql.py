#!/home/pablo/Spymovil/www/cgi-bin/spcommsV3/venv/bin/python3

'''
Funciones de API de configuracion.
Se utilizan para leer configuracion de equipos de una BD persistente (GDA)
'''

import os
import sys
from sqlalchemy import create_engine
from sqlalchemy import text
import random

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

class ApiBdSql:
    '''
    Interface con la BD SQL.

    La interface que presenta esta normalizada. Todos los datos de entrada
    en un dict y todos los de salida en otro.
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
        self.pgh = __BdSql__()
        self.callback_endpoints =  { 'READ_CONFIG': self.__read_config__, 
                                     'READ_DLGID_FROM_UID': self.__read_dlgid_from_ui__,
                                     'SAVE_DLGID_UID': self.__save_dlgid_uid__
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
        trace_request( endpoint=self.endpoint, params=self.params, msg=f'Input API Sql ({self.tag})')
        # Ejecuto la funcion de callback
        if self.endpoint in self.callback_endpoints:
            # La response la fija la funcion de callback
            self.callback_endpoints[self.endpoint]()
        else:
            # ERROR: No existe el endpoint
            self.response.set_status_code(405)
            self.response.set_reason(f"API Sql: No existe endpoint {endpoint}")
        #
        trace_response( response=self.response, msg=f'Output API Sql ({self.tag})')
        return self.response
   
    def __read_config__(self):
        ''' La api SQL devuelve lo que hay en la BD o sea un dict. !!'''
        dlgid = self.params.get('DLGID','00000')
        d_response = self.pgh.read_config(dlgid)
        #
        self.response.set_dlgid(dlgid)
        if d_response.get('STATUS',False):
            self.response.set_status_code(200)
            self.response.set_reason('OK')
            self.response.set_json( { 'D_CONFIG': d_response.get('CONTENT',{}) } ) 
        else:
            self.response.set_status_code(400)
            self.response.set_reason(d_response.get('REASON','Err'))
            self.response.set_json( { 'D_CONFIG':{} } ) 
    
    def __read_dlgid_from_ui__(self):
        uid = self.params.get('UID','0123456789')
        d_response = self.pgh.read_dlgid_from_uid(uid)
        if d_response.get('STATUS',False):
            self.response.set_status_code(200)
            self.response.set_reason('OK')
            self.response.set_json( { 'DLGID': d_response.get('CONTENT','')})
        else:
            self.response.set_status_code(400)
            self.response.set_reason(d_response.get('REASON','Err'))
            self.response.set_json( { 'DLGID':'' } )
        #

    def __save_dlgid_uid__(self):
        ''' Guardo el par dlgid,uid '''
        dlgid = self.params.get('DLGID','00000')
        uid = self.params.get('UID','0123456789')
        d_response = self.pgh.save_dlgid_uid(dlgid,uid)
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

class __BdSql__:

    def __init__(self):
        self.connected = False
        self.conn = None
        self.handle = None
        self.engine = None
        self.url = Config['BDATOS']['url_gda_spymovil']
        self.response = False

    def connect(self):

        spc_stats.inc_count_accesos_SQL()
        
        if self.connected:
            return True
        # Engine
        try:
            self.engine = create_engine(self.url)
        except Exception as err_var:
            self.connected = False
            log2( { 'MODULE':__name__, 'FUNTION':'connect', 'LEVEL':'ERROR', 'MSG':'ERROR: engine NOT created. ABORT !!' } )
            log2( { 'MODULE':__name__, 'FUNTION':'connect', 'LEVEL':'ERROR', 'MSG':f'ERROR: EXCEPTION {err_var}' } )
            return False
        #
        # Connection
        try:
            self.conn = self.engine.connect()
            self.connected = True
            return True
        except Exception as err_var:
            self.connected = False
            log2( { 'MODULE':__name__, 'FUNTION':'connect', 'LEVEL':'ERROR', 'MSG':'ERROR: BDSPY NOT connected. ABORT !!' } )
            log2( { 'MODULE':__name__, 'FUNTION':'connect', 'LEVEL':'ERROR', 'MSG':f'ERROR: EXCEPTION {err_var}' } )
            return False

    def exec_sql(self, dlgid:str, sql:str):
        '''
        Ejecuta la orden sql.
        Retorna un resultProxy o None
        '''
        if not self.connect():
            log2( { 'MODULE':__name__, 'FUNTION':'exec_sql', 'LEVEL':'ERROR', 'DLGID': dlgid,
                   'MSG':'ERROR: No hay conexion a BD. Exit !!' } )
            return None

        try:
            query = text(sql)
        except Exception as err_var:
            log2( { 'MODULE':__name__, 'FUNTION':'exec_sql', 'LEVEL':'ERROR', 
                   'DLGID':dlgid, 'MSG':f'ERROR: SQLQUERY: {sql}' } )
            log2( { 'MODULE':__name__, 'FUNTION':'exec_sql', 'LEVEL':'ERROR', 
                   'DLGID':dlgid, 'MSG':f'ERROR: EXCEPTION {err_var}' } )
            return

        log2( { 'MODULE':__name__, 'FUNTION':'exec_sql', 'LEVEL':'SELECT', 
                   'DLGID':dlgid, 'MSG':f'QUERY={query}' } )
        rp = None
        try:
            rp = self.conn.execute(query)
        except Exception as err_var:
            return None
        #
        return rp

    def read_config(self, dlgid='00000')->dict:
        '''
        Leo la configuracion desde GDA
                +----------+---------------+------------------------+----------+
                | canal    | parametro     | value                  | param_id |
                +----------+---------------+------------------------+----------+
                | BASE     | RESET         | 0                      |      899 |
                | BASE     | UID           | 304632333433180f000500 |      899 |
                | BASE     | TPOLL         | 60                     |      899 |
                | BASE     | COMMITED_CONF |                        |      899 |
                | BASE     | IMEI          | 860585004331632        |      899 |

                EL diccionario lo manejo con 2 claves para poder usar el metodo get y tener
                un valor por default en caso de que no tenga alguna clave
        '''
        sql = """SELECT spx_unidades_configuracion.nombre as canal, spx_configuracion_parametros.parametro, 
                    spx_configuracion_parametros.value, spx_configuracion_parametros.configuracion_id as \"param_id\" FROM spx_unidades,
                    spx_unidades_configuracion, spx_tipo_configuracion, spx_configuracion_parametros 
                    WHERE spx_unidades.id = spx_unidades_configuracion.dlgid_id 
                    AND spx_unidades_configuracion.tipo_configuracion_id = spx_tipo_configuracion.id 
                    AND spx_configuracion_parametros.configuracion_id = spx_unidades_configuracion.id 
                    AND spx_unidades.dlgid = '{}'""".format(dlgid)
        
        rp = self.exec_sql(dlgid, sql)
        try:
            results = rp.fetchall()
        except AttributeError as ax:
            log2( { 'MODULE':__name__, 'FUNTION':'get_config', 'LEVEL':'ERROR', 'DLGID':dlgid, 'MSG':f'ERROR: AttributeError fetchall: {ax}' } )
            return { 'STATUS':False,'REASON':'AttributeError fetchall','CONTENT':None }
        #
        except Exception as ex:  # good idea to be prepared to handle various fails
            log2( { 'MODULE':__name__, 'FUNTION':'get_config', 'LEVEL':'ERROR', 'DLGID':dlgid, 'MSG':f'ERROR: ExceptionError fetchall: {ex}' } )
            return { 'STATUS':False,'REASON':'ExceptionError fetchall','CONTENT':None }

        d_conf = {}
        log2( { 'MODULE':__name__, 'FUNTION':'get_config', 'LEVEL':'SELECT', 'DLGID':dlgid, 'MSG':'Reading conf from GDA' } )
        for row in results:
            canal, pname, value, *pid = row
            d_conf[(canal, pname)] = value
            log2( { 'MODULE':__name__, 'FUNTION':'get_config', 'LEVEL':'SELECT', 'DLGID':dlgid, 'MSG':f'BD conf: [{canal}][{pname}]=[{d_conf[(canal, pname)]}]' } )
        #
        return { 'STATUS':True,'REASON':'OK','CONTENT': d_conf }

    def read_dlgid_from_uid(self, uid='0123456789')->dict:
        '''
        Consulta en GDA con clave UID para encontrar el DLGID correspondiente
        '''
        sql = f"""SELECT u.dlgid FROM spx_configuracion_parametros AS cp 
            INNER JOIN spx_unidades_configuracion AS uc ON cp.configuracion_id = uc.id 
            INNER JOIN spx_unidades AS u ON u.id = uc.dlgid_id 
            WHERE uc.nombre = 'BASE' AND cp.parametro = 'UID' AND value = '{uid}';"""

        #
        rp = self.exec_sql('DEFAULT', sql)
        try:
            results = rp.fetchall()
        except AttributeError as ax:
            log2( { 'MODULE':__name__, 'FUNTION':'get_dlgid_from_uid', 'LEVEL':'ERROR', 'MSG':f'ERROR: AttributeError fetchall: {ax}' } )
            return { 'STATUS':False,'REASON':'AttributeError fetchall','CONTENT':None }
        #
        except Exception as ex:  # good idea to be prepared to handle various fails
            log2( { 'MODULE':__name__, 'FUNTION':'get_dlgid_from_uid', 'LEVEL':'ERROR', 'MSG':f'ERROR: Exception fetchall: {ex}' } )
            return { 'STATUS':False,'REASON':'ExceptionError fetchall','CONTENT':None }
        #
        log2( { 'MODULE':__name__, 'FUNTION':'get_dlgid_from_uid', 'LEVEL':'INFO', 'MSG':'Reading dlgid_from_uid in SQL.' } )
        if len(results) == 0:
            return { 'STATUS':False,'REASON':'NO UID RCD.','CONTENT':None }
            
        dlgid = results[0][0]
        log2( { 'MODULE':__name__, 'FUNTION':'get_dlgid_from_uid', 'LEVEL':'INFO', 'MSG':f'UID:{uid}, DLGID:{dlgid}' } )
        return { 'STATUS':True,'REASON':'OK','CONTENT':dlgid }

    def save_dlgid_uid(self, dlgid='00000', uid='0123456789')->dict:
        ''' Actualiza '''
        return { 'STATUS':True,'REASON':'OK','CONTENT':'' }
    
class TestApiBdSql:

    def __init__(self):
        self.api = ApiBdSql()
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

if __name__ == "__main__":

    config_logger('CONSOLA')

    # Test api_redis
    print('TESTING API SQL...')
    test = TestApiBdSql()
    test.read_config()
    #test.read_dlgid_from_uid()
    sys.exit(0)
   

    
