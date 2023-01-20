#!/opt/anaconda3/envs/mlearn/bin/python3
# !/usr/bin/python3 -u

import numpy as np
from sqlalchemy import create_engine
from sqlalchemy import text
from FUNCAUX.UTILS.spc_config import Config
from FUNCAUX.UTILS.spc_log import log


class BD_ATVISE:

    def __init__(self):
        self.connected = False
        self.conn = None
        self.handle = None
        self.engine = None
        self.url = Config['ATVISEDB']['url_atvisedb']
        #self.url = "postgresql+psycopg2://admin:pexco599@192.168.0.6:5433/atvisedb"
        self.response = False

    def connect(self):
        if self.connected:
            return True
        # Engine
        try:
            self.engine = create_engine(self.url)
        except Exception as err_var:
            self.connected = False
            log(module=__name__, function='connect', level='ERROR', msg='ERROR: engine NOT created. ABORT !!')
            log(module=__name__, function='connect', level='ERROR', msg='ERROR: EXCEPTION {0}'.format(err_var))
            return False

        # Connection
        try:
            self.conn = self.engine.connect()
            self.connected = True
            return True
        except Exception as err_var:
            self.connected = False
            log(module=__name__, function='connect', level='ERROR', msg='ERROR: BDSPY NOT connected. ABORT !!')
            log(module=__name__, function='connect', level='ERROR', msg='ERROR: EXCEPTION {0}'.format(err_var))
            return False

    def exec_sql(self, dlgid, sql):
        # Ejecuta la orden sql.
        # Retorna un resultProxy o None

        if not self.connect():
            log(module=__name__, function='exec_sql', level='ERROR', dlgid=dlgid, msg='ERROR: No hay conexion a BD. Exit !!')
            return None

        #log(module=__name__, function='exec_sql', level='ERROR', dlgid=dlgid, msg='SQLQUERY: {0}'.format(sql))

        try:
            query = text(sql)
        except Exception as err_var:
            log(module=__name__, function='exec_sql', level='ERROR', dlgid=dlgid, msg='ERROR: SQLQUERY: {0}'.format(sql))
            log(module=__name__, function='exec_sql', level='ERROR', dlgid=dlgid, msg='ERROR: EXCEPTION {0}'.format(err_var))
            return

        #log(module=__name__, function='exec_sql', level='SELECT', dlgid=dlgid, msg='QUERY={0}'.format(query))
        rp = None
        try:
            rp = self.conn.execute(query)
        except Exception as err_var:
            '''
            if 'duplicate'.lower() in (str(err_var)).lower():
                # Los duplicados no hacen nada malo. Se da mucho en testing.
                log(module=__name__, function='exec_sql', level='WARN', dlgid=dlgid, msg='WARN {0}: Duplicated Key'.format(dlgid))
            else:
                log(module=__name__, function='exec_sql', level='ERROR', dlgid=dlgid, msg='ERROR: {0}, EXCEPTION {1}'.format(dlgid, err_var))
            '''
            return None
        return rp

    def read_plc_conf(self, dlgid):
        '''
        Leo la configuracion desde PGSQL DB ATVISE
        La configuracion es un diccionario (json) y cada clave tiene la informacion necesaria.
        Inicialmente tiene 2 claves: REENVIOS, MEMBLOCK
        La tupla que leo tiene los campos dlgid,json, etc.
        '''
        log(module=__name__, function='read_plc_conf', level='SELECT', dlgid=dlgid, msg='start')
        sql = "SELECT * FROM db_equipo WHERE equipo='{}';".format(dlgid)
        rp = self.exec_sql(dlgid, sql)
        try:
            results = rp.fetchone()
        except AttributeError as ax:
            log(module=__name__, function='read_plc_conf', level='ERROR', msg='AttributeError fetchone: {0}'.format(ax))
            return None
        except Exception as ex:  # good idea to be prepared to handle various fails
            log(module=__name__, function='read_dlg_conf', level='ERROR', msg='Exception fetchone: {0}'.format(ex))
            return None

        if results is not None:
            d = results[1]
            log(module=__name__, function='read_plc_conf', dlgid=dlgid, level='SELECT', msg='D={0}'.format(d))
            return d
        else:
            return None

    def insert_plcR2_data(self, d:dict):
        '''
        Recibe un diccionario donde la clave 'ID' es el dlgid.
        El resto de las claves son los nombres de las variables.
        Retorna un resultProxy o None
        Data es una lista de diccionarios.
        '''
        dlgid=d.get('ID','SPY000')
        if 'ID' in d:
            d.pop('ID')
        #log(module=__name__, function='insert_plcR2_data', level='SELECT', dlgid=dlgid, msg='Start')
        for key in d:
            # Inserta cada par key_value
            value = d.get(key, 'None')
            if value != 'None':
                try:
                    fvalue = float(value)
                except:
                    fvalue = np.NaN

            sql = """INSERT INTO db_datos (fechadata, equipo, tag, valor) VALUES ( NOW(),'{0}', '{1}', '{2}')""".format(dlgid, key, fvalue)
            #
            result = self.exec_sql(dlgid, sql)
            if result is None:
                log(module=__name__, function='insert_plcR2_data', level='ERROR', dlgid=dlgid, msg='INSERT PG_ATVISE FAIL.')
        #
        return True