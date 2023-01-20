#!/opt/anaconda3/envs/mlearn/bin/python3
#!/usr/bin/python3 -u

'''
La configuracion en REDIS va en un HASH con la estructura:
'PABLO':{ 'RESET':True,'PSLOT':-1,'DATASOURCE':'ose','LINE':'....','MODBUS':NULL }
'324572109652':{ 'DLGID':'PABLO'}

Los inits ( Frames AUTH ) van a quedar en una lista: L_INITS
Los frames de datos quedan en otra lista L_DATOS
'''
import redis
import pickle
from FUNCAUX.UTILS import spc_stats as stats
from FUNCAUX.UTILS.spc_config import Config
from FUNCAUX.UTILS.spc_log import log
from FUNCAUX.BD.spc_bd_gda import BD_GDA
from FUNCAUX.BD.spc_bd_atvise import BD_ATVISE


class BD_REDIS:

    def __init__(self):
        self.connected = False
        self.handle = None

    def connect(self):
        '''
        Cada acceso primero verifica si esta conectada asi que aqui incrementamos el contador de accesos.
        '''
        if self.connected:
            stats.inc_count_accesos_REDIS()
            return True

        try:
            self.handle = redis.Redis(
                host=Config['REDIS']['host'], port=Config['REDIS']['port'], db=Config['REDIS']['db'])
            self.connected = True
            stats.inc_count_accesos_REDIS()
        except Exception as err_var:
            log(module=__name__, function='connect',
                level='ERROR', msg='Init ERROR !!')
            log(module=__name__, function='connect',
                level='ERROR', msg='EXCEPTION={}'.format(err_var))
            stats.inc_count_errors()
            self.connected = False

        return self.connected

    def init_sysvars_record(self, dlgid):
        '''
        Inicializa el registro del DLGID Solo si no existe !!!!
        '''
        if not self.connect():
            stats.inc_count_errors()
            return False

        # Guarda la ultima linea de datos recibida
        if not self.handle.hexists(dlgid, 'LINE'):
            self.handle.hset(dlgid, 'LINE', 'NUL')
        # Guarda un dict serializado con los datos de la ultima linea
        if not self.handle.hexists(dlgid, 'PKLINE'):
            self.handle.hset(dlgid, 'PKLINE', 'NUL')
        # Guarda un dict serializado con los datos de las variables remotas
        if not self.handle.hexists(dlgid, 'REMVARS'):
            self.handle.hset(dlgid, 'REMVARS', 'NUL')
        # Guarda el memblock serializado.
        if not self.handle.hexists(dlgid, 'MEMBLOCK'):
            self.handle.hset(dlgid, 'MEMBLOCK', 'NUL')
        # Ordenes del Atvise al PLC
        if not self.handle.hexists(dlgid, 'ATVISE'):
            self.handle.hset(dlgid, 'ATVISE', 'NUL')

        # Guarda el serializado de un diccionario de remotos.
        if not self.handle.hexists(dlgid, 'REENVIOS'):
            self.handle.hset(dlgid, 'REENVIOS', 'NUL')
        # Comandos a enviar por el SPX para controlar las salidas
        if not self.handle.hexists(dlgid, 'OUTPUTS'):
            self.handle.hset(dlgid, 'OUTPUTS', '-1')
        # Indica al SPX que debe resetearse
        if not self.handle.hexists(dlgid, 'RESET'):
            self.handle.hset(dlgid, 'RESET', 'FALSE')
        # Valores de presion a mandar a un DLG c/piloto
        if not self.handle.hexists(dlgid, 'POUT'):
            self.handle.hset(dlgid, 'POUT', '-1')
        # Valores de timeslots a mandar a un DLG c/pilotos
        if not self.handle.hexists(dlgid, 'PSLOT'):
            self.handle.hset(dlgid, 'PSLOT', '-1')
        # Indica al SPX que debe resetear su memoria
        if not self.handle.hexists(dlgid, 'MEMFORMAT'):
            self.handle.hset(dlgid, 'MEMFORMAT', 'FALSE')
        # Comandos modbus para ejecutar en el SPX
        if not self.handle.hexists(dlgid, 'MODBUS'):
            self.handle.hset(dlgid, 'MODBUS', 'NUL')
        # Linea con comandos modbus a ejecutar el SPX/PLC
        if not self.handle.hexists(dlgid, 'BROADCAST'):
            self.handle.hset(dlgid, 'BROADCAST', 'NUL')
        # Guarda lista de dlg remotos en automatismos
        if not self.handle.hexists(dlgid, 'DLGREMOTOS'):
            self.handle.hset(dlgid, 'DLGREMOTOS', 'NUL')
        # Validez del registro. Si no es valido, todo se debe
        if not self.handle.hexists(dlgid, 'VALID'):
            # releer de BD persistente y recrear el registro
            self.handle.hset(dlgid, 'VALID', "TRUE")
        if not self.handle.hexists(dlgid, 'CONFIG'):            #
            self.handle.hset(dlgid, 'CONFIG', 'NUL')
        if not self.handle.hexists(dlgid, 'UID'):               #
            self.handle.hset(dlgid, 'UID', 'NUL')

        return True

    def save_statistics(self, pkdict):
        '''
        Guarda un diccionario de estadisticas de procesamiento del frame en una lista de REDIS
        '''
        if not self.connect():
            stats.inc_count_errors()
            return False

        # self.handle.rpush('LQUEUE_STATS', pkdict)
        return True

    def invalidate_record(self, dlgid):
        if not self.connect():
            stats.inc_count_errors()
            return False

        self.handle.hset(dlgid, 'VALID', "False")

    def enqueue_data_record(self, dlgid, d, queue_name='LQ_PLCDATA'):
        '''
         Encola en LQ_PLCDATA un pickle con los datos.
         Luego un proceso se encargara de generar un INIT record en GDA

         Si la cola LQ_PLCDATA no existe, con el compando rpush se crea automaticamente

         2023-01-04: Guardo el pickle en un registro 'PKDATA' del datalogger que los envio.
         '''
        if not self.connect():
            stats.inc_count_errors()
            return False

        pkdict = pickle.dumps(d)

        # Si no hay registro ( HASH ) del datalogger lo creo.
        if not self.handle.exists(dlgid):
            self.init_sysvars_record(dlgid)
            return False

        # Si no esta creado el registro PKLINE, lo creo.(inicializo)
        if not self.handle.hexists(dlgid, 'PKLINE'):
            log(module=__name__, function='enqueue_data_record', level='ERROR',
                dlgid=dlgid, msg='REDIS ERROR: No existe key PKLINE')
            self.init_sysvars_record(dlgid)
            return False

        # Guardo el dict de las variables serializado
        try:
            self.handle.hset(dlgid, 'PKLINE', pkdict)
        except Exception as err_var:
            log(module=__name__, function='save_data_record', level='ERROR',
                msg='{0}: Save PKLINE ERROR !!'.format(queue_name))
            log(module=__name__, function='save_data_record', level='ERROR',
                msg='{0}: EXCEPTION={1}'.format(queue_name, err_var))
            stats.inc_count_errors()

        try:
            if self.handle.rpush(queue_name, pkdict):
                #log(module=__name__, function='enqueue_data_record', level='ERROR', msg='REDIS ENQUEUE OK {0}{1}'.format(Config['REDIS']['host'], pkdict ))
                return True
        except Exception as err_var:
            log(module=__name__, function='save_data_record',
                level='ERROR', msg='{0}: Init ERROR !!'.format(queue_name))
            log(module=__name__, function='save_data_record', level='ERROR',
                msg='{0}: EXCEPTION={1}'.format(queue_name, err_var))
            stats.inc_count_errors()
        return False

    def get_modbusline(self, dlgid, clear=True):
        '''
        En el campo MODBUS están los comandos que mando de respuesta al PLC
        El formato actual es del tipo [2223,F,12.430][1234,I,245].....
        Esta hecho para que un datalogger genere los comandos modbus necesarios pero el PLC solo necesita el campo
        address y valor
        [add,valor][addr,valor].......
        Una vez que envio la respuesta pongo el campo en NUL.
        '''
        response = ''

        # log(module=__name__, function='get_orders2plc', dlgid=dlgid, msg='REDIS {0}'.format(dlgid))

        if not self.connect():
            log(module=__name__, function='get_modbusline',
                level='ERROR', dlgid=dlgid, msg='REDIS NOT CONNECTED')
            stats.inc_count_errors()
            return response

        # Si no hay registro ( HASH ) del datalogger lo creo.
        if not self.handle.exists(dlgid):
            self.init_sysvars_record(dlgid)
            return response

        # Si no esta creado el registro MODBUS, lo creo.(inicializo)
        if not self.handle.hexists(dlgid, 'MODBUS'):
            log(module=__name__, function='get_modbusline', level='ERROR',
                dlgid=dlgid, msg='REDIS ERROR: No existe key MODBUS')
            self.init_sysvars_record(dlgid)
            return response

        try:
            modbus_line = self.handle.hget(dlgid, 'MODBUS').decode()
        except:
            log(module=__name__, function='get_modbusline', level='ERROR',
                dlgid=dlgid, msg='ERROR in HGET{0}'.format(dlgid))
            stats.inc_count_errors()
            return response

        log(module=__name__, function='get_modbusline', level='SELECT',
            dlgid=dlgid, msg='MODBUS={}'.format(modbus_line))
        if modbus_line != 'NUL':
            l1 = modbus_line.replace('][', ';')
            l1 = l1.replace('[', '')
            l1 = l1.replace(']', '')
            l2 = [(a, c) for (a, b, c) in [x.split(',')
                                           for x in l1.split(';')]]
            for i, j in l2:
                response += '{0}:{1};'.format(i, j)
            # Elimino todos los caracteres vacios que pudiesen haber...
            response = response.replace(' ', '')
            log(module=__name__, function='get_modbusline', level='SELECT',
                dlgid=dlgid, msg='MODBUS_CMDS={}'.format(response))
        if clear:
            self.handle.hset(dlgid, 'MODBUS', 'NUL')

        return response

    def get_bcastline(self, dlgid):
        '''
        En el campo BROADCAST está los comandos que mando de respuesta al PLC
        El formato actual es del tipo [2,2223,2,16,FLOAT,C1032,0][3,1234,5,16,FLOAT,C3210,7.34].....
        Esta hecho para que un datalogger genere los comandos modbus necesarios pero el PLC solo necesita el campo
        address y valor
        [add,valor][addr,valor].......
        NO BORRO EL CAMPO BROADCAST.
        '''
        response = ''

        # log(module=__name__, function='get_orders2plc', dlgid=dlgid, msg='REDIS {0}'.format(dlgid))

        if not self.connect():
            log(module=__name__, function='get_bcastline',
                level='ERROR', dlgid=dlgid, msg='REDIS NOT CONNECTED')
            stats.inc_count_errors()
            return response

        # Si no hay registro ( HASH) del datalogger lo creo.
        if not self.handle.exists(dlgid):
            self.init_sysvars_record(dlgid)
            return response

        if not self.handle.hexists(dlgid, 'BROADCAST'):
            self.init_sysvars_record(dlgid)
            log(module=__name__, function='get_bcastline', level='ERROR',
                dlgid=dlgid, msg='REDIS ERROR: No existe key BROADCAST')
            return response

        try:
            bcast_line = self.handle.hget(dlgid, 'BROADCAST').decode()
        except:
            log(module=__name__, function='get_bcastline', level='ERROR',
                dlgid=dlgid, msg='ERROR in HGET{0}'.format(dlgid))
            stats.inc_count_errors()
            return response

        log(module=__name__, function='get_bcastline', level='SELECT',
            dlgid=dlgid, msg='BCAST:{}'.format(bcast_line))
        if bcast_line != 'NUL':
            l1 = bcast_line.replace('][', ';')
            l1 = l1.replace('[', '')
            l1 = l1.replace(']', '')
            l2 = [(int(b), float(g)) for (a, b, c, d, e, f, g)
                  in [x.split(',') for x in l1.split(';')]]
            for i, j in l2:
                response += '{0}:{1};'.format(i, j)
            # Elimino todos los caracteres vacios
            response = response.replace(' ', '')
            log(module=__name__, function='get_bcastline', level='SELECT',
                dlgid=dlgid, msg='PLC_CMDS:{}'.format(response))

        return response

    def read_lqueue_length(self, queue_name):
        if not self.connect():
            stats.inc_count_errors()
            return False

        return self.handle.llen(queue_name)

    def lpop_lqueue(self, queue_name, size):
        if not self.connect():
            stats.inc_count_errors()
            return False
        lines = []
        try:
            lines = self.handle.lpop(queue_name, size)
        except Exception as err_var:
            log(module=__name__, function='lpop_lqueue', level='ERROR',
                msg='ERROR Queue={0} !!'.format(queue_name))
            log(module=__name__, function='lpop_lqueue', level='ERROR',
                msg='Queue={0}, EXCEPTION={1}'.format(queue_name, err_var))
            stats.inc_count_errors()
        return lines

    def save_line(self, dlgid, line):
        # Guardo la ultima linea en la redis porque la uso para los automatismos
        if not self.connect():
            log(module=__name__, function='save_line', level='ERROR',
                dlgid=dlgid, msg='REDIS NOT CONNECTED')
            stats.inc_count_errors()
            return False

        line = 'LINE=' + line
        try:
            self.handle.hset(dlgid, 'LINE', line)
            return True
        except Exception as err_var:
            log(module=__name__, function='save_line',
                dlgid=dlgid, msg='ERROR: Redis insert line err !!')
            log(module=__name__, function='save_line', dlgid=dlgid,
                msg='ERROR: EXCEPTION={}'.format(err_var))
            stats.inc_count_errors()
            return False

    def get_d_reenvios(self, dlgid):
        '''
        Lee el diccionario de reenvios de medidas a dataloggers remotos.
        '''
        if not self.connect():
            log(module=__name__, function='get_d_reenvios',
                level='ERROR', dlgid=dlgid, msg='REDIS NOT CONNECTED')
            stats.inc_count_errors()
            return None

        # Si no hay registro (HASH) del datalogger lo creo.
        if not self.handle.exists(dlgid):
            self.init_sysvars_record(dlgid)
            return None

        if not self.handle.hexists(dlgid, 'REENVIOS'):
            self.init_sysvars_record(dlgid)
            log(module=__name__, function='get_d_reenvios', level='SELECT', dlgid=dlgid, msg='REDIS ERROR: No existe key REENVIOS')
            return None

        try:
            pk_line = self.handle.hget(dlgid, 'REENVIOS')
            d_reenvios = pickle.loads(pk_line)
        except:
            log(module=__name__, function='get_d_reenvios', level='ERROR', dlgid=dlgid, msg='REDIS ERROR in HGET {0}'.format(dlgid))
            stats.inc_count_errors()
            return None

        # log(module=__name__, function='get_d_reenvios', level='SELECT', dlgid=dlgid, msg='REDIS D_REENVIOS={}'.format(d_reenvios))
        return d_reenvios

    def set_d_reenvios(self, dlgid, d_reenvios):
        '''
        Guarda en la REDIS el diccionario de reenvios serializado
        '''
        if not self.connect():
            log(module=__name__, function='set_d_reenvios', level='ERROR', dlgid=dlgid, msg='REDIS NOT CONNECTED')
            stats.inc_count_errors()
            return False

        # Si no hay registro (HASH) del datalogger lo creo.
        if not self.handle.exists(dlgid):
            log(module=__name__, function='set_d_reenvios', level='ERROR', dlgid=dlgid, msg='No existe HASH.Se crea.')
            self.init_sysvars_record(dlgid)

        pkline = pickle.dumps(d_reenvios)

        try:
            self.handle.hset(dlgid, 'REENVIOS', pkline)
            return True
        except Exception as err_var:
            log(module=__name__, function='save_line', dlgid=dlgid, msg='ERROR: Redis insert line err !!')
            log(module=__name__, function='save_line', dlgid=dlgid, msg='ERROR: EXCEPTION={}'.format(err_var))
            stats.inc_count_errors()
            return False

    def exist_entry(self, dlgid):
        '''
        Verifica que exista la entrada en Redis.
        '''
        if not self.connect():
            stats.inc_count_errors()
            return False

        if self.handle.hexists(dlgid, 'CONFIG'):
            log(module=__name__, function='exist_entry', level='ALERT', dlgid=dlgid, msg='REDIS {0} entry exists'.format(dlgid))
            return True
        else:
            return False

    def create_entry(self, dlgid):
        '''
        Crea la entrada en Redis.
        '''
        log(module=__name__, function='create_entry', level='ALERT', dlgid=dlgid, msg='CREATING REDIS {0} entry'.format(dlgid))
        retS = self.init_sysvars_record(dlgid)
        if retS:
            return True
        else:
            return False

    def update_config(self, dlgid):
        '''
        Guarda la configuracion en modo pickle con la key CONFIG
        '''
        if self.exist_entry(dlgid):
            gdah = BD_GDA()
            pdict = gdah.get_pickle_conf_from_gda(dlgid)
            if pdict is None:
                return False
            #
            self.handle.hset(dlgid, 'CONFIG', pdict)
            self.handle.hset(dlgid, 'VALID', "TRUE")
            return True

    def get_config(self, dlgid):
        '''
        Recupera la clave dlgid, des-serializa y obtiene el diccionario con la configuracion
        '''
        pdict = self.handle.hget(dlgid, 'CONFIG')
        if pdict is not None:
            dconf = pickle.loads(pdict)
        else:
            dconf = None
        return dconf

    def get_debug_dlgid(self):
        # Leo de la redis el dlgid que debo loguer en modo SELECT
        if not self.connect():
            log(module=__name__, function='get_debug_dlgid', level='ERROR', msg='REDIS NOT CONNECTED')
            stats.inc_count_errors()
            return None

        # Si no hay registro ( HASH) del datalogger lo creo.
        if not self.handle.exists('SPCOMMS', 'DEBUG_DLGID'):
            # Lo creo
            self.handle.hset('SPCOMMS', 'DEBUG_DLGID', 'None')
            return None
        else:
            try:
                debug_dlgid = self.handle.hget('SPCOMMS', 'DEBUG_DLGID')
            except:
                log(module=__name__, function='get_debug_dlgid', level='ERROR', msg='ERROR in HGET')
                stats.inc_count_errors()
                return None

        #log(module=__name__, function='get_debug_dlgid', level='ERROR', msg='DEBUG_DLGID:{0}'.format(debug_dlgid))
        return debug_dlgid

    def check_config_valid(self, dlgid):
        '''
        Verfica que exista en REDIS un registro de configuracion del dlgid
        con datos valido.
        Si no existe trata de crearlo y carga la configuracion de GDA.
        Si existe per la key VALID es False, descartamos y recargamos la configuracion
        '''
        if not self.exist_entry(dlgid):
            # Creo la entrada
            if not self.create_entry(dlgid):
                return False
            # Actualizo
            if not self.update_config(dlgid):
                return False

        valid_conf = False
        if not self.handle.hexists(dlgid, 'VALID'):
            # Existe la entrada. Vemos si la actual configuracion es valida
            self.handle.hset(dlgid, 'VALID', "FALSE")

        valid_flag = self.handle.hget(dlgid, 'VALID')
        if valid_flag == b'TRUE':
            dconf = self.get_config(dlgid)
            return dconf
        else:
            if not self.update_config(dlgid):
                return False
        #
        dconf = self.get_config(dlgid)
        return dconf

    def get_reset_order(self, dlgid):
        '''
        Si existe la key RESET en la redis, devuleve su valor
        '''
        if self.handle.hexists(dlgid, 'RESET'):
            s_rsp = self.handle.hget(dlgid, 'RESET')
            #print(f'REDIS RSP: {s_rsp}')

        if s_rsp == b'FALSE':
            return False
        else:
            return True

    def clear_reset_order(self, dlgid):
        '''
        Borra la orden de reset.
        '''
        if self.handle.hexists(dlgid, 'RESET'):
            self.handle.hset(dlgid, 'RESET', 'FALSE')

    def update_uid(self, dlgid, uid):
        '''
        Actualizo el UID
        '''
        # Leo de la redis el dlgid que debo loguer en modo SELECT
        if not self.connect():
            stats.inc_count_errors()
            return False

        self.handle.hset(dlgid, 'UID', uid)
        return True

    def get_d_remotes_vars(self, dlgid):
        '''
        Devuelve un diccionario con la definicion de las variables remotas
        El formato del diccionario es:
        
        REMOTOS = { 'KIYU001':[('HTQ1', 'ALTURA_TANQUE_KIYU_1'), ('HTQ2', 'ALTURA_TANQUE_KIYU_2')],
            'SJOSE001' : [ ('PA', 'PRESION_ALTA_SJ1'), ('PB', 'PRESION_BAJA_SQ1')]
          }

        Si no tengo nada, devuelvo un dict vacio !!.
        '''
        if not self.connect():
            log(module=__name__, function='get_d_remotes_vars', level='ERROR', dlgid=dlgid, msg='REDIS NOT CONNECTED')
            stats.inc_count_errors()
            return None

        # Si no hay registro ( HASH) del datalogger lo creo.
        if not self.handle.exists(dlgid):
            self.init_sysvars_record(dlgid)
            return None

        if not self.handle.hexists(dlgid, 'REMVARS'):
            self.init_sysvars_record(dlgid)
            log(module=__name__, function='get_d_remotes_vars', level='ERROR',  dlgid=dlgid, msg='REDIS ERROR: No existe key REMVARS')
            return None

        try:
            remotes_vars_pkline = self.handle.hget(dlgid, 'REMVARS')
        except:
            log(module=__name__, function='get_d_remotes_vars', level='ERROR', dlgid=dlgid, msg='ERROR in HGET{0}'.format(dlgid))
            stats.inc_count_errors()
            return None

        if remotes_vars_pkline != b'NUL':
            d_remotes_vars = pickle.loads(remotes_vars_pkline)
            log(module=__name__, function='get_d_remotes_vars', level='SELECT', dlgid=dlgid, msg=f'D_REM_VARS={d_remotes_vars}')
            return d_remotes_vars
        else:
            # Lo debo leer de la base persistente
            atv=BD_ATVISE()
            d_conf=atv.read_plc_conf(dlgid)
            if d_conf is None:
                log(module=__name__, function='get_d_remotes_vars', level='ERROR', dlgid=dlgid, msg='ERROR: Conf in PGSQL is None !!!')
                return None
            else:
                # Guardo la configuracion en REDIS para la proxima vez
                d_memblocks=d_conf['MEMBLOCK']
                mbk_pkline = pickle.dumps(d_memblocks)
                self.handle.hset(dlgid, 'MEMBLOCK', mbk_pkline)
                d_remotes_vars=d_conf['REMVARS']
                remotes_vars_pkline = pickle.dumps(d_remotes_vars)
                self.handle.hset(dlgid, 'REMVARS', remotes_vars_pkline)
                log(module=__name__, function='get_d_remotes_vars', level='SELECT', dlgid=dlgid, msg=f'D_REM_VARS(BD)={d_remotes_vars}')
                #
                return d_remotes_vars

        return None

    def get_remote_var_value(self, dlgid, var_name_remoto):
        '''

        Lee de la redis del dlgid el valor de la variable con nombre var_name_remoto
        Leo la PKLINE, y de este saco el valor de la variable.
        '''
        if not self.connect():
            log(module=__name__, function='get_remote_var_value', level='ERROR', dlgid=dlgid, msg='REDIS NOT CONNECTED')
            stats.inc_count_errors()
            return None

        try:
            pk_line = self.handle.hget(dlgid, 'PKLINE')
            d_datos = pickle.loads(pk_line)
        except:
            log(module=__name__, function='get_remote_var_value', level='ERROR', dlgid=dlgid, msg='REDIS ERROR in HGET {0}'.format(dlgid))
            stats.inc_count_errors()
            return None

        value = d_datos.get(var_name_remoto, -99)
        return float(value)

    def get_memblock(self, dlgid):
        '''
        Si no existe la entrada en REDIS o el valor que tiene es NUL, lo intento leer de la base persistente.
        Lee el campo 'MEMBLOCK', lo des-serializa y retorna el dict.
        '''
        if not self.connect():
            log(module=__name__, function='read_memblock', level='ERROR', dlgid=dlgid, msg='REDIS NOT CONNECTED')
            stats.inc_count_errors()
            return None

        # Si no hay registro ( HASH) del datalogger lo creo.
        if not self.handle.exists(dlgid):
            self.init_sysvars_record(dlgid)
            return None

        if not self.handle.hexists(dlgid, 'MEMBLOCK'):
            self.init_sysvars_record(dlgid)
            log(module=__name__, function='read_memblock', level='ERROR',  dlgid=dlgid, msg='REDIS ERROR: No existe key MEMBLOCK')
            return None

        try:
            mbk_pkline = self.handle.hget(dlgid, 'MEMBLOCK')
        except:
            log(module=__name__, function='read_memblock', level='ERROR', dlgid=dlgid, msg='ERROR in HGET{0}'.format(dlgid))
            stats.inc_count_errors()
            return None

        if mbk_pkline == b'NUL':
            # Lo debo leer de la base persistente
            atv=BD_ATVISE()
            d_conf=atv.read_plc_conf(dlgid)
            if d_conf is None:
                log(module=__name__, function='read_memblock', level='ERROR', dlgid=dlgid, msg='ERROR: Conf in PGSQL is None !!!')
                return None

            # Guardo la configuracion en REDIS para la proxima vez
            print(d_conf)
            d_memblocks=d_conf['MEMBLOCK']
            mbk_pkline = pickle.dumps(d_memblocks)
            self.handle.hset(dlgid, 'MEMBLOCK', mbk_pkline)
            d_remotes_vars=d_conf['REMVARS']
            remotes_vars_pkline = pickle.dumps(d_remotes_vars)
            self.handle.hset(dlgid, 'REMVARS', remotes_vars_pkline)
            #
            return d_memblocks
        else:
            d_mbk = pickle.loads(mbk_pkline)
            return d_mbk

        return None

    def get_atvise_responses(self, dlgid):
        '''
        Leo la clave ATVISE.
        Este es un diccionario serializado con las ordenes ( respuestas ) hacia el PLC
        Responde con un diccionario.
        '''
        if not self.connect():
            log(module=__name__, function='get_atvise_responses', level='ERROR', dlgid=dlgid, msg='REDIS NOT CONNECTED')
            stats.inc_count_errors()
            return None

        # Si no hay registro ( HASH) del datalogger lo creo.
        if not self.handle.exists(dlgid):
            self.init_sysvars_record(dlgid)
            return None

        if not self.handle.hexists(dlgid, 'ATVISE'):
            self.init_sysvars_record(dlgid)
            log(module=__name__, function='get_atvise_responses', level='ERROR',  dlgid=dlgid, msg='REDIS ERROR: No existe key MEMBLOCK')
            return None

        try:
            d_rsp_pkline = self.handle.hget(dlgid, 'ATVISE')
        except:
            log(module=__name__, function='get_atvise_responses', level='ERROR', dlgid=dlgid, msg='ERROR in HGET{0}'.format(dlgid))
            stats.inc_count_errors()
            return None

        if d_rsp_pkline != b'NUL':
            d_rsp = pickle.loads(d_rsp_pkline)
            self.handle.hset(dlgid, 'ATVISE', 'NUL')
            return d_rsp

        return {}


