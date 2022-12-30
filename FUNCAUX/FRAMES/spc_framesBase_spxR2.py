#!/opt/anaconda3/envs/mlearn/bin/python3

"""
Esta clase está especializada en los frames de datos de los dataloggers SPX2022
Estos dataloggers transmiten via modems USR-IOT por medio de GET.
Hay 5 tipos de frames que se diferencian por el parametro CLASS
 RECOVER:       ID:DEFAULT;TYPE:SPXR2;VER:1.0.0;CLASS:RECOVER;UID:42125128300065090117010400000000
 CONF_BASE:     ID:PABLO;TYPE:SPXR2;VER:1.0.0;CLASS:CONF_BASE;UID:42125128300065090117010400000000;HASH:0x11
 CONF_AINPUTS:  ID:PABLO;TYPE:SPXR2;VER:1.0.0;CLASS:CONF_AINPUTS;HASH:0x01           
 CONF_COUNTERS: ID:PABLO;TYPE:SPXR2;VER:1.0.0;CLASS:CONF_COUNTERS;HASH:0x86
 DATA:          ID:PABLO;TYPE:SPXR2;VER:1.0.0;CLASS:DATA;A0:0.00;A1:0.00;A2:0.00;C0:0.000;C1:0.000
 PING:          ID:PABLO;TYPE:SPXR2;VER:1.0.0;CLASS:PING

Los frames de Recover, conf_base, conf_ainputs,conf_counters mandan un checksum de la respectiva
configuracion local.

Modo de testing:
telnet localhost 80
GET /cgi-bin/spcomms/spcomms.py?ID:DEFAULT;TYPE:SPXR2;VER:1.0.0;CLASS:RECOVER;UID:42125128300065090117010400000000
HTTP/1.1
Host: www.spymovil.com

GET /cgi-bin/spcomms/spcomms.py?ID:PABLO;TYPE:SPXR2;VER:1.0.0;CLASS:CONF_BASE;UID:42125128300065090117010400000000;HASH:0xAF
HTTP/1.1
Host: www.spymovil.com

GET /cgi-bin/spcomms/spcomms.py?ID:PABLO;TYPE:SPXR2;VER:1.0.0;CLASS:CONF_AINPUTS;HASH:0x23
HTTP/1.1
Host: www.spymovil.com

GET /cgi-bin/spcomms/spcomms.py?ID:PABLO;TYPE:SPXR2;VER:1.0.0;CLASS:CONF_COUNTERS;HASH:0x74
HTTP/1.1
Host: www.spymovil.com

GET /cgi-bin/spcomms/spcomms.py?ID:PABLO;TYPE:SPXR2;VER:1.0.0;CLASS:DATA;A0:0.00;A1:0.00;A2:0.00;C0:0.000;C1:0.000
HTTP/1.1
Host: www.spymovil.com

GET /cgi-bin/spcomms/spcomms.py?ID:PABLO;TYPE:SPXR2;VER:1.0.0;CLASS:PING
HTTP/1.1
Host: www.spymovil.com

"""

import datetime as dt
from FUNCAUX.FRAMES.spc_framesBase import BASE_frame
from FUNCAUX.UTILS.spc_log import log
from FUNCAUX.BD.spc_bd_redis import BD_REDIS
from FUNCAUX.BD.spc_bd_gda import BD_GDA
from FUNCAUX.UTILS.spc_utils import u_hash

# ------------------------------------------------------------------------------

class SPXR2_FRAME(BASE_frame):
    '''
    En el __init__ de BASE_frame tomo el dict d que tiene {'QUERY_STRING', 'ID', 'VER', 'TYPE', 'PAYLOAD'}
    Hago un overload del 'process'
    '''

    def process(self):
        '''
        Procesamiento del frame tipo SPX en todas las CLASES ( Overload )
        '''
        self.dlgid = self.d.get('ID', '00000')
        self.version = self.d.get('VER', 'R0.0.0')
        self.dict_payload = { k:v for (k, v) in [x.split(':') for x in self.d['PAYLOAD'].split(';') if ':' in x ] }

        #log(module=__name__, function='process', level='SELECT', dlgid=self.dlgid, msg='DEBUG SPXR2_frame')

        payload = self.d.get('PAYLOAD','NONE')
        #log(module=__name__, function='process', level='SELECT', dlgid=self.dlgid, msg=f'DEBUG PAYLOAD={payload}')

        clase = self.dict_payload.get('CLASS','UNKNOWN')

        if clase == 'RECOVER':
            self.response = self.process_frame_recover()
        elif clase == 'PING':
            self.response = self.process_frame_ping()
        elif clase == 'CONF_BASE':
            self.response = self.process_frame_config_base()
        elif clase == 'CONF_AINPUTS':
            self.response = self.process_frame_config_ainputs()
        elif clase == 'CONF_COUNTERS':
            self.response = self.process_frame_config_counters()
        elif clase == 'DATA':
            self.response = self.process_frame_data('DATA')
        elif clase == 'BLOCK':
            self.response = self.process_frame_data('BLOCK')
        elif clase == 'BLOCKEND':
            self.response = self.process_frame_data('BLOCKEND')
        else:
            # Frame no reconocido
            self.response = 'ERROR:UNKNOWN FRAME TYPE'

        #log(module=__name__, function='process', level='SELECT', dlgid=self.dlgid, msg=f'RSP={self.response}')
        self.process_response()
        return self.dlgid, self.response

    def process_frame_recover(self):
        '''
        FRAME: ID:DEFAULT;TYPE:SPXR2;VER:1.0.0;CLASS:RECOVER;UID:42125128300065090117010400000000
        Extrae el UID del payload.
        Lee de la base de datos GDA el DLGID que corresponde al UID
        Si no lo encuentro, mando DEFAULT
        Si no manda la nueva configuracion
        
        Testing:
        GET /cgi-bin/spcomms/spcomms.py?ID:DEFAULT;TYPE:SPXR2;VER:1.0.0;CLASS:RECOVER;UID:42125128300065090117010400000000
        HTTP/1.1
        Host: www.spymovil.com
        '''
        log(module=__name__, function='process', level='SELECT', dlgid=self.dlgid, msg='DEBUG: SPXR2_FRAME_RECOVER')
        uid = self.dict_payload.get('UID',None)
        gda_handle = BD_GDA()
        new_dlgid = gda_handle.get_dlgid_from_uid(uid)
        if new_dlgid is None:
            response = 'CLASS:RECOVER;DLGID:DEFAULT'
        else:
            response = f'CLASS:RECOVER;DLGID:{new_dlgid}'
        return response

    def process_frame_config_base(self):
        '''
        FRAME ID:PABLO;TYPE:SPXR2;VER:1.0.0;CLASS:CONF_BASE;UID:42125128300065090117010400000000;HASH:0x11
        Primero veo que haya un registro en REDIS con una configuracion valida.
        Compara el hash con el que calcula localmente de la configuracion.
        Si coincide devuelve un OK.
        Si no manda la configuracion BASE
        #
        Testing:
        GET /cgi-bin/spcomms/spcomms.py?ID:PABLO;TYPE:SPXR2;VER:1.0.0;CLASS:CONF_BASE;UID:42125128300065090117010400000000;HASH:0x11
        HTTP/1.1
        Host: www.spymovil.com
        '''
        redis_handle = BD_REDIS()
        gda_handle = BD_GDA()
        # Vemos que exista una configuracion valida.
        dconf = redis_handle.check_config_valid(self.dlgid)
        if dconf is None:
            response = 'CLASS:CONF_BASE;CONFIG:ERROR'
            return response
        #
        # Primero actualizo el UID en la redis y en GDA
        uid = self.dict_payload.get('UID',None)
        redis_handle.update_uid(self.dlgid, uid)
        gda_handle.update_uid(self.dlgid, uid)

        # Calculo el hash ( viene en hex por eso lo convierto a decimal )
        dlg_hash = int(self.dict_payload.get('HASH', 0), 16)
        # Calculo el local_cks con los datos de configuracion de la BD.
        timerpoll = int(dconf.get(('BASE', 'TPOLL'),'0'))
        timerdial = int(dconf.get(('BASE', 'TDIAL'),'0'))
        pwr_modo = int(dconf.get(('BASE', 'PWRS_MODO'),'0'))
        pwr_hhmm_on = int(dconf.get(('BASE', 'PWRS_HHMM1'),'601'))
        pwr_hhmm_off = int(dconf.get(('BASE', 'PWRS_HHMM2'),'2201'))
        #
        xhash = 0
        hash_str = f'[TIMERPOLL:{timerpoll:03}]'
        xhash = u_hash(xhash, hash_str)
        # log(module=__name__, function='process', level='ERROR', dlgid=self.dlgid, msg=f'DEBUG: HASH_BASE: hash_str=<{hash_str}>, hash={xhash}')
        hash_str = f'[TIMERDIAL:{timerdial:03}]'
        xhash = u_hash(xhash, hash_str)
        # log(module=__name__, function='process', level='ERROR', dlgid=self.dlgid, msg=f'DEBUG: HASH_BASE: hash_str=<{hash_str}>, hash={xhash}')
        hash_str = f'[PWRMODO:{pwr_modo}]'
        xhash = u_hash(xhash, hash_str)
        # log(module=__name__, function='process', level='ERROR', dlgid=self.dlgid, msg=f'DEBUG: HASH_BASE: hash_str=<{hash_str}>, hash={xhash}')
        hash_str = f'[PWRON:{pwr_hhmm_on:04}]'
        xhash = u_hash(xhash, hash_str)
        # log(module=__name__, function='process', level='ERROR', dlgid=self.dlgid, msg=f'DEBUG: HASH_BASE: hash_str=<{hash_str}>, hash={xhash}')
        hash_str = f'[PWROFF:{pwr_hhmm_off:04}]'
        xhash = u_hash(xhash, hash_str)
        # log(module=__name__, function='process', level='ERROR', dlgid=self.dlgid, msg=f'DEBUG: HASH_BASE: hash_str=<{hash_str}>, hash={xhash}')
        #
        bd_hash = xhash
        log(module=__name__, function='process', level='SELECT', dlgid=self.dlgid, msg=f'BASE: dlg_hash={dlg_hash}, bd_hash={bd_hash}')
        if dlg_hash == bd_hash:
            response = 'CLASS:CONF_BASE;CONFIG:OK'
        else:
            # Envio la nueva configuracion
            if pwr_modo == 0:
                s_pwrmodo = 'CONTINUO'
            elif pwr_modo == 1:
                s_pwrmodo = 'DISCRETO'
            else:
                s_pwrmodo = 'MIXTO'
            response = f'CLASS:CONF_BASE;TPOLL:{timerpoll};TDIAL:{timerdial};PWRMODO:{s_pwrmodo};PWRON:{pwr_hhmm_on:04};PWROFF:{pwr_hhmm_off:04}'
        return response

    def process_frame_config_ainputs(self):
        '''
        FRAME ID:PABLO;TYPE:SPXR2;VER:1.0.0;CLASS:CONF_AINPUTS;HASH:0x01
        Primero veo que haya un registro en REDIS con una configuracion valida.
        Compara el hash con el que calcula localmente de la configuracion.
        Si coincide devuelve un OK.
        Si no manda la configuracion CONF_AINPUTS

        Testing:
        GET /cgi-bin/spcomms/spcomms.py?ID:PABLO;TYPE:SPXR2;VER:1.0.0;CLASS:CONF_AINPUTS;HASH:0x01
        HTTP/1.1
        Host: www.spymovil.com
        '''
        redis_handle = BD_REDIS()
        # Vemos que exista una configuracion valida.
        dconf = redis_handle.check_config_valid(self.dlgid)
        if dconf is None:
            response = 'CLASS:BASE;CONF_AINPUTS:ERROR'
            return response
        #
        # Extrae el hash que envia el datalogger
        dlg_hash = int(self.dict_payload.get('HASH', 0), 16)
        # Calculo el local hash con los datos de configuracion de la BD.
        xhash = 0
        for ch in ['A0','A1','A2']:
            if (ch,'NAME') in dconf.keys():      # ( 'A1', 'NAME' ) in  d.keys()
                name=dconf.get((ch, 'NAME'),'X')
                imin=int(dconf.get((ch, 'IMIN'),'0'))
                imax=int(dconf.get((ch, 'IMAX'),'0'))
                mmin=float(dconf.get((ch, 'MMIN'),'0'))
                mmax=float(dconf.get((ch, 'MMAX'),'0'))
                offset=float(dconf.get((ch, 'OFFSET'),'0'))
                hash_str = f'[{ch}:{name},{imin},{imax},{mmin:.02f},{mmax:.02f},{offset:.02f}]'
            else:
                hash_str = f'[{ch}:X,4,20,0.00,10.00,0.00]'
            xhash = u_hash(xhash, hash_str)
            # log(module=__name__, function='process', level='ERROR', dlgid=self.dlgid, msg=f'DEBUG: HASH_AINPUTS: hash_str=<{hash_str}>, hash={xhash}')

        bd_hash = xhash
        log(module=__name__, function='process', level='SELECT', dlgid=self.dlgid, msg=f'AINPUTS: dlg_hash={dlg_hash}, bd_hash={bd_hash}')
        if dlg_hash == bd_hash:
            response = 'CLASS:CONF_AINPUTS;CONFIG:OK'
        else:
            # Envio la nueva configuracion
            response = 'CLASS:CONF_AINPUTS;'
            for ch in ('A0','A1','A2'):
                name = dconf.get((ch, 'NAME'), 'X')
                imin = int(dconf.get((ch, 'IMIN'), 4))
                imax = int(dconf.get((ch, 'IMAX'), 20))
                mmin = float(dconf.get((ch, 'MMIN'), 0.00))
                mmax = float(dconf.get((ch, 'MMAX'), 10.00))
                offset = float(dconf.get((ch, 'OFFSET'), 0.00))
                response += f'{ch}:{name},{imin},{imax},{mmin},{mmax},{offset};'

        return response

    def process_frame_config_counters(self):
        '''
        FRAME ID:DEFAULT;TYPE:SPX;VER:1.0.0;CLASS:CONF_COUNTERS;HASH:0x86
        Primero veo que haya un registro en REDIS con una configuracion valida.
        Compara el hash con el que calcula localmente de la configuracion.
        Si coincide devuelve un OK.
        Si no manda la configuracion CONF_COUNTERS

        Testing:
        GET /cgi-bin/spcomms/spcomms.py?ID:PABLO;TYPE:SPXR2;VER:1.0.0;CLASS:CONF_COUNTERS;HASH:0x86
        HTTP/1.1
        Host: www.spymovil.com
        '''
        redis_handle = BD_REDIS()
        # Vemos que exista una configuracion valida.
        dconf = redis_handle.check_config_valid(self.dlgid)
        if dconf is None:
            response = 'CLASS:BASE;CONF_COUNTERS:ERROR'
            return response
        #
        # Extraigo el hash que trae el frame
        dlg_hash = int(self.dict_payload.get('HASH', 0), 16)
        # Calculo el local hash con los datos de configuracion de la BD.
        xhash = 0
        hash_str = '['
        for ch in ['C0','C1']:
            if (ch,'NAME') in dconf.keys():      # ( 'C0', 'NAME' ) in  d.keys()
                name=dconf.get((ch, 'NAME'),'X')
                str_modo = dconf.get((ch, 'MODO'),'CAUDAL')
                if str_modo == 'CAUDAL':
                    modo = 0    # caudal
                else:
                    modo = 1    # pulsos
                magpp=float(dconf.get((ch, 'MAGPP'),'0'))
                hash_str = f'[{ch}:{name},{magpp:.03f},{modo}]'
            else:
                hash_str = f'[{ch}:X,1.000,0]'
            #
            xhash = u_hash(xhash, hash_str)
            # log(module=__name__, function='process', level='ERROR', dlgid=self.dlgid, msg=f'DEBUG: HASH_CUNTERS: hash_str=<{hash_str}>, hash={xhash}')
        #
        bd_hash = xhash
        log(module=__name__, function='process', level='SELECT', dlgid=self.dlgid, msg=f'COUNTERS: dlg_hash={dlg_hash}, bd_hash={bd_hash}')
        #
        if dlg_hash == bd_hash:
            response = 'CLASS:CONF_COUNTERS;CONFIG:OK'
        else:
            # Envio la nueva configuracion
            response = 'CLASS:CONF_COUNTERS;'
            for ch in ('C0','C1'):
                name = dconf.get((ch, 'NAME'), 'X')
                magpp = float(dconf.get((ch, 'MAGPP'), 1.00))
                str_modo = dconf.get((ch, 'MODO'),'CAUDAL')
                response += f'{ch}:{name},{magpp},{str_modo};'

        return response

    def process_frame_ping(self):
        '''
        Funcion usada para indicar que el enlace esta activo
        '''
        response = 'CLASS:PONG'
        return response

    def process_frame_data(self, modo='DATA'):
        '''
        Agrego el dict_payload al d y encolo.

        Testing:
        GET /cgi-bin/spcomms/spcomms.py?ID:PABLO;TYPE:SPXR2;VER:1.0.0;CLASS:DATA;A0:0.00;A1:0.00;A2:0.00;C0:0.000;C1:0.000
        HTTP/1.1
        Host: www.spymovil.com

        Solo en los modo DATA o BLOCKEND mando respuesta.
        Esto es para acelarar los envios, que el datalogger no espere al servidor y desagote la memoria rapido.
        '''
        redis_handle = BD_REDIS()
        self.d.update(self.dict_payload)
        redis_handle.enqueue_data_record(self.d, 'LQ_SPXR2DATA')
        # Preparo la respuesta y transmito. Agrego las órdenes 

        #log(module=__name__, function='process', level='ERROR', dlgid=self.dlgid, msg=f'DEBUG process_frame_data1: modo={modo}')
        if modo in ('DATA', 'BLOCKEND'):
            #log(module=__name__, function='process', level='ERROR', dlgid=self.dlgid, msg=f'DEBUG process_frame_data2: modo={modo}')
            now=dt.datetime.now().strftime('%y%m%d%H%M')
            response = f'CLASS:DATA;CLOCK:{now};'
            # Agrego ordenes que leo del redis.
            # RESET
            if redis_handle.get_reset_order(self.dlgid):
                response += 'RESET;'
                # Borro el pedido.
                redis_handle.clear_reset_order(self.dlgid)

            # RELE STATE ??
        else:
            response = None
        #
        return response


