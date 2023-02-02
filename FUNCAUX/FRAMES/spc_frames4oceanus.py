#!/opt/anaconda3/envs/mlearn/bin/python3

from FUNCAUX.FRAMES.spc_framesBase import BASE_frame
from FUNCAUX.UTILS.spc_log import log
from FUNCAUX.BD.spc_bd_redis import BD_REDIS
import datetime as dt
import re

codes2names = { 'a34004-Rtd':'PM2.5',
                'td':'PM10',
                'a34001-Rtd':'TSP',
                'a01001-Rtd': 'TEMPER',
                'a01002-Rtd': 'HUM',
                'a01006-Rtd': 'AIRPRE'
            }

# ------------------------------------------------------------------------------

class OCEANUS_frame(BASE_frame):
    # Esta clase está especializada en los frames de datos de las estaciones OCEANUS
    # PAYLOAD: '5,@\\FO000TEMPER:21Z3*@\\FO000HUM:66%RH6zZ,#$@\\FO000Z'
    # 'PAYLOAD': '@\\FO000HUM:54%RHsZ,#$@\\FO000Z'}
    # 'PAYLOAD': '@\\FO000PM10:15ug/m3+Z5,$@\\FO000TSP:20ug/m3.Z'
    # 'PAYLOAD': '6-@\\FO000PM2.5:8ug/m3Z6-'}
    # 'PAYLOAD': 'd=16;a34001-Rtd=29;a01001-Rtd=27.3;a01002-Rtd=51.0;a01006-Rtd=1013&&84C1'
    # 'PAYLOAD': '##0175QN=20230201211705000;ST=27;CN=2011;PW=123456;MN=;Flag=5;CP=&&DataTime=20230201211705;a34004-Rtd=4;a34002-R'

    #
    def process(self):
        # Especializo el metodo con caracteristicas de estaciones OCEANUS
        self.dlgid = self.d.get('ID', '00000')
        self.version = self.d.get('VER', 'R0.0.0')
        log(module=__name__, function='process', level='SELECT', dlgid=self.dlgid, msg='D={0}</br>'.format(self.d))

        # Las variables que maneja la estacion OCEANUS son: HUM,TEMPER,PM10,TSP,PM2.5
        for needle in ['HUM:[0-9]+', 'TEMPER:[0-9]+', 'PM10:[0-9]+', 'PM2.5:[0-9]+', 'TSP:[0-9]+']:
            subs = re.search(needle,self.d['PAYLOAD'])
            if subs:
                s = subs.group()
                name,value = s.split(':')
                self.d[name] = value
                log(module=__name__, function='process', level='SELECT', dlgid=self.dlgid, msg='{0}={1}'.format(name,value))
        #
        # Otras versiones manejan codigos.
        for field in re.split(';|&&', self.d['PAYLOAD']):
            if '=' in field:
                name_code,value=field.split('=')
                if name_code in codes2names:
                    name =codes2names[name_code]
                    self.d[name] = value
                    log(module=__name__, function='process', level='SELECT', dlgid=self.dlgid, msg='{0}->{1}={2}'.format(name_code, name,value))


        # Guardo el frame el Redis
        self.rh = BD_REDIS()
        self.rh.enqueue_data_record(self.dlgid, self.d, 'LQ_OCEANUSDATA')

        self.response = 'OK1'
        self.process_response()
        return self.dlgid, self.response


