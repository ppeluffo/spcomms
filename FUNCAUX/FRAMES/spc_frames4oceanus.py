#!/opt/anaconda3/envs/mlearn/bin/python3

from FUNCAUX.FRAMES.spc_framesBase import BASE_frame
from FUNCAUX.UTILS.spc_log import log
from FUNCAUX.BD.spc_bd_redis import BD_REDIS
import datetime as dt
import re
# ------------------------------------------------------------------------------

class OCEANUS_frame(BASE_frame):
    # Esta clase está especializada en los frames de datos de las estaciones OCEANUS
    # PAYLOAD: '5,@\\FO000TEMPER:21Z3*@\\FO000HUM:66%RH6zZ,#$@\\FO000Z'
    # 'PAYLOAD': '@\\FO000HUM:54%RHsZ,#$@\\FO000Z'}
    # 'PAYLOAD': '@\\FO000PM10:15ug/m3+Z5,$@\\FO000TSP:20ug/m3.Z'
    # 'PAYLOAD': '6-@\\FO000PM2.5:8ug/m3Z6-'}

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
                log(module=__name__, function='process', level='ERROR', dlgid=self.dlgid, msg='{0}={1}'.format(name,value))
        #
        # Guardo el frame el Redis
        self.rh = BD_REDIS()
        self.rh.enqueue_data_record(self.dlgid, self.d, 'LQ_OCEANUSDATA')

        self.response = 'OK'
        self.process_response()
        return self.dlgid, self.response


