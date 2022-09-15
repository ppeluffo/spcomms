#!/opt/anaconda3/envs/mlearn/bin/python3

from FUNCAUX.FRAMES.spc_framesBase import BASE_frame
from FUNCAUX.UTILS.spc_log import log
import datetime as dt

# ------------------------------------------------------------------------------

class GENERICO_frame(BASE_frame):
    # Esta clase está especializada en los frames de datos de las estaciones OCEANUS
    #
    def process(self):
        # Especializo el metodo con caracteristicas de estaciones OCEANUS
        log(module=__name__, function='process', level='SELECT', dlgid=self.dlgid, msg='DEBUG PROCESS GENERICO')

        self.dlgid = self.d.get('ID', '00000')
        self.version = self.d.get('VER', 'R0.0.0')
        log(module=__name__, function='process', level='ERROR', dlgid=self.dlgid, msg='D={0}</br>'.format(self.d))
        
        self.response += 'GENERICO:{0};{1}'.format(dt.datetime.now().strftime('%y%m%d%H%M'), self.d['PAYLOAD'])
        self.process_response()
        return self.dlgid, self.response


