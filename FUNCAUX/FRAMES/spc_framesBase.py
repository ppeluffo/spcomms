#!/opt/anaconda3/envs/mlearn/bin/python3

import datetime as dt
import sys
from FUNCAUX.UTILS import spc_stats as stats
from FUNCAUX.UTILS.spc_log import log
from FUNCAUX.BD.spc_bd_redis import BD_REDIS

# ------------------------------------------------------------------------------


class BASE_frame:
    # Esta clase es una superclase de procesamiento de frames.
    # La heredan las otras clases mas especializadas ( SPX,SP5K,PCL)
    # ID:PABLO;TYPE:S???;VER:4.0.4a;PA:3.21;PB:1.34;H:4.56;bt:10.11

    def __init__(self, d):
        self.d = d
        self.dlgid = None
        self.version = None
        self.rh = None
        self.dict_payload = None
        self.response = ''

        stats.inc_count_frame()
        stats.inc_count_frame_data()

        log(module=__name__, function='__init__', level='SELECT',dlgid=self.dlgid, msg='start')
        return

    def send_response(self):
        if self.response is not None:
            response = '{}'.format(self.response)
            try:
                print('Content-type: text/html\n\n',end='')
                print('<html><body><h1>{0}</h1></body></html>'.format(response))
            except Exception as e:
                stats.inc_count_errors()
                log(module=__name__, function='u_send_response',level='ERROR',dlgid=self.dlgid,msg='EXCEPTION=[{0}]'.format(e))
                return
        #
        #log(module=__name__, function='send_response',level='SELECT',dlgid=self.dlgid, msg='RSP={0}'.format(response))
        return

    def process_base(self):
        self.dlgid = self.d.get('ID', '00000')
        self.version = self.d.get('VER', 'R0.0.0')
        log(module=__name__, function='process', level='SELECT',dlgid=self.dlgid, msg='D={0}</br>'.format(self.d))
        #
        # Guardo el frame el Redis
        self.rh = BD_REDIS()
        #
        # Parseo ahora las variables que al ser un GET vinieron en el query_string
        try:
            dp = {k: v for (k, v) in [x.split(':') for x in self.d['PAYLOAD'].split(';') if ':' in x]}
        except:
            stats.inc_count_errors()
            log(module=__name__, function='process', level='ERROR', msg='DECODE ERROR: QS={0}'.format(self.d['QUERY_STRING']))
        self.d.update(dp)
        #
        # Encolo: cada tipo tiene una cola distinta
        device_type = self.d.get('TYPE', 'UNKNOWN')
        if device_type == 'PLC':
            self.rh.enqueue_data_record(self.d, 'LQ_PLCDATA')
        elif device_type == 'PLCPAY':
            self.rh.enqueue_data_record(self.d, 'LQ_PLCPAYDATA')
        elif device_type == 'SPX':
            self.rh.enqueue_data_record(self.d, 'LQ_SPXDATA')
        elif device_type == 'SP5K':
            self.rh.enqueue_data_record(self.d, 'LQ_SP5KDATA')

        # Preparo la respuesta y transmito. Agrego las órdenes MODBUS para el PLC
        self.response += '{};'.format(dt.datetime.now().strftime('%y%m%d%H%M'))

    def process_response(self):
        self.send_response()
        sys.stdout.flush()

    def process(self):
        # Realizo todos los pasos necesarios en el payload para generar la respuesta al datalooger e insertar
        # los datos en GDA
        self.process_base()
        self.process_response()
        return self.dlgid, self.response


