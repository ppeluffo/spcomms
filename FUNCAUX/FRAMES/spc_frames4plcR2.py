#!/opt/anaconda3/envs/mlearn/bin/python3
'''
En esta version del protocolo de los PLC, el payload es un bytearray que viene formateado segun un
plc_memblock_definition.
Al decodificarlo, tenemos un dict() con las variables.

Al enviar datos, estos son bytearray por lo tanto debemos usar un Content-type acorde y enviarlos
con sys.stdout.buffer.write para que no sean formateados. ( El print los formatea ).

'''
from FUNCAUX.FRAMES.spc_framesBase import BASE_frame
from FUNCAUX.UTILS.spc_log import log
from FUNCAUX.UTILS.spc_memblocks import MEMBLOCK
from FUNCAUX.BD.spc_bd_redis import BD_REDIS
import sys

def dummy_response_mbk():
    #d1 = {'count': 32, 'endian': 101.32, 'version': 2, 'var1': 356.89, 'var2': 10345.6}
    d1 = {'var1': 356.89, 'var2': 10345.6}
    # 356.89 : 43B271EC : C \xB2 q \xEC: \xECq\xB2C
    # 10345.6 : 4621A666: F ! \A6 f    : f\A6!F
    #
    # Lo que recibo en COMMIX es
    #  8\CR\LF\xECq\xB2Cf\xA6!F\CR\LFCD CC F6 42 66 E6 76 44
    #  38 0D 0A EC 71 B2 43 66 A6 21 46 0D 0A 30 0D 0A 0D 0A
    return d1

# ------------------------------------------------------------------------------

class PLCR2_frame(BASE_frame):
    # Esta clase está especializada en los frames de datos de PLCR2 en el que el payload es la serializacion
    # de un memblock.
    #
    def process(self):
        log(module=__name__, function='process', level='SELECT', dlgid=self.dlgid, msg='DEBUG PROCESS PLCR2')

        self.dlgid = self.d.get('ID', '00000')
        self.version = self.d.get('VER', 'R0.0.0')
        log(module=__name__, function='process', level='SELECT', dlgid=self.dlgid, msg='D={0}</br>'.format(self.d))

        # El payload debo transformarlo para obtener un diccionario con las variables
        mbk = MEMBLOCK()
        log(module=__name__, function='process', level='SELECT', dlgid=self.dlgid, msg='MBK_DEFINITION={0}</br>'.format(mbk.get_memblock_definition()))
        #
        d_rcvdMbk = mbk.convert_bytes2dict(self.d['PAYLOAD'])
        log(module=__name__, function='process', level='ERROR', dlgid=self.dlgid, msg='RCVD_MBK={0}</br>'.format(d_rcvdMbk))
        #
        # Guardo el frame el Redis
        self.rh = BD_REDIS()
        self.rh.enqueue_data_record(d_rcvdMbk, 'LQ_PLCR2DATA')
        #
        # ---------------------------------------------------------------------
        # Preparo la respuesta y transmito. Agrego las órdenes (en REDIS) para el PLC
        #
        # ---------------------------------------------------------------------
        # Dummy Response Testing
        d_dummy = dummy_response_mbk()
        log(module=__name__, function='process', level='SELECT', dlgid=self.dlgid, msg='RSP_D={0}</br>'.format(d_dummy))
        rsp_payload = mbk.convert_dict2bytes(d_dummy)
        #rsp_payload = b'\x0e\x00\x00\x00\xcd\xcc\xf6Bd\x00\x00\x00\xc3\xf5H@\xcd\xcc,@'
        #rsp_payload = b'\x0e\x00\x00\x00\xcd'
        #rsp_payload = 'HOLA'
        #rsp_payload = b'\x0e\x00\x00\x00\xcd\xbd'
        #rsp_payload = b'ABC'
        log(module=__name__, function='process', level='SELECT', dlgid=self.dlgid, msg='RSP_MBK={0}</br>'.format(rsp_payload))
        self.response = rsp_payload
        self.send_response()
        return self.dlgid, self.response

    def send_response(self):
        try:
            #print("Content-Type: application/octet-stream\n")
            print("Content-Type: application/x-binary\n")
            sys.stdout.flush()
            sys.stdout.buffer.write(self.response)
            sys.stdout.flush()
        except Exception as e:
            log(module=__name__, function='u_send_response',level='ERROR',dlgid=self.dlgid,msg='EXCEPTION=[{0}]'.format(e))
            return
        sys.stdout.flush()
        log(module=__name__, function='send_response',level='SELECT',dlgid=self.dlgid, msg='RSP={0}'.format(rsp_payload))
        return
