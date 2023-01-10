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

REMOTOS = { 'KIYU001':[('HTQ1', 'ALTURA_TANQUE_KIYU_1'), ('HTQ2', 'ALTURA_TANQUE_KIYU_2')],
            'SJOSE001' : [ ('PA', 'PRESION_ALTA_SJ1'), ('PB', 'PRESION_BAJA_SQ1')]
          }

          
def dummy_response_mbk():
    #d1 = {'count': 32, 'endian': 101.32, 'version': 2, 'var1': 356.89, 'var2': 10345.6}
    d1 = {'UPA1_ORDER_1': 12, 'UPA1_CONSIGNA_6': 15, 'NIVEL_TQ_KIYU':12.34  }
    # d1 = {'var1': 12, 'var2': 15, 'var3':32.45, 'var4':2233, 'var5':1234, 'var6':0.0, 'var7':0.0, 'var8':0, 'var9':0 }
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
        '''
        Procesamiento de la informacion enviada por el PLC.
        Las variables están en un bloque que debe decodificarse y las respuestas deben colocarse en otro
        bloque a enviar.
        '''
        self.dlgid = self.d.get('ID', '00000')
        self.version = self.d.get('VER', 'R0.0.0')

        log(module=__name__, function='process', level='SELECT', dlgid=self.dlgid, msg='DEBUG PROCESS RCVD PLCR2')
        log(module=__name__, function='process', level='SELECT', dlgid=self.dlgid, msg='D={0}</br>'.format(self.d))

        # El payload debo transformarlo para obtener un diccionario con las variables
        mbk = MEMBLOCK()
        mbk.load(self.dlgid)

        log(module=__name__, function='process', level='SELECT', dlgid=self.dlgid, msg='MBK_DEFINITION={0}'.format(mbk.get_d_mbk()))
        #
        # ---------------------------------------------------------------------
        # RECEPCION DE DATOS DEL PLC
        # ---------------------------------------------------------------------
        d_rcvd_data = mbk.convert_bytes2dict(self.d['PAYLOAD'])
        log(module=__name__, function='process', level='SELECT', dlgid=self.dlgid, msg='RCVD_D_DATA={0}'.format(d_rcvd_data))
        #
        # Guardo el frame el Redis
        self.rh = BD_REDIS()
        self.rh.enqueue_data_record(self.dlgid, d_rcvd_data, 'LQ_PLCR2DATA')
        
        
        # ---------------------------------------------------------------------
        # TRANSMISION DE RESPUESTA AL PLC
        # ---------------------------------------------------------------------
        d_responses = {}
        # Leo el diccionario con las variables remotas ( dlgid: [ (var_name_remoto, var_name_local),  ] )
        # var_name_remoto es el nombre de la variable en el equipo remoto (pA)
        # var_name_local es el nombre definido en el mbk. ( ALTURA_TANQUE_KIYU )
        d_rem_vars = self.rh.get_d_remotes_vars(self.dlgid)
        for dlgid in d_rem_vars:
            var_list = d_rem_vars[dlgid]
            for var_name_remoto, var_name_central in var_list:
                rem_value = self.rh.get_remote_var_value(dlgid, var_name_remoto)
                d_responses[var_name_central] = rem_value

        log(module=__name__, function='process', level='SELECT', dlgid=self.dlgid, msg='RSP_D_REM={0}'.format(d_responses))

        # Leo las variables de la API del ATVISE y las agrego al diccionario de respuestas
        # ---------------------------------------------------------------------
        d_atvise_responses = self.rh.get_atvise_responses(self.dlgid)
        log(module=__name__, function='process', level='SELECT', dlgid=self.dlgid, msg='RSP_D_ATV={0}'.format(d_atvise_responses))

        # Junto todas las variables de la respuesta
        d_responses.update(d_atvise_responses)

        # Dummy Response Testing
        #d_responses = dummy_response_mbk()

        log(module=__name__, function='process', level='SELECT', dlgid=self.dlgid, msg='RSP_D={0}'.format(d_responses))
        # Armo el bloque de respuestas a enviar apareando el d_responses con el mbk dando como resultado un rsp_dict.
        rsp_dict, rsp_payload = mbk.convert_dict2bytes(d_responses)

        log(module=__name__, function='process', level='SELECT', dlgid=self.dlgid, msg='RSP_DICT={0}'.format(rsp_dict))
        log(module=__name__, function='process', level='SELECT', dlgid=self.dlgid, msg='RSP_MBK={0}'.format(rsp_payload))
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
        log(module=__name__, function='send_response',level='SELECT',dlgid=self.dlgid, msg='END OK; RSP={0}'.format(self.response))
        return
