#!/opt/anaconda3/envs/mlearn/bin/python3

import os
from FUNCAUX.FRAMES.spc_frames4plc import PLC_frame
from FUNCAUX.UTILS.spc_log import log

# ------------------------------------------------------------------------------
# http://192.168.0.9/cgi-bin/COMMS/spcomms.py?ID:PABLO;TYPE:PLCPAY;VER:4.0.4;UMOD:100;MOD:102;UFREQ:100;PS:0.00;QS:0.00;HC:0.00;VF:10.05;VC:3.46;VT:30.56;ST:20562

class PLCPAY_frame(PLC_frame):
    # Esta clase está especializada en los frames de datos de los PLC
    # ID:PABLO;TYPE:PLCPAY;VER:4.0.4a;PA:3.21;PB:1.34;H:4.56;bt:10.11

    def process_automatismos_paysandu(self):
        # Exception 001: Atencion a PLC de paysandu
        CBK = '/datos/cgi-bin/spx/AUTOMATISMOS/serv_APP_selection.py'
        
        # os.system regresa el valor numérico del resultado no una excepción.
        # 0 es OK cualquier otro es un error si bien se debe asegurar que el script a ejecutar retorna un 0 
        # puesto que esto se puede definir en el mismo. Con un exit(0)
        
        result = os.system('{0} {1}'.format(CBK, self.dlgid))
        if result == 0: 
            log(module=__name__, function='process', level='SELECT', dlgid=self.dlgid, msg='DEBUG AUTOMATISMOS PAYSANDU2 OK')
        elif result == 32512: 
            log(module=__name__, function='process', level='SELECT', dlgid=self.dlgid, msg='DEBUG AUTOMATISMOS PAYSANDU2 ERROR not found {0}'.format(CBK))
            self.response="ERROR LOAD MODULE serv_APP_selection"
        else: 
            log(module=__name__, function='process', level='SELECT', dlgid=self.dlgid, msg='DEBUG AUTOMATISMOS PAYSANDU2 ERROR')
            self.response="ERROR LOAD MODULE serv_APP_selection"

        return

    def process(self):
        # Especializo el metodo con caracteristicas de automatismos
        self.process_base()
        log(module=__name__, function='process', level='SELECT', dlgid=self.dlgid, msg='DEBUG PROCESS OK')
        self.process_automatismos()
        log(module=__name__, function='process', level='SELECT', dlgid=self.dlgid, msg='DEBUG AUTOMATISMOS OK')
        self.process_automatismos_paysandu()
        log(module=__name__, function='process', level='SELECT', dlgid=self.dlgid, msg='DEBUG AUTOMATISMOS PAYSANDU OK')
        self.process_response()
        return self.dlgid, self.response


