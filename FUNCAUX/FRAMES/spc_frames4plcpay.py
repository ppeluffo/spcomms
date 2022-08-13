#!/opt/anaconda3/envs/mlearn/bin/python3

import os
from FUNCAUX.FRAMES.spc_frames4plc import PLC_frame
# ------------------------------------------------------------------------------

class PLCPAY_frame(PLC_frame):
    # Esta clase está especializada en los frames de datos de los PLC
    # ID:PABLO;TYPE:PLCPAY;VER:4.0.4a;PA:3.21;PB:1.34;H:4.56;bt:10.11

    def process_automatismos_paysandu(self):
        # Exception 001: Atencion a PLC de paysandu
        CBK = '/datos/cgi-bin/spx/AUTOMATISMOS/serv_APP_selection.py'
        try:
            os.system('{0} {1}'.format(CBK, self.dlgid))
        except:
            self.response="ERROR LOAD MODULE serv_APP_selection"
        return

    def process(self):
        # Especializo el metodo con caracteristicas de automatismos
        self.process_base()
        self.process_automatismos()
        self.process_automatismos_paysandu()
        self.process_response()
        return self.dlgid, self.response


