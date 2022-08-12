#!/opt/anaconda3/envs/mlearn/bin/python3

import datetime as dt
from FUNCAUX.spc_mbus_write import mbusWrite
from FUNCAUX.spc_framesBase import BASE_frame
# ------------------------------------------------------------------------------


class PLC_frame(BASE_frame):
    # Esta clase está especializada en los frames de datos de los PLC
    # ID:PABLO;TYPE:PLC;VER:4.0.4a;PA:3.21;PB:1.34;H:4.56;bt:10.11

    def convert_rcvd_line_to_old_format(self, d):
        '''
        Convierte la nueva linea recibida:
        # ID:PABLO;TYPE:PLC;VER:4.0.4a;PA:3.21;PB:1.34;H:4.56;bt:10.11
        al formato anterior:
        DATE:220802;TIME:122441;PA:3.21;PB:1.34;H:4.56;bt:10.11
        '''
        rcvd_line = d.get('QUERY_STRING', "ERROR Line")
        l = rcvd_line.split(d['VER'])
        try:
            payload = l[1]  # ';PA:3.21;PB:1.34;H:4.56;bt:10.11'
        except:
            return "ERROR LINE"

        # Agrego el time stamp
        timestamp = dt.datetime.now().strftime("DATE:%02Y%02m%02d;TIME:%02H%02M")
        rcvd_line_new_format = timestamp + payload
        return rcvd_line_new_format

    def process_automatismos(self):
        # Guardo la linea recibida (d['RCVD']) en Redis en el campo 'LINE', para otros procesamientos
        # El formato debe ser igual al original.
        rcvd_line_new_format = self.convert_rcvd_line_to_old_format(self.d)
        self.rh.save_line( self.dlgid, rcvd_line_new_format)
        # Copio de "lastMODBUS" a "MODBUS" ( Esto es para prender/apagar las bombas !!!)
        mbusWrite(self.dlgid)
        # Agrego la línea MODBUS a la respuesta y borro.
        self.response += self.rh.get_modbusline(self.dlgid, clear=True)
        return

    def process(self):
        # Especializo el metodo con caracteristicas de automatismos
        self.process_base()
        self.process_automatismos()
        self.process_response()
        return self.dlgid, self.response


