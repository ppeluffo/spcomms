#!/home/pablo/Spymovil/www/cgi-bin/spcommsV3/venv/bin/python3
'''
Recibe el d_inputs y analiza el protocolo para determinar
que servicio va a atender el frame
'''
from FUNCAUX.UTILS.spc_log import log2

class SelectorProtocolo:

    def __init__(self):
        pass

    def decode_protocol(self, d_input):
        
        query_string = d_input.get('GET',{}).get('QS', '')
        protocolo = 'UNKNOWN'
        #
        if 'SPXR3' in query_string:
            protocolo = 'SPXR3'
        elif 'SPXR2' in query_string:
            protocolo = 'SPXR3'
        elif 'PLCR2' in query_string:
            protocolo = 'PLCR2'
        else:
            d_log = { 'MODULE':__name__, 'FUNCTION':'decode_protocol', 'LEVEL':'INFO',  
                 'MSG':f'ERROR: PROTOCOLO DESCONOCIDO !!'}
            log2(d_log)
        #
        return protocolo
    
    