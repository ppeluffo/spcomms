#!/opt/anaconda3/envs/mlearn/bin/python3
#!/usr/bin/python3 -u
#

"""
Servidor de comunicaciones generales de los dispositivos de SPYMOVIL.
Todos los equipos acceden inicialmente a este script.
Se decodifica el query_string y se genera un diccionario.
De acuerdo a la clave TYPE se bifurca a la clase que atiende el dispositivo.
Version: 1.0.0 @ 2022-08-06
El query_string que deben mandar los dispositivos es del tipo:
ID:PABLO;TYPE:PLC;VER:4.0.4a;PA:3.21;PB:1.34;H:4.56;bt:10.11
ID:PABLO;TYPE:SP5K;VER:4.0.4a;PA:3.21;PB:1.34;H:4.56;bt:10.11
ID:PABLO;TYPE:SPX;VER:4.0.4a;PA:3.21;PB:1.34;H:4.56;bt:10.11

-----------------------------------------------------------------------------------------------------
Version: 1.0.1 @ 2022-08-05
1) Excepcion 001:
Se coloca en spc_frames4spx.py para el manejo de los PLC de PAYSANDU.
2) La inserción del LINE a la redis se hace en spyplc.py y no en xprocess porque los automatismos (paysandú)
   requieren que esta la linea insertada antes de ejecutarse
3) En la redis la linea se inserta con el formato:
LINE=DATE:220802;TIME:122441;UMOD:100;MOD:102;UFREQ:100;PS:0.00;QS:38.81;HC:3.92;VF:46.05;VC:0.00;VT:0.00;ST:20562;
En spyplc_xprocess::process_child_plc, en la seccion Automatismos se invoca a  rh.save_line(dlgid, rcvd_line).

-----------------------------------------------------------------------------------------------------
Version: 1.0.0 @ 2022-07-01
Este servidor solo atiende a los PLC que transmiten directo por medio de un router LTE.
Solo hay un frame que es de datos.
El formato es:  ID:PABLO;VER:4.0.4a;PA:3.21;PB:1.34;H:4.56;bt:10.11

Funcionamiento:
Cuando llega un frame se loguea y se le pasa a la clase DATA_frame para que lo procese.
Esta clase hace:
    1-Elimina el ultimo caracter si no es digito ( para que no de error mas adelante en el parseo ()
    2-Genera un diccionario de los nombres de variables: valor
    3-Usa la clase redis::enqueue_data_record para que serialize la linea recibida y la encole para su posterior
      procesamiento
    4-Le manda la respuesta al PLC agregando los datos que tenga la redis de este.


Testing:
- Con telnet:
telnet localhost 80
GET /cgi-bin/COMMS/spcomms.py?ID:PABLO;TYPE:SP5K;VER:4.0.4a;PA:3.21;PB:1.34;H:4.56;bt:10.11
 HTTP/1.1
Host: www.spymovil.com

telnet www.spymovil.com 90
GET /cgi-bin/AUTOM/spyplc.py?ID:PABLO;VER:4.0.4a;PA:3.21;PB:1.34;H:4.56;bt:10.11
HTTP/1.1
Host: www.spymovil.com

- Con browser:
> usamos el url: http://localhost/cgi-bin/COMMS/spcomms.py?ID:PABLO;VER:4.0.4a;PA:3.21;PB:1.34;H:4.56;bt:10.11

"""

import os
import sys
from FUNCAUX.UTILS.spc_log import *
from FUNCAUX.UTILS.spc_config import Config
from FUNCAUX.UTILS import spc_stats as stats

version = '1.0.0 @ 2022-08-06'
# -----------------------------------------------------------------------------

if __name__ == '__main__':

    # Lo primero es configurar el logger
    config_logger('SYSLOG')
    query_string = ''
    stats.init()
    # Leo del cgi
    query_string = os.environ.get('QUERY_STRING')

    # Modo consola ?
    if query_string is None and len(sys.argv) == 2:
        if sys.argv[1] == 'DEBUG_DATA_SPX':
            # Uso un query string fijo de test del archivo .conf
            query_string = Config['DEBUG']['debug_data_spx']

        elif sys.argv[1] == 'DEBUG_DATA_SP5K':
            # Uso un query string fijo de test del archivo .conf
            query_string = Config['DEBUG']['debug_data_sp5k']

        elif sys.argv[1] == 'DEBUG_DATA_PLC':
            # Uso un query string fijo de test del archivo .conf
            query_string = Config['DEBUG']['debug_data_plc']

        elif sys.argv[1] == 'DEBUG_DATA_PLCPAY':
            # Uso un query string fijo de test del archivo .conf
            query_string = Config['DEBUG']['debug_data_plcpay']

        else:
            print('ERROR: USO ./spcomms.py (DEBUG_DATA_SPX|DEBUG_DATA_SP5K|DEBUG_DATA_PLC|DEBUG_DATA_PLCPAY')
            exit(0)

        os.environ['QUERY_STRING'] = query_string
        log(module=__name__, function='__init__', level='WARN', msg='MODO CONSOLA: query_string: {0}'.format(query_string))

    # Modo CGI
    log(module=__name__, function='__init__', level='INFO', msg='QS: {0}'.format(query_string))
    if query_string is None:
        log(module=__name__, function='__init__', level='ALERT', msg='ERROR QS NULL')
        exit(1)

    # Proceso.
    pid = os.getpid()
    log(module=__name__, function='__init__', level='ALERT', msg='PID:{0} RX:[{1}]'.format(pid, query_string))

    # Parseo el query_string para determinar el TYPE y bifurcar a la clase correcta.
    # ID:PABLO;TYPE:SP5K;VER:4.0.4a;PA:3.21;PB:1.34;H:4.56;bt:10.11
    if not query_string[-1].isdigit():
        query_string = query_string[:-1]
    d = {}
    try:
        d = {k: v for (k, v) in [x.split(':') for x in query_string.split(';')]}
        d['QUERY_STRING'] = query_string
    except:
        stats.inc_count_errors()
        log(module=__name__, function='process', level='ERROR', msg='DECODE ERROR: QS={0}'.format(query_string))

    device_type = d.get('TYPE', 'UNKNOWN')
    dlgid = None
    response = None
    if device_type == 'PLC':
        from FUNCAUX.FRAMES.spc_frames4plc import PLC_frame
        plc_frame = PLC_frame(d)
        dlgid, response = plc_frame.process()

    if device_type == 'PLCPAY':
        from FUNCAUX.FRAMES.spc_frames4plcpay import PLCPAY_frame
        plcpay_frame = PLCPAY_frame(d)
        dlgid, response = plcpay_frame.process()

    elif device_type == 'SPX':
        from FUNCAUX.FRAMES.spc_frames4spx import SPX_frame
        spx_frame = SPX_frame(d)
        dlgid, response = spx_frame.process()

    elif device_type == 'SP5K':
        from FUNCAUX.FRAMES.spc_frames4sp5k import SP5K_frame
        sp5K_frame = SP5K_frame(d)
        dlgid, response = sp5K_frame.process()

    elif device_type == 'UNKNOWN':
        print('Content-type: text/html\n\n', end='')
        print('<html><body><h1>UNKNOWN</h1></body></html>')
        log(module=__name__, function='u_send_response', level='ERROR', msg='UNKNOWN FRAME')
        exit(0)

    log(module=__name__, function='__init__', level='ALERT', msg='PID:{0} DLGID:{1}: Process OK: RSP={2}'.format(pid, dlgid, response))
    stats.end()
