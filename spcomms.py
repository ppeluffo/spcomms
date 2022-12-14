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
#
Para activar el debug selectivo, en el server de REDIS 192.168.0.6 hay que dar un comando del tipo:
hset 'SPCOMMS' 'DEBUG_DLGID' 'PLCTEST'
----------------------------------------------------------------------------------------------------
Version 1.3.0 @ 2023-01-10:
- Funciones para procesar datos de los PLCR2.
  Manejo de memblocks.
  Process
  Actualizacion de redis
  Funciones de uso de la nueva BD PGSQL atvise.

----------------------------------------------------------------------------------------------------
Version 1.2.5 @ 2022-12-26:
- Ajusto el nivel de los logs para no llenar el disco
----------------------------------------------------------------------------------------------------
Version 1.2.4 @ 2022-12-10:
- Creo una nuevo tipo de frames (SPXR2) para los nuevos dataloggers.

----------------------------------------------------------------------------------------------------
Version 1.2.3 @ 2022-10-26:
Problema: Cuando tenemos una red de automatismos, cuando el TANQUE envia los datos, no se hace el
          broadcast de las variables a la REDIS:MODBUS:DLG_REMOTOS. Por lo tanto los remotos no tienen datos
          para leer en sus frames de respuestas.
          El programa SI considera la REDIS:MODBUS al armar el frame ya que en los casos que tenemos
          un mix de tanques (spy.py) con perforaciones (spcomms.py) funciona. ( rh.get_modbusline() )
          El tema es en la RECEPCION y PROCESS del frame de spcomms.py que no hacemos el broadcast_local_vars().
Solucion: Copiamos el broadcast_local_vars() de spy.py en spcomm.py y lo adaptamos.
          Esto lo hacemos en el PROCESS.

----------------------------------------------------------------------------------------------------
Version 1.2.2 @ 2022-10-03:
Agrego el tipo PLCR2 en el cual manejamos el concepto de los memblocks.
En el post viene un memblock con determinada definicion de variables.
Es la forma de comunicarse la nueva version de firmware de PLCs

----------------------------------------------------------------------------------------------------
Version 1.2.1 @ 2022-10-03:
Se agrega que en los PLC si no existe el registro redis, lo cree.

----------------------------------------------------------------------------------------------------
Version 1.2.0 @ 2022-09-13
Todos los protocolos traern una parte de GET y luego, los de OCEANUS traen un POST.
Esto hace que primero leo siempre el GET y luego decodifico. Si el protocolo es POST, leo el stdin.
Agrego que el datalogger que se loguea especificamente se escriba en REDIS y no se toque mas el spcomms.conf
El nuevo archivo de configuracion es spcomms.conf

----------------------------------------------------------------------------------------------------
Version 1.1.0 @ 2022-08-31
Agrego las clases para el manejo de 2 nuevos tipos de frames: OCEANUS y GENERICO.
OCEANUS procesa los datos que vienen de las estaciones meteorologicas OCEANUS
GENERICO procesa dato comunes. Solo para ver que algo llegue al servidor. No almacena nada.

-----------------------------------------------------------------------------------------------------
Version: 1.0.1 @ 2022-08-05
1) Excepcion 001:
Se coloca en spc_frames4spx.py para el manejo de los PLC de PAYSANDU.
2) La inserci??n del LINE a la redis se hace en spyplc.py y no en xprocess porque los automatismos (paysand??)
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
#GET /cgi-bin/COMMS/spcomms.py?ID:PABLO;TYPE:SP5K;VER:4.0.4a;PA:3.21;PB:1.34;H:4.56;bt:10.11
GET /cgi-bin/COMMS/spcomms.py?ID:SJTQ001;TYPE:PLC;VER:4.0.4a;LTQ:1.68;LTQ1:1.70;bt:14.38
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
from FUNCAUX.UTILS import spc_stats as stats


version = '1.2.5 @ 2022-12-26'
# -----------------------------------------------------------------------------


def decode_input ( query_string ):
    '''
    La entrada puede ser GET (PLC,PLCPAY,SPX,...) o POST ( OCEANUS )
    Todos los frames tienen un query_string que me va a determinar el TYPE.
    Es lo primero que parseo
    '''
    if query_string is None:
        log(module=__name__, function='decode_input', level='ALERT', msg='ERROR QS NULL')
        exit(1)

    l_elements = query_string.split(';')

    d_tmp = { k:v for (k, v) in [x.split(':') for x in query_string.split(';') if ':' in x ] }
    d = {}
    d['QUERY_STRING'] = query_string
    d['TYPE'] = d_tmp['TYPE']
    #
    log(module=__name__, function='decode_input', level='ERROR', msg='QS1: {0}'.format(query_string))
    log(module=__name__, function='decode_input', level='ERROR', msg='TYPE: {0}'.format(d['TYPE']))
    #
    # Solo me quedo con ID,TYPE,VER, y el resto es PAYLOAD si es GET. Si es POST el payload esta en stdin_args
    for s in l_elements[0:3]:
        (key,value) = s.split(':')
        d[key] = value
    #

    # De acuerdo al campo TYPE parseo el PAYLOAD
    if ( d['TYPE'] in ['PLC', 'PLCPAY', 'SPX', 'SP5K']):
        # En estos tipos el payload sale del GET
        # Rearmo el payload
        payload = ''
        for s in l_elements[3:]:
            payload += '{0};'.format(s)
        # Elimino el ultimo ';'
        payload = payload[:-1]
        # El payload no puede tener caracteres no numericos al final
        while not payload[-1].isdigit():
            payload = payload[:-1]
        d['PAYLOAD'] = payload
        pid = os.getpid()
        log(module=__name__, function='__init__', level='INFO', msg='PID:{0} RX:[{1}]'.format(pid, query_string))

    elif ( d['TYPE'] in ['SPXR2'] ):
        # El payload puede terminar en letras cuando trae el cks por lo tanto lo debo procesar en particular.
        payload = ''
        for s in l_elements[3:]:
            payload += f'{s};'
        # Elimino el ultimo ';'
        payload = payload[:-1]
        d['PAYLOAD'] = payload
        pid = os.getpid()
        log(module=__name__, function='__init__', level='INFO', msg='PID:{0} RX:[{1}]'.format(pid, query_string))

    elif ( d['TYPE'] in ['OCEANUS', 'GENERICO']):
        # El payload esta en el stdin_args del POST
        # stdin_args tiene codificacion UNICODE.
        # Leo el stdin por si llego como POST
        stdin_args = sys.stdin.read()
        stdin_args_bytes = stdin_args.encode('ascii', 'surrogateescape')
        # stdin_args_bytes son bytes. Lo convierto a una lista de caracteres imprimibles. ( descarto el resto )
        l_payload = []
        for i in stdin_args_bytes:
            if i > 33 and i < 126:
                l_payload.append(chr(i))
        # Lo transformo en un string
        payload = ''.join(l_payload)
        d['PAYLOAD'] = payload
        log(module=__name__, function='decode_input', level='INFO', msg='POST RX PAYLOAD:[{0}]'.format(payload))

    elif ( d['TYPE'] in ['PLCR2']):
        # El payload esta en el stdin_args del POST
        # stdin_args tiene codificacion UNICODE.
        # Leo el stdin por si llego como POST
        stdin_args = sys.stdin.read()
        stdin_args_bytes = stdin_args.encode('ascii', 'surrogateescape')
        # stdin_args_bytes son bytes.
        d['PAYLOAD'] = stdin_args_bytes
        log(module=__name__, function='decode_input', level='INFO', msg='POST PLCR2 RX PAYLOAD:[{0}]'.format(d['PAYLOAD']))

    else:
        # No reconozco el tipo.
        log(module=__name__, function='decode_input', level='ALERT', msg='ERROR TYPE NO DETERMINADO')
        exit(1)
    #
    return d


if __name__ == '__main__':

    # Lo primero es configurar el logger
    config_logger('SYSLOG')
    query_string = ''
    stats.init()
    # Leo el cgi GET ( Todos los protocolos lo traen )
    query_string = os.environ.get('QUERY_STRING')
    d = decode_input( query_string )
    #
    device_type = d.get('TYPE', 'UNKNOWN')
    dlgid = None
    response = None
    #
    if device_type == 'PLC':
        from FUNCAUX.FRAMES.spc_frames4plc import PLC_frame
        plc_frame = PLC_frame(d)
        dlgid, response = plc_frame.process()

    elif device_type == 'PLCPAY':
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

    elif device_type == 'OCEANUS':
        from FUNCAUX.FRAMES.spc_frames4oceanus import OCEANUS_frame
        oceanus_frame = OCEANUS_frame(d)
        dlgid, response = oceanus_frame.process()

    elif device_type == 'GENERICO':
        from FUNCAUX.FRAMES.spc_frames4generico import GENERICO_frame
        generico_frame = GENERICO_frame(d)
        dlgid, response = generico_frame.process()

    elif device_type == 'PLCR2':
        from FUNCAUX.FRAMES.spc_frames4plcR2 import PLCR2_frame
        plcR2_frame = PLCR2_frame(d)
        dlgid, response = plcR2_frame.process()

    elif device_type == 'SPXR2':
        from FUNCAUX.FRAMES.spc_framesBase_spxR2 import SPXR2_FRAME
        spxR2_frame = SPXR2_FRAME(d)
        dlgid, response = spxR2_frame.process()

    elif device_type == 'UNKNOWN':
        print('Content-type: text/html\n\n', end='')
        print('<html><body><h1>UNKNOWN</h1></body></html>')
        log(module=__name__, function='u_send_response', level='ERROR', msg='UNKNOWN FRAME')
        exit(0)

    log(module=__name__, function='__init__', level='INFO', msg='DLGID:{0}: Status OK: RSP={1}'.format(dlgid, response))
    stats.end()
