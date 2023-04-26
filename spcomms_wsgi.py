#!/home/pablo/Spymovil/www/cgi-bin/spcommsV3/venv/bin/python3
"""
Servidor WSGI de comunicaciones.

https://stackoverflow.com/questions/24150077/how-do-i-convert-cgi-to-wsgi
https://realpython.com/django-nginx-gunicorn/#:~:text=Gunicorn%20implements%20the%20Web%20Server,to%20thousands%20of%20simultaneous%20connections.

Procesa los frames de los dispositivos SPXR3, SPXR3, PLCV2
Corre debajo de gunicorn !!
Testing:
    gunicorn --bind 0.0.0.0:8000 spcomms_wsgi:application
# 
La entrada y salida la maneja de acuerdo a la recomendacion wsgi.
Con la entrada genera un diccionario que pasa al procesamiento.

d_input =  { 
    'GET':{ QS', 'SIZE'}, 
    'POST': {'STREAM', 'BYTES', 'SIZE' } 
}

d_output = {
    'DLGID':
    'RSP_PAYLOAD':
}
Para testear tenemos el script test_spcommsV2.py
o sino
Modo de testing:
telnet localhost 80
GET /cgi-bin/spcommsV3/spcommsV3.py?ID=PABLO&TYPE=SPXR3&VER=1.0.0&CLASS=PING
HTTP/1.1
Host: www.spymovil.com


Pendiente:

5- Testing con prueba de carga
6- Grafana: 
    incorporar probes prometheus
    Visualizar graficas.
7- Pasar a docker y github
8- Profilear el codigo (vcode + austin)
---------------------------------------------------------------------------
"""

from FUNCAUX.UTILS.spc_log import config_logger, log2, set_debug_dlgid
from FUNCAUX.PROTOCOLOS import selector_protocolo, protocolo_SPXR3, protocolo_PLCR2
from FUNCAUX.UTILS import spc_stats
from FUNCAUX.SERVICIOS import servicios

VERSION = '0.0.4 @ 2023-04-11'

# -----------------------------------------------------------------------------
def application (environ, start_response):
    '''
    Aplicacion WSGI. Es a donde apunta el Gunicorn.
    gunicorn --bind 0.0.0.0:8000 spcomms_wsgi:application
    La entrada puede ser GET (PLC,PLCPAY,SPX,...) o POST ( OCEANUS ) o ambas
    Responde con un diccionario con las claves GET y POST:
    d{ 'GET':{ QS', 'SIZE'}, 
       'POST': {'STREAM', 'BYTES', 'SIZE' } 
    }
    '''
    #
    spc_stats.init_stats()

    # Lo primero es configurar el logger
    config_logger('SYSLOG')
    #
    # Leo la entrada (WSGI)
    # Parte GET:
    get_query_string = environ['QUERY_STRING']
    body_size = int(environ.get('CONTENT_LENGTH', 0))
    d_get = { 'QS': get_query_string, 'SIZE': body_size }
    #
    # Parte POST:
    try:
        post_body_size = int(environ.get('CONTENT_LENGTH', 0))
    except (ValueError):
        post_body_size = 0
    post_body_stream = environ['wsgi.input'].read(post_body_size)
    #
    d_post = { 'STREAM': post_body_stream,
              'BYTES': post_body_stream,
              'SIZE': post_body_size
              }
    #
    d_input = {'GET':d_get, 'POST':d_post }
    #
    # Proceso los frames --------------------------------------
    d_output = main(d_input)
    # ---------------------------------------------------------
    #
    # Response
    dlgid =d_output.get('DLGID','00000')
    tag = d_output.get('TAG',0)
    response_body = d_output.get('RSP_PAYLOAD','ERROR')
    status = '200 OK'
    #('Content-Type', 'text/plain'),
    response_headers = [ 
        ('Content-Type', 'application/x-binary'),
        ('Content-Length', str(len(response_body)))
    ]
    # Envio parte de la respuesta al Gurnicorn
    start_response(status, response_headers)
    #
    log2 ( { 'MODULE':__name__,
            'DLGID':dlgid,
            'FUNCTION':'send_response','MSG':f'({tag}) RSP=>{response_body}' }
        )
    # Termino de enviar la respuesta.
    return [response_body]

def main(d_input):
    '''
    Funciones especificas del servidor spcomms V3.
    '''
    # Determino que servicio va a atender al frame en virtud del prtocolo de este.
    prot_selector = selector_protocolo.SelectorProtocolo()
    protocolo = prot_selector.decode_protocol(d_input)
    #
    # Leemos el DEBUG_DLGID para setearlo bien al ppio.
    endpoint = 'READ_DEBUG_DLGID'
    params = {'LOG':False}
    servicio = servicios.Servicios()
    response = servicio.process(params=params, endpoint=endpoint)
    if response.status_code() == 200:
        debug_dlgid = response.json().get('DEBUG_DLGID','00000')
        set_debug_dlgid(debug_dlgid)
    #
    # Todos los protocolos me devuelven un dict con los datos de la salida.
    d_output = {}
    if protocolo == 'SPXR3':
        # Incluye los protocolos SPXR2 y SPXR3
        d_output = protocolo_SPXR3.ProtocoloSPXR3().process_protocol(d_input)
    elif protocolo == 'PLCR2':
        d_output = protocolo_PLCR2.ProtocoloPLCR2().process_protocol(d_input)
    else:
        log2 ({ 'MODULE':__name__, 'FUNCTION':'process', 'LEVEL':'INFO',
        'MSG':f'ERROR: PROTOCOLO NO DEFINIDO {protocolo}'})
        d_output = {}
    #
    # Actualizo las estadisticas
    endpoint = 'READ_QUEUE_LENGTH'
    dlgid = d_output.get('DLGID','00000')
    params = { 'QUEUE_NAME': 'STATS_QUEUE' }
    response = servicio.process(params=params, endpoint=endpoint)
    stats_queue_length = 0
    if response.status_code() == 200:
        stats_queue_length = response.json().get('QUEUE_LENGTH',0)
    spc_stats.set_stats_queue_length(stats_queue_length)
    #   
    params = { 'QUEUE_NAME': 'RXDATA_QUEUE' }
    response = servicio.process(params=params, endpoint=endpoint)
    data_queue_length = 0
    if response.status_code() == 200:
        data_queue_length = response.json().get('QUEUE_LENGTH',0)
    spc_stats.set_data_queue_length(data_queue_length)
    #
    # Save stats
    dlgid = d_output.get('DLGID','00000')
    d_stats = spc_stats.read_d_stats()
    endpoint = 'SAVE_STATS'
    params = { 'DLGID':dlgid,'DSTATS':d_stats }
    _ = servicio.process(params=params, endpoint=endpoint)
    #
    d_statistics = spc_stats.end_stats()
    # Log
    msg = f'STATS: elapsed={d_statistics["duracion_frame"]:.04f},'
    msg += f'cfconf={d_statistics["count_frame_config_base"]},'
    msg += f'cfdata={d_statistics["count_frame_data"]},'
    msg += f'cRedis={ d_statistics["count_accesos_REDIS"]},'
    msg += f'cSql={d_statistics["count_accesos_SQL"]},'
    msg += f'stats_QUEUE={d_statistics["length_stats_queue"]},'
    msg += f'data_QUEUE={d_statistics["length_data_queue"]}'
    d_log = { 'MODULE':__name__, 'FUNCTION':'STATS', 'LEVEL':'INFO', 'MSG':msg }
    log2(d_log)
    #
    return d_output


