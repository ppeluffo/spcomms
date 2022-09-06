#!/opt/anaconda3/envs/mlearn/bin/python3
#!/usr/bin/python3 -u

import os
import sys
import cgi
# import cStringIO
from FUNCAUX.UTILS.spc_log import *
import redis
import pickle

# -----------------------------------------------------------------------------

if __name__ == '__main__':


    # Lo primero es configurar el logger
    config_logger('SYSLOG')
    query_string = ''
    rh = redis.Redis(host='127.0.0.1')
    #rh.hset('PABLO','POST','BIEN')

     # Leo del cgi
    args = sys.stdin.read()
    query_string = os.environ.get('QUERY_STRING')

    log(module=__name__, function='__init__', level='INFO', msg='TEST: QS={0}'.format(query_string))

    # Args tiene codificacion UNICODE.
    p3 = args.encode('ascii', 'surrogateescape')
    # p3 son bytes. Lo convierto a una lista de caracteres imprimibles. ( descarto el resto )
    payload = []
    for i in p3:
        if i>33 and i<126:
            payload.append(chr(i))
    # Lo transformo en un string
    s_payload = ''.join(payload)

    #p1=pickle.dumps(args)
    #rh.hset('PABLO', 'POST', p1)

    # copyInput = cStringIO.StringIO(sys.stdin.read())
    log(module=__name__, function='__init__', level='INFO', msg='ARGS TYPE = {0}'.format(type(args)))
    log(module=__name__, function='__init__', level='INFO', msg='ARGS LEN = {0}'.format(len(args)))
    log(module=__name__, function='__init__', level='INFO', msg='TEST: POST_IN')
    log(module=__name__, function='__init__', level='INFO', msg='TEST: POST={0}'.format(args))
    log(module=__name__, function='__init__', level='INFO', msg='TEST: P3={0}'.format(p3))
    log(module=__name__, function='__init__', level='INFO', msg='TEST: SP3={0}'.format(s_payload))
    log(module=__name__, function='__init__', level='INFO', msg='TEST: POST_OUT')

    if query_string is None:
        log(module=__name__, function='__init__', level='ALERT', msg='ERROR QS NULL')
        exit(1)

    print('Content-type: text/html\n\n', end='')
    print('<html><body><h1>TEST</h1></body></html>')
    log(module=__name__, function='u_send_response', level='ERROR', msg='OK')
    exit(0)
