#!/home/pablo/Spymovil/www/cgi-bin/spcommsV3/venv/bin/python3u

'''
Funciones de uso general.
'''

from FUNCAUX.UTILS.spc_log import log2
import datetime as dt
import re

TRACE_DEBUG=True

def version2int (str_version):
    '''
    VER tiene un formato tipo A.B.C.
    Lo convertimos a un numero A*100 + B*10 + C
    '''
    return int(re.sub(r"[A-Z,a-z,.]",'',str_version))

def u_hash( seed, line ):
    '''
    Calculo un hash con el string pasado en line.
    Devuelve un entero
    Se utiliza el algoritmo de Pearson
    https://es.abcdef.wiki/wiki/Pearson_hashing
    La función original usa una tabla de nros.aleatorios de 256 elementos.
	Ya que son aleatorios, yo la modifico a algo mas simple.

    Es la misma implementacion que se usa en los dataloggers.
    '''

    hash_table = [ 93,  153, 124,  98, 233, 146, 184, 207, 215,  54, 208, 223, 254, 216, 162, 141,
		 10,  148, 232, 115,   7, 202,  66,  31,   1,  33,  51, 145, 198, 181,  13,  95,
		 242, 110, 107, 231, 140, 170,  44, 176, 166,   8,   9, 163, 150, 105, 113, 149,
		 171, 152,  58, 133, 186,  27,  53, 111, 210,  96,  35, 240,  36, 168,  67, 213,
		 12,  123, 101, 227, 182, 156, 190, 205, 218, 139,  68, 217,  79,  16, 196, 246,
		 154, 116,  29, 131, 197, 117, 127,  76,  92,  14,  38,  99,   2, 219, 192, 102,
		 252,  74,  91, 179,  71, 155,  84, 250, 200, 121, 159,  78,  69,  11,  63,   5,
		 126, 157, 120, 136, 185,  88, 187, 114, 100, 214, 104, 226,  40, 191, 194,  50,
		 221, 224, 128, 172, 135, 238,  25, 212,   0, 220, 251, 142, 211, 244, 229, 230,
		 46,   89, 158, 253, 249,  81, 164, 234, 103,  59,  86, 134,  60, 193, 109,  77,
		 180, 161, 119, 118, 195,  82,  49,  20, 255,  90,  26, 222,  39,  75, 243, 237,
		 17,   72,  48, 239,  70,  19,   3,  65, 206,  32, 129,  57,  62,  21,  34, 112,
		 4,    56, 189,  83, 228, 106,  61,   6,  24, 165, 201, 167, 132,  45, 241, 247,
		 97,   30, 188, 177, 125,  42,  18, 178,  85, 137,  41, 173,  43, 174,  73, 130,
		 203, 236, 209, 235,  15,  52,  47,  37,  22, 199, 245,  23, 144, 147, 138,  28,
		 183,  87, 248, 160,  55,  64, 204,  94, 225, 143, 175, 169,  80, 151, 108, 122 ]

    h = seed
    for c in line:
        h = hash_table[h ^ ord(c)]
    return h

def trace_request(endpoint='', params={}, msg=''):
    ''' Loguea la informacion de un api request'''
    dlgid = params.get('DLGID','00000')
    log_allowed = params.get('LOG',True)
    if log_allowed:
        log2 ( { 'MODULE':__name__, 'DLGID':dlgid, 'LEVEL':'SELECT',
                'FUNCTION':'trace_request',
                'MSG':f'{msg}=>endpoint:{endpoint}, params{params}' } )

def trace_response(response=None, msg=''):
    ''' Loguea la informacion de una api response'''
    json = response.json()
    dlgid = response.dlgid()
    log_allowed = json.get('LOG',True)
    if log_allowed:
        log2 ( { 'MODULE':__name__, 'DLGID':dlgid, 'LEVEL':'SELECT',
                'FUNCTION':'trace_response',
                'MSG':f'{msg}=>status_code={response.status_code()},reason={response.reason()},json={json}' } )

def normalize_querystring(query_string):
    '''
    Normaliza los caracteres de separador de campo y de keys
    '''
    qs = ''
    if 'SPXR3' in query_string:
         qs = query_string
    elif 'SPXR2' in query_string:
        # Normalizo los separadores de campo y keys
        qs = query_string.replace(';','&').replace(':','=')
    elif 'PLCR2' in query_string:
        qs = query_string.replace(';','&').replace(':','=')
    elif 'PLCR3' in query_string:
        qs = query_string
    return qs
  
def translate_rsp_payload(proto, str_payload):
    '''
    Convierto los caracteres normales de la respuesta a los particulares del protocolo SPXR2
    '''
    t_payload = ''
    if proto == 'SPXR3':
        t_payload = str_payload
    elif proto == 'SPXR2':
        # Normalizo los separadores de campo y keys
        t_payload = str_payload.replace('&',';').replace('=',':')
    return t_payload
    
def normalize_frame_data(proto, d_payload):
    '''
    Funcion general que procesa los frames de data.
    En el protocolo SPXR2 no tienen los campos DATE ni TIME por lo que se usa
    este paso de normalizacion.
    '''
    d_p = {}
    if proto == 'SPXR3':
        d_p = d_payload
    elif proto == 'SPXR2':
        # Agrego los campos DATE y TIME para normalizarlos al protocolo SPXV3
        # 'DATE': '230417', 'TIME': '161057'
        now = dt.datetime.now()
        d_p['DATE'] = now.strftime('%y%m%d')
        d_p['TIME'] = now.strftime('%H%M%S')
    return d_p