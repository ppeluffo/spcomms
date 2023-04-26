#!/home/pablo/Spymovil/www/cgi-bin/spcommsV3/venv/bin/python3
'''
Funciones donde implemento los objetos Memblocks.

'MEMBLOCK' = { 'RCVD_MBK_LENGTH': 21, 'RCVD_MBK_DEF': [   ('var1','uchar',0), ('var2','uchar',1), ('var3','float',2), ('var4','short',3), ('var5','short',5)],
             'SEND_MBK_LENGTH': 21, 'SEND_MBK_DEF': [   ('var1','uchar',0), ('var2','uchar',1), ('var3','float',2), ('var4','short',3), ('var5','short',5)],
           }

'REMVARS': {'KYTQ003': [['HTQ1', 'NIVEL_TQ_KIYU']]}

b=pack('ififf',14,123.4,100,3.14,2.7)
b'\x0e\x00\x00\x00\xcd\xcc\xf6Bd\x00\x00\x00\xc3\xf5H@\xcd\xcc,@'
b'\n\x0bf\xe6\xf6Bb\x04W\x02\xecq\xe4C:\x16\x00\x00\x00\x00\x00\x0b\xa3'}

MEMBLOCK = {'RCVD_MBK_DEF': [
                              ['UPA1_CAUDALIMETRO', 'float', 0],['UPA1_STATE1', 'uchar', 1],['UPA1_POS_ACTUAL_6', 'short', 8],
                              ['UPA2_CAUDALIMETRO', 'float', 0],['BMB_STATE18', 'uchar', 1],['BMB_STATE19', 'uchar', 1]
                            ],
             'SEND_MBK_DEF': [
                              ['UPA1_ORDER_1', 'short', 1],['UPA1_CONSIGNA_6', 'short', 2560],['ESP_ORDER_8', 'short', 200],
                              ['NIVEL_TQ_KIYU', 'float', 2560]
                            ],
             'RCVD_MBK_LENGTH':15,
             'SEND_MBK_LENGTH':10
            }

RCVD_MBK_LENGTH y SEND_MBK_LENGTH incluyen los 2 bytes del CRC.!!!
En este caso el len('RCVD_MBK_DEF') es 13 
            
Datos simulados a transmitir por el PLC

d={'UPA1_CAUDALIMETRO': 123.45,
 'UPA1_STATE1': 100,
 'UPA1_POS_ACTUAL_6': 120,
 'UPA2_CAUDALIMETRO': 32.45,
 'BMB_STATE18': 20,
 'BMB_STATE19': 40
 }

 sformat,largo,var_names = mbk.__process_mbk__(mbk.rcvd_mbk_def)
 sformat = '<fBhfBB'
 largo = 13
 var_names = 'UPA1_CAUDALIMETRO UPA1_STATE1 UPA1_POS_ACTUAL_6 UPA2_CAUDALIMETRO BMB_STATE18 BMB_STATE19 '

ntuple = namedtuple('nt', d.keys())(*d.values())
nt(UPA1_CAUDALIMETRO=123.45, UPA1_STATE1=100, UPA1_POS_ACTUAL_6=120, UPA2_CAUDALIMETRO=32.45, BMB_STATE18=20, BMB_STATE19=40)

tx_bytestream = pack( sformat, *ntuple)
b'f\xe6\xf6Bdx\x00\xcd\xcc\x01B\x14('

crc = computeCRC(tx_bytestream)
crc=36181

tx_bytestream += crc.to_bytes(2,'big')
b'f\xe6\xf6Bdx\x00\xcd\xcc\x01B\x14(\x8dU'

 Este es el payload que debo recibir en modo test.

'''

from collections import namedtuple
from struct import unpack_from, pack
from pymodbus.utilities import computeCRC
from FUNCAUX.UTILS.spc_log import log2


class Memblock:
    ''' Defino los objetos memblock que se usan en las comunicaciones de los PLC '''

    def __init__(self):
        self.dlgid = None
        self.rcvd_mbk_def = []
        self.rcvd_mbk_length = 0
        self.send_mbk_def = []
        self.send_mbk_length = 0
        self.rx_payload_stream = ''
        self.rx_payload_bytes = ''
        self.rx_payload_length = 0
        self.d_rx_payload = {}
        self.tx_bytestream = b''

    def load_configuration(self, dlgid, d_mbk):
        ''' Configura el objeto con los elementos del d_mbk '''
        self.dlgid = dlgid
        self.rcvd_mbk_def = d_mbk.get('RCVD_MBK_DEF',[])
        self.rcvd_mbk_length = d_mbk.get('RCVD_MBK_LENGTH',0)
        self.send_mbk_def = d_mbk.get('SEND_MBK_DEF',[])
        self.send_mbk_length = d_mbk.get('SEND_MBK_LENGTH',0)
        #
        log2 ({ 'MODULE':__name__, 'FUNCTION':'process', 'LEVEL':'SELECT',
                   'DLGID':dlgid, 'MSG':f'MBK={d_mbk}'})

    def load_rx_payload(self, d_rx_payload):
        ''' Carga en el objeto mbk los datos del payload '''
        self.rx_payload_stream = d_rx_payload.get('STREAM','')
        self.rx_payload_bytes = d_rx_payload.get('BYTES',b'')
        self.rx_payload_length = d_rx_payload.get('SIZE',0)
        #
        log2 ({ 'MODULE':__name__, 'FUNCTION':'process', 'LEVEL':'SELECT',
                   'DLGID':self.dlgid, 'MSG':f'RX_PAYLOAD={d_rx_payload}'})

    def get_d_rx_payload(self):
        return self.d_rx_payload
    
    def convert_rxbytes2dict( self):
        '''
        Toma el payload; si el crc es correcto lo decodifica de acuerdo a la struct definida en el memblock
        y retorna un dict con las variables y sus valores.
        Utiliza el r_mbk ( RCVD)
        La defincion de la struct puede tener menos bytes que el bloque !!!. Para esto debo usar 'unpack_from'
        '''
        # El CRC debe ser correcto
        if not self.__check_payload_crc_valid__():
            log2 ( { 'MODULE':__name__, 'LEVEL':'ERROR',
                'FUNCTION':'convert_bytes2dict', 'MSG':'MBK_CRC_ERROR' } )
            return False

        # EL payload debe tener largo para ser decodificado de acuerdo al memblock def.
        if self.rx_payload_length < self.rcvd_mbk_length:
            log2 ( { 'MODULE':__name__, 'LEVEL':'ERROR','DLGID':self.dlgid,
                'FUNCTION':'convert_bytes2dict', 'MSG':f'MBK_RCVD_LENGTH_ERROR: payload_length={self.rx_payload_length}, mbk_length={self.rcvd_mbk_length}'} )
            return False
        #
        # Calculo los componentes del memblock de recepcion (formato,largo, lista de nombres)
        sformat, largo, var_names = self.__process_mbk__(self.rcvd_mbk_def)
        # Creo una namedtuple con la lista de nombres del rx_mbk
        t_names = namedtuple('RCVD_VARS_NAMES', var_names)
        # Descompongo el payload recibido de acuerdo al formato dado en un tupla de valores
        try:
            t_vals = unpack_from(sformat, self.rx_payload_bytes)
        except:
            log2 ( { 'MODULE':__name__, 'LEVEL':'ERROR',
                'FUNCTION':'convert_bytes2dict', 'MSG':f'NO PUEDO UNPACK. sformat={sformat}, rx_payload_bytes={self.rx_payload_bytes}'} )
            return False
        #
        # Genero una namedtuple con los valores anteriores y los nombres de las variables
        try:
            rx_tuple = t_names._make(t_vals)
        except:
            log2 ( { 'MODULE':__name__, 'LEVEL':'ERROR',
                'FUNCTION':'convert_bytes2dict', 'MSG':'NO PUEDO GENERAR LA TUPLA DE LOS VALORES.!!'} )
            return False
        #
        # La convierto a diccionario
        self.d_rx_payload = rx_tuple._asdict()
        #log2 ( { 'MODULE':__name__, 'LEVEL':'SELECT','DLGID':self.dlgid,
        #        'FUNCTION':'convert_bytes2dict', 'MSG':f'd_rx_payload={self.d_rx_payload}'} )
       
        return True
 
    def convert_dict2bytes( self, dlgid, d_data ):
        '''
        Recibo un diccionario con variables definidas en la estructura de un plc memblock.
        Utiliza el s_mbk ( SEND )
        El d_data puede tener mas variables que las que tiene el mbk.
        Serializo la estructura de acuerdo al mbk.!! (solo puedo mandar lo que dicta el memblock )
        Relleno con 0 hasta completar el largo del memblock
        Agrego el CRC del largo del memblock
        Retorno un bytearray.
        '''
        self.dlgid = dlgid
        d_payload = {}
        for t in self.send_mbk_def:
            # t = ['ESP_ORDER_8', 'short', 200]
            dst_var_name, _, dst_default_value = t
            d_payload[dst_var_name] = d_data.get(dst_var_name, dst_default_value)
        #
        # En d_payload tengo todas las variables definidas en el send_mbk con sus valores reales o x defecto
        log2 ({ 'MODULE':__name__, 'FUNCTION':'convert_dict2bytes', 'LEVEL':'SELECT',
                'DLGID':self.dlgid, 'MSG':f'D_RESP_PAYLOAD={d_payload}'})    
        #
        # Convierto el diccionario a una namedtuple (template)
        sformat, largo, var_names = self.__process_mbk__(self.send_mbk_def)
        ntuple = namedtuple('nt', d_payload.keys())(*d_payload.values())
        #
        # Convierto la ntuple a un bytearray serializado
        try:
            self.tx_bytestream = pack( sformat, *ntuple)
        except:
            log2 ({ 'MODULE':__name__, 'FUNCTION':'convert_dict2bytes', 'DLGID':self.dlgid, 'MSG':f'ERROR CONVERT NTUPLE={ntuple}'}) 
            return self.tx_bytestream
        #
        # Controlo errores: el payload no puede ser mas largo que el tamaño del bloque (frame)
        if len(self.tx_bytestream) > self.send_mbk_length:
            log2 ({ 'MODULE':__name__, 'FUNCTION':'convert_dict2bytes', 'DLGID':self.dlgid, 
                   'MSG':f'LENGTH ERROR: tx_bytestream={len(self.tx_bytestream)}, send_mbk_length={self.send_mbk_length}'}) 
            return self.tx_bytestream
        #
        # Relleno con 0 el bloque
        largo_relleno = self.send_mbk_length - len(self.tx_bytestream)
        relleno = bytes(largo_relleno*[0])
        self.tx_bytestream += relleno
        #
        # Calculo el CRC y lo agrego al final. Lo debo convertir a bytes antes.
        crc = computeCRC(self.tx_bytestream)
        self.tx_bytestream += crc.to_bytes(2,'big')
        #
        return self.tx_bytestream

    def __check_payload_crc_valid__(self):
        '''
        Calcula el CRC del payload y lo compara el que trae.
        El payload trae el CRC que envia el PLC en los 2 ultimos bytes
        El payload es un bytestring
        '''
        crc = int.from_bytes(self.rx_payload_bytes[-2:],'big')
        calc_crc = computeCRC(self.rx_payload_bytes[:-2])
        if crc == calc_crc:
            return True
        else:
            return False
    
    def __process_mbk__( self, l_mbk_def:list ):
        '''
        Toma una lista de definicion de un memblok de recepcion y genera 3 elementos:
        -un iterable con los nombres en orden
        -el formato a usar en la conversion de struct
        -el largo total definido por las variables
        '''
        #
        sformat = '<'
        largo = 0
        var_names = ''
        for ( name, tipo, _) in l_mbk_def:
            var_names += f'{name} '
            if tipo.lower() == 'char':
                sformat += 'c'
                largo += 1
            elif tipo.lower() == 'schar':
                sformat += 'b'
                largo += 1
            elif tipo.lower() == 'uchar':
                sformat += 'B'
                largo += 1
            elif tipo.lower() == 'short':
                sformat += 'h'
                largo += 2
            elif tipo.lower() == 'int':
                sformat += 'i'
                largo += 4
            elif tipo.lower() == 'float':
                sformat += 'f'
                largo += 4
            elif tipo.lower() == 'unsigned':
                sformat += 'H'
                largo += 2
            else:
                sformat += '?'
        #
        return sformat, largo, var_names
