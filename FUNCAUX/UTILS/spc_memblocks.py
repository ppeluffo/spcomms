#!/opt/anaconda3/envs/mlearn/bin/python3
'''
Implementa la clase con los metodos necesarios para convertir un bytestream en un diccionario de acuerdo
a las definiciones de variables dentro del bloque de memoria.
Se usa para que los PLC manden bloques de memoria serializados y no tengamos que usar un protocolo que agregue
al payload lo que es cada variable.
La definicion de estas debe estar en el servidor.

pip install -U pymodbus

                                    ('var6','float',6),
                                    ('var7','float',7),
                                    ('var8','uchar',8),
                                    ('var9','unsigned',9),
                                    ('crc','unsigned',10),
                                    ]

'''

from collections import namedtuple
from struct import *
from FUNCAUX.UTILS.spc_log import log
from pymodbus.utilities import computeCRC
from FUNCAUX.BD.spc_bd_redis import BD_REDIS


#PLC_MEMBLOCK_DEFINITION_V1_0 = [('count','int',0),('endian','float',1),('version','int',2),('var1','float',3),('var2','float',4)]
MBK_KIYU = { 'RCVD_MBK_LENGTH': 21, 'RCVD_MBK_DEF': [   ('var1','uchar',0), ('var2','uchar',1), ('var3','float',2), ('var4','short',3), ('var5','short',5)],
             'SEND_MBK_LENGTH': 21, 'SEND_MBK_DEF': [   ('var1','uchar',0), ('var2','uchar',1), ('var3','float',2), ('var4','short',3), ('var5','short',5)],
           }

MBK_KIYU2 = {'REMVARS': {'KYTQ003': [['HTQ1', 'NIVEL_TQ_KIYU']]},
 'MEMBLOCK': {'RCVD_MBK_DEF': [['UPA1_CAUDALIMETRO', 'float', 0],
   ['UPA1_DUMMY1', 'uchar', 1],
   ['UPA1_DUMMY2', 'uchar', 1],
   ['UPA1_STATE1', 'uchar', 1],
   ['UPA1_STATE2', 'uchar', 2],
   ['UPA1_STATE3', 'uchar', 3],
   ['UPA1_STATE4', 'uchar', 4],
   ['UPA1_STATE5', 'uchar', 5],
   ['UPA1_STATE6', 'uchar', 6],
   ['UPA1_STATE7', 'uchar', 7],
   ['UPA1_POS_ACTUAL_6', 'short', 8],
   ['UPA1_POS_ACTUAL_7', 'short', 9],
   ['UPA2_CAUDALIMETRO', 'float', 0],
   ['UPA2_DUMMY1', 'uchar', 1],
   ['UPA2_DUMMY2', 'uchar', 1],
   ['UPA2_STATE1', 'uchar', 1],
   ['UPA2_STATE2', 'uchar', 2],
   ['UPA2_STATE3', 'uchar', 3],
   ['UPA2_STATE4', 'uchar', 4],
   ['UPA2_STATE5', 'uchar', 5],
   ['UPA2_STATE6', 'uchar', 6],
   ['UPA2_STATE7', 'uchar', 7],
   ['UPA2_POS_ACTUAL_6', 'short', 8],
   ['UPA2_POS_ACTUAL_7', 'short', 9],
   ['UPA3_CAUDALIMETRO', 'float', 0],
   ['UPA3_DUMMY1', 'uchar', 1],
   ['UPA3_DUMMY2', 'uchar', 1],
   ['UPA3_STATE1', 'uchar', 1],
   ['UPA3_STATE2', 'uchar', 2],
   ['UPA3_STATE3', 'uchar', 3],
   ['UPA3_STATE4', 'uchar', 4],
   ['UPA3_STATE5', 'uchar', 5],
   ['UPA3_STATE6', 'uchar', 6],
   ['UPA3_STATE7', 'uchar', 7],
   ['UPA3_POS_ACTUAL_6', 'short', 8],
   ['UPA3_POS_ACTUAL_7', 'short', 9],
   ['ESP_CAUDALIMETRO', 'float', 0],
   ['ESP_SENSOR_NIVEL_POZO', 'short', 9],
   ['ESP_STATE1', 'uchar', 1],
   ['ESP_STATE2', 'uchar', 1],
   ['ESP_STATE3', 'uchar', 1],
   ['ESP_STATE4', 'uchar', 1],
   ['ESP_STATE5', 'uchar', 1],
   ['ESP_STATE6', 'uchar', 1],
   ['ESP_STATE7', 'uchar', 1],
   ['ESP_STATE8', 'uchar', 1],
   ['TQ_NIVEL_CLARA', 'short', 1],
   ['TQ_NIVEL_BRUTA', 'short', 1],
   ['BMB_STATE1', 'uchar', 1],
   ['BMB_STATE2', 'uchar', 1],
   ['BMB_STATE3', 'uchar', 1],
   ['BMB_STATE4', 'uchar', 1],
   ['BMB_STATE5', 'uchar', 1],
   ['BMB_STATE6', 'uchar', 1],
   ['BMB_STATE7', 'uchar', 1],
   ['BMB_STATE8', 'uchar', 1],
   ['BMB_STATE9', 'uchar', 1],
   ['BMB_STATE10', 'uchar', 1],
   ['BMB_STATE11', 'uchar', 1],
   ['BMB_STATE12', 'uchar', 1],
   ['BMB_STATE13', 'uchar', 1],
   ['BMB_STATE14', 'uchar', 1],
   ['BMB_STATE15', 'uchar', 1],
   ['BMB_STATE16', 'uchar', 1],
   ['BMB_STATE17', 'uchar', 1],
   ['BMB_STATE18', 'uchar', 1],
   ['BMB_STATE19', 'uchar', 1]],
  'SEND_MBK_DEF': [['UPA1_ORDER_1', 'short', 1],
   ['UPA1_ORDER_2', 'short', 200],
   ['UPA1_ORDER_3', 'short', 200],
   ['UPA1_ORDER_4', 'short', 200],
   ['UPA1_ORDER_5', 'short', 200],
   ['UPA1_CONSIGNA_6', 'short', 2560],
   ['UPA1_CONSIGNA_7', 'short', 2560],
   ['UPA2_ORDER_1', 'short', 200],
   ['UPA2_ORDER_2', 'short', 200],
   ['UPA2_ORDER_3', 'short', 200],
   ['UPA2_ORDER_4', 'short', 200],
   ['UPA2_ORDER_5', 'short', 200],
   ['UPA2_CONSIGNA_6', 'short', 2560],
   ['UPA2_CONSIGNA_7', 'short', 2560],
   ['UPA3_ORDER_1', 'short', 200],
   ['UPA3_ORDER_2', 'short', 200],
   ['UPA3_ORDER_3', 'short', 200],
   ['UPA3_ORDER_4', 'short', 200],
   ['UPA3_ORDER_5', 'short', 200],
   ['UPA3_CONSIGNA_6', 'short', 2560],
   ['UPA3_CONSIGNA_7', 'short', 2560],
   ['ESP_ORDER_1', 'short', 200],
   ['ESP_ORDER_2', 'short', 200],
   ['ESP_ORDER_3', 'short', 200],
   ['ESP_ORDER_4', 'short', 200],
   ['ESP_ORDER_5', 'short', 200],
   ['ESP_ORDER_6', 'short', 200],
   ['ESP_ORDER_7', 'short', 200],
   ['ESP_ORDER_8', 'short', 200],
   ['NIVEL_TQ_KIYU', 'float', 2560]],
  'RCVD_MBK_LENGTH': 151,
  'SEND_MBK_LENGTH': 151}}


class MEMBLOCK:
    '''
    Definimos las operaciones para manejar los memblocks que son las definiciones de la memoria de los PLC
    '''
    # b=pack('ififf',14,123.4,100,3.14,2.7)
    # b'\x0e\x00\x00\x00\xcd\xcc\xf6Bd\x00\x00\x00\xc3\xf5H@\xcd\xcc,@'
    # b'\n\x0bf\xe6\xf6Bb\x04W\x02\xecq\xe4C:\x16\x00\x00\x00\x00\x00\x0b\xa3'}

    def __init__(self):
        self.d_mbk = None
        self.r_mbk_sformat = None
        self.r_mbk_largo = 0
        self.r_mbk_ntuple = None
        self.r_mbk_d_vars = None
        self.s_mbk_sformat = None
        self.s_mbk_largo = 0
        self.s_mbk_ntuple = None
        self.s_mbk_d_vars = None

    def load(self, dlgid=None):
        '''
        Lee la defincicion del memblock de la REDIS ( o GDA )
        '''
        #self.d_mbk = MBK_KIYU2
        #return;

        rh = BD_REDIS()
        self.d_mbk = rh.get_memblock(dlgid)
        if self.d_mbk is None:
            self.d_mbk = MBK_KIYU2
            return False
        #
        return True

    def set_d_mem(self, d_mbk=None):
        '''
        Inicializo con la definicion de un memblock. ( Lo deberia leer de redis)
        '''
        self.d_mbk = d_mbk

    def get_d_mbk(self):
        '''
        Devuelve el memblock configurado
        '''
        return self.d_mbk

    def reset_mbk(self):
        '''
        Reinicia el formato local (self) de la struct del memblock
        '''
        self.d_mbk = None
        self.r_mbk_largo = 0
        self.r_mbk_ntuple = None
        self.r_mbk_d_vars = None
        self.s_mbk_sformat = None
        self.s_mbk_largo = 0
        self.s_mbk_ntuple = None
        self.s_mbk_d_vars = None

    def process_mbk(self, force=False):
        '''
        Retorna el formato a usar para decodificar el bytearray recibido de acuerdo a la definicion de la estructura
        de datos que debe enviar el PLC.
        Tipos admitidos:
        https://docs.python.org/es/3/library/struct.html#module-struct
        V1.0: Solo admitimos
        CHAR: 1, SHORT (2), INT (4), FLOAT (4)

        El sformat es un string con el codigo (caracter) que define cada campo del memblock
        Se utiliza para decodificar el bytearray. 
        El largo es la cantidad de bytes que ocupa el memblock
        Names es una lista de nombres de las variables para que al decodificar el memblock de acuerdo al sformat
        se asignen los valores a los nombres indicados en la lista ( en el mismo orden )
        '''
        if self.r_mbk_sformat is not None and not force:
            return

        def process( l_mbk_def:list ):
            #
            sformat = '<'
            largo = 0
            names = ''
            for ( name, tipo, _) in l_mbk_def:
                names += f'{name} '
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
            return sformat, largo, names

        self.r_mbk_sformat, self.r_mbk_largo, names = process(self.d_mbk['RCVD_MBK_DEF'])
        log(module=__name__, function='process_mbk', level='ERROR', msg='r_mbk_sformat={0}, d_mbk={1}, names={2}'.format(self.r_mbk_sformat, self.d_mbk, names))

        self.r_mbk_ntuple = namedtuple('RCVD_VARS_NAMES', names)                # nombres de c/variable
        self.r_mbk_d_vars = { i:0 for i,*rest in self.d_mbk['RCVD_MBK_DEF']}    # Diccionario con las variables a usar

        self.s_mbk_sformat, self.s_mbk_largo, names = process(self.d_mbk['SEND_MBK_DEF'])
        log(module=__name__, function='process_mbk', level='ERROR', msg='s_mbk_sformat={0}, d_mbk={1}'.format(self.s_mbk_sformat, self.d_mbk))

        self.s_mbk_ntuple = namedtuple('SEND_VARS_NAMES', names)                # nombres de c/variable
        self.s_mbk_d_vars = { i:k for i,j,k,*rest in self.d_mbk['SEND_MBK_DEF']}    # Diccionario con las variables a usar

        return

    def payload_crc_valid(self, payload: bytearray):
        '''
        Calcula el CRC del payload y lo compara el que trae.
        El payload trae el CRC que envia el PLC en los 2 ultimos bytes
        El payload es un bytestring
        '''
        crc = int.from_bytes(payload[-2:],'big')
        calc_crc = computeCRC(payload[:-2])
        if crc == calc_crc:
            return True
        else:
            return False
        
    def convert_bytes2dict( self, payload: bytearray):
        '''
        Toma un payload; si el crc es correcto lo decodifica de acuerdo a la struct definida en el memblock
        y retorna un dict con las variables y sus valores.
        Utiliza el r_mbk ( RCVD)
        La defincion de la struct puede tener menos bytes que el bloque !!!. Para esto debo usar 'unpack_from'
        '''
        # El CRC debe ser correcto
        if not self.payload_crc_valid(payload):
            log(module=__name__, function='convert_bytes2dict', level='ERROR', msg='MBK_CRC_ERROR')
            return None

        # EL payload debe tener largo para ser decodificado de acuerdo al memblock def.
        if len(payload) < self.d_mbk['RCVD_MBK_LENGTH']:
            log(module=__name__, function='convert_bytes2dict', level='ERROR', msg='MBK_RCVD_LENGTH_ERROR')
            return None

        # Calculo los componentes del memblock
        self.process_mbk(force=True)
        # Desempaco el payload y relleno en c/variable
        try:
            mem_block = self.r_mbk_ntuple._make(unpack_from(self.r_mbk_sformat, payload))
        except:
            log(module=__name__, function='convert_bytes2dict', level='ERROR', msg='ERROR !!!: r_mbk_sformat={0}'.format(self.r_mbk_sformat))
            return None
        # La transformo en un diccionario.
        d_vars = mem_block._asdict()
        return d_vars

    def convert_dict2bytes( self, dlgid, d_data ):
        '''
        Recibo un diccionario con variables definidas en la estructura de un plc memblock.
        Utiliza el s_mbk ( SEND )
        El d_data puede tener mas variables que las que tiene el mbk.
        Serializo la estructura de acuerdo al mbk.
        Relleno con 0 hasta completar el largo del memblock
        Agrego el CRC del largo del memblock
        Retorno un bytearray.
        '''
        # Calculo los componentes del memblock
        self.process_mbk()
        # Apareo el d_data con el self.mbk_d_vars.
        # Si la variable NO está en d_data, pongo el valor por defecto leido del memblock.
        for key in self.s_mbk_d_vars:
            default_value = self.s_mbk_d_vars[key]
            self.s_mbk_d_vars[key] = d_data.get(key,default_value)
        # Convierto el diccionario a una namedtuple (template)
        ntuple = self.s_mbk_ntuple(**self.s_mbk_d_vars)
        # Convierto la ntuple a un bytearray serializado
        try:
            payload = pack( self.s_mbk_sformat, *ntuple)
        except:
            log(module=__name__,function='convert_dict2bytes', level='ERROR', dlgid=dlgid, msg='pack ERROR:')
            log(module=__name__,function='convert_dict2bytes', level='ERROR', dlgid=dlgid, msg=f's_mbk_sformat={self.s_mbk_sformat}')
            log(module=__name__,function='convert_dict2bytes', level='ERROR', dlgid=dlgid, msg=f's_mbk_d_vars={self.s_mbk_d_vars}')
            log(module=__name__,function='convert_dict2bytes', level='ERROR', dlgid=dlgid, msg=f'ntuple={ntuple}')
            return None


        # Controlo errores: el payload no puede ser mas largo que el tamaño del bloque (frame)
        if len(payload) > self.d_mbk['SEND_MBK_LENGTH']:
            return None

        # Relleno con 0 el bloque
        largo_relleno = self.d_mbk['SEND_MBK_LENGTH'] - len(payload)
        relleno = bytes(largo_relleno*[0])
        payload += relleno

        # Calculo el CRC y lo agrego al final. Lo debo convertir a bytes antes.
        crc = computeCRC(payload)
        payload += crc.to_bytes(2,'big')

        return self.s_mbk_d_vars, payload

    def process( self, l_mbk_def:list ):
        '''
        Testing
        '''
        #
        field_fmt = ''
        field_length = 0
        sformat = '<'
        largo_sformat = 0
        names = ''
        pos_in_payload = 0
        for ( name, tipo, _) in l_mbk_def:
            if tipo == 'char':
                field_fmt = 'c'
                field_length = 1
            elif tipo == 'schar':
                field_fmt = 'b'
                field_length = 1
            elif tipo == 'uchar':
                field_fmt = 'B'
                field_length = 1
            elif tipo == 'short':
                field_fmt = 'h'
                field_length = 2
            elif tipo == 'int':
                field_fmt = 'i'
                field_length = 4
            elif tipo == 'float':
                field_fmt = 'f'
                field_length = 4
            elif tipo == 'unsigned':
                field_fmt = 'H'
                field_length = 2
            else:
                field_fmt = '?'
                field_length = 0
            
            print(f'{name}, {pos_in_payload}, {field_length}, {field_fmt}')
            names += f'{name} '
            sformat += field_fmt
            largo_sformat += field_length
            pos_in_payload += field_length
            
        return sformat, largo_sformat, names
