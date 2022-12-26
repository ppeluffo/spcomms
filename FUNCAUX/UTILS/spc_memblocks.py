#!/opt/anaconda3/envs/mlearn/bin/python3
'''
Implementa la clase con los metodos necesarios para convertir un bytestream en un diccionario de acuerdo
a las definiciones de variables dentro del bloque de memoria.
Se usa para que los PLC manden bloques de memoria serializados y no tengamos que usar un protocolo que agregue
al payload lo que es cada variable.
La definicion de estas debe estar en el servidor.
'''

from collections import namedtuple
from struct import *

#PLC_MEMBLOCK_DEFINITION_V1_0 = [('count','int',0),('endian','float',1),('version','int',2),('var1','float',3),('var2','float',4)]
PLC_MEMBLOCK_DEFINITION_V1_0 = [('var1','float',0),('var2','float',0)]

class MEMBLOCK:
    # b=pack('ififf',14,123.4,100,3.14,2.7)
    # b'\x0e\x00\x00\x00\xcd\xcc\xf6Bd\x00\x00\x00\xc3\xf5H@\xcd\xcc,@'

    def __init__(self):
        self.memblock_definition = PLC_MEMBLOCK_DEFINITION_V1_0
        self.sformat = None
        self.ntuple_template = None

    def set_memblock_definition(self, memblock_definition):
        self.memblock_definition = memblock_definition

    def get_memblock_definition(self):
        return self.memblock_definition

    def get_sformat(self):
        return self.sformat

    def reset_sformat(self):
        self.sformat = None

    def calc_sformat(self, force=False):
        '''
        Retorna el formato a usar para decodificar el bytearray recibido de acuerdo a la definicion de la estructura
        de datos que debe enviar el PLC.
        Tipos admitidos:
        https://docs.python.org/es/3/library/struct.html#module-struct
        V1.0: Solo admitimos
        CHAR: 1, SHORT (2), INT (4), FLOAT (4)
        '''
        if force == False and self.sformat is not None:
            return
        #
        sformat = ''
        for (name, tipo, pos) in self.memblock_definition:
            if tipo == 'char':
                sformat += 'c'
            elif tipo == 'short':
                sformat += 'h'
            elif tipo == 'int':
                sformat += 'i'
            elif tipo == 'float':
                sformat += 'f'
            else:
                sformat = '?'
        self.sformat = sformat

    def get_ntuple_template(self):
        return self.ntuple_template

    def reset_ntuple_template(self):
        self.ntuple_template = None

    def calc_ntuple_template(self, force=False):
        '''
        Retorna un template de namedtuple para que luego al unpack los datos, estos queden asignados a las variables correctas
        '''
        if force == False and self.ntuple_template is not None:
            return
        #
        names = ''
        for name, tipo, var in self.memblock_definition:
            names += '{0} '.format(name)
        self.ntuple_template = namedtuple('STATUS', names)

    def convert_bytes2dict( self, payload):
        # Genero el formato para desempacar
        self.calc_sformat()
        # Genero la estructura con los nombres de c/variable
        self.calc_ntuple_template()
        # Desempaco el payload y relleno en c/variable
        mem_block = self.ntuple_template._make(unpack(self.sformat, payload))
        # La transformo en un diccionario.
        dict_of_vars = mem_block._asdict()
        return dict_of_vars

    def convert_dict2bytes( self, d_payload):
        '''
        Recibo un diccionario con una estructura definida en plc_memblock_template y lo serializo
        para enviar.
        '''
        # Convierto el diccionario a una namedtuple (template)
        self.calc_ntuple_template()
        ntuple = self.ntuple_template(**d_payload)
        # Convierto la ntuple a un bytearray serializado
        self.calc_sformat()
        payload = pack( self.sformat, *ntuple)
        return payload
