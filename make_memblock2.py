#!./venv/bin/python3

'''
    MBK_KIYU2 = {'REMVARS': {'KYTQ003': [['HTQ1', 'NIVEL_TQ_KIYU']]},
    'MEMBLOCK': 
    {'RCVD_MBK_DEF': [['UPA1_CAUDALIMETRO', 'float', 0],['UPA1_DUMMY1', 'uchar', 1],['UPA1_DUMMY2', 'uchar', 1],['UPA1_STATE1', 'uchar', 1]],
    'SEND_MBK_DEF': [['UPA1_ORDER_1', 'short', 1],['UPA1_ORDER_2', 'short', 200],['UPA1_ORDER_3', 'short', 200],['ORDER_B19', 'uchar',	100]],
    'RCVD_MBK_LENGTH': 165,
    'SEND_MBK_LENGTH': 165}
    }
'''

import json
import argparse
import pandas as pd

class MAKEMBK:

    def __init__(self):
        self.filename = None
        self.length_rcvdmbk = 0
        self.length_sendmbk = 0
        self.l_items = []
        self.d_mbk = {}
        self.d_remvars = {}
        self.json = None
        self.json_idents = 0

    def set_json_idents(self, idents):
        self.json_idents = int(idents)

    def convert_d_to_json(self):
        d={}
        d['MEMBLOCK'] = self.d_mbk
        d['REMVARS'] = self.d_remvars
        self.json = json.dumps(d)

    def get_json(self):
        return self.json

    def get_d_mbk(self):
        return self.d_mbk

    def set_filename(self, filename):
        self.filename = filename

    def get_filename(self):
        return self.get_filename

    def process_tx_memblock(self):
        '''
        El tx_memblock es el que envia el PLC
        La hoja en que esta la definicion debe llamarse 'TXMBK'
        '''
        df = pd.read_excel(self.filename, engine='openpyxl', sheet_name='TXMBK')
        df.dropna(how='all', inplace=True)
        df['default'] = 1
        #
        payload_length = df.iloc[-1,1]
        self.d_mbk['RCVD_MBK_LENGTH'] = payload_length
        #
        l=list(df.itertuples(index=False, name=None))[:-1]
        self.d_mbk['RCVD_MBK_DEF'] = l

    def process_rx_memblock(self):
        '''
        El rx_memblock es el que recibe el PLC
        La hoja en que esta la definicion debe llamarse 'RXMBK'
        '''
        df = pd.read_excel(self.filename, engine='openpyxl', sheet_name='RXMBK')
        df.dropna(how='all', inplace=True)
        #
        payload_length = df.iloc[-1,1]
        self.d_mbk['SEND_MBK_LENGTH'] = payload_length
        #
        l=list(df.itertuples(index=False, name=None))[:-1]
        l1=[]
        for (a,b,c) in l:
            if b != 'FLOAT':
                c=int(c)
            l1.append([a,b,c])
        #
        self.d_mbk['SEND_MBK_DEF'] = l1

    def process_remvars(self):
        '''
        Las remvars son las variables remotas que deben leerse de otros equipos
        La hoja en que esta la definicion debe llamarse 'REMVARS'
        '''
        df = pd.read_excel(self.filename, engine='openpyxl', sheet_name='REMVARS')
        df.dropna(how='all', inplace=True)
        l=list(df.itertuples(index=False, name=None))
        self.d_remvars={}
        for (id,lv,rv) in l:
            if id in self.d_remvars:
                self.d_remvars[id].append([lv,rv])
            else:
                self.d_remvars[id]=[[lv,rv]]

    def make_mbk(self):
        self.process_tx_memblock()
        self.process_rx_memblock()
        self.process_remvars()
        self.convert_d_to_json()


def process_arguments():
    '''
    Proceso la linea de comandos.
    d_vp es un diccionario donde guardamos todas las variables del programa
    Corrijo los parametros que sean necesarios
    '''
    parser = argparse.ArgumentParser(description='Script de creacion de formato de configuracion de PLCR2')
    parser.add_argument('-f', '--filename', dest='filename', action='store', default='MEMBLOCK_KIYU.xlsx',
                        help='Nombre de archvo xlsx con definciones')
    parser.add_argument('-i', '--idents', dest='json_idents', action='store', default=None,
                        help='Idents del json')

    args = parser.parse_args()
    d_o_args = vars(args)
    return d_o_args


if __name__ == "__main__":
    d_args = process_arguments()
    print('Configuracion:')
    print(f"\tArchivo xlsx: {d_args['filename']}")
    print(f"\tJson Idents: {d_args['json_idents']}")

    #
    mmbk=MAKEMBK()
    mmbk.set_filename(d_args['filename'])
    #mmbk.set_json_idents(d_args['json_idents'])
    mmbk.make_mbk()
    #print(mmbk.get_d_mbk())
    print(mmbk.get_json())

