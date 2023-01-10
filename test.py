#!/usr/bin/python3 -u

import os
from pymodbus.utilities import computeCRC
import pickle

l1=[('UPA1_ORDER_1', 'short', 1),
 ('UPA1_ORDER_2', 'short', 1),
 ('UPA1_ORDER_3', 'short', 1),
 ('UPA1_ORDER_4', 'short', 1),
 ('UPA1_ORDER_5', 'short', 1),
 ('UPA1_CONSIGNA_6', 'short', 1),
 ('UPA1_CONSIGNA_7', 'short', 1),
 ('UPA2_ORDER_1', 'short', 1),
 ('UPA2_ORDER_2', 'short', 1),
 ('UPA2_ORDER_3', 'short', 1),
 ('UPA2_ORDER_4', 'short', 1),
 ('UPA2_ORDER_5', 'short', 1),
 ('UPA2_CONSIGNA_6', 'short', 1),
 ('UPA2_CONSIGNA_7', 'short', 1),
 ('UPA3_ORDER_1', 'short', 1),
 ('UPA3_ORDER_2', 'short', 1),
 ('UPA3_ORDER_3', 'short', 1),
 ('UPA3_ORDER_4', 'short', 1),
 ('UPA3_ORDER_5', 'short', 1),
 ('UPA3_CONSIGNA_6', 'short', 1),
 ('UPA3_CONSIGNA_7', 'short', 1),
 ('ESP_ORDER_1', 'short', 1),
 ('ESP_ORDER_2', 'short', 1),
 ('ESP_ORDER_3', 'short', 1),
 ('ESP_ORDER_4', 'short', 1),
 ('ESP_ORDER_5', 'short', 1),
 ('ESP_ORDER_6', 'short', 1),
 ('ESP_ORDER_7', 'short', 1),
 ('ESP_ORDER_8', 'short', 1),
 ('NIVEL_TQ_KIYU', 'float', 1)]

def setATV(orderVal,consignaVal):
    d={}
    for (i,j,k) in l1:
        if 'ORDER' in i:
            d[i]=orderVal
        if 'CONSIGNA' in i:
            d[i]=consignaVal
    pkline=pickle.dumps(d)
    rh.hset('PLCTEST','ATVISE',pkline)
    return d

def setHTQ(h):
    d={'HTQ1':h}
    pkline=pickle.dumps(d)
    rh.hset('KYTQ003','PKLINE',pkline)    


def crc16(self, data : bytearray, offset , length):
        '''
        Calcula el CRC16 de un bytearray.
        '''
        if data is None or offset < 0 or offset > len(data)- 1 and offset+length > len(data):
            return 0
        crc = 0xFFFF
        for i in range(0, length):
            crc ^= data[offset + i] << 8
            for j in range(0,8):
                if (crc & 0x8000) > 0:
                    crc =(crc << 1) ^ 0x1021
                else:
                    crc = crc << 1
        return crc & 0xFFFF


if __name__ == '__main__':
    data = '0A 0B 66 E6 F6 42 62 04 57 02 EC 71 E4 43 3A 16 00 00 00 00 00'
    numeric_data = [ int(i,16) for i in data.split(' ')]
    bytes_data = bytes(numeric_data)
    crc=computeCRC(bytes_data)
    print('CRC={0}'.format(hex(crc)))




