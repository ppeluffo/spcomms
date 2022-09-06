#!/opt/anaconda3/envs/mlearn/bin/python3

import  requests


if __name__ == '__main__':

    s1 = 'A5 A5 A5 A5 A5 A5 A5 A5 01 00 00 80 00 00 00 00 00 00 FE 02 35 00 A3 06 02 00 00 00 01 2C 00 00 00 00 0C 00 40 00 0C 00 01 0A 01 02 00 00 00 00 01 01 01 00 17 02 11 00 00 00 5C 46 4F 30 30 30 54 45 4D 50 45 52 3A 32 31 A1 E6 82 DA 5A A5 A5 A5 A5 A5 A5 A5 A5 01 00 00 80 00 00 00 00 00 00 FE 02 33 00 A3 06 02 00 00 00 01 2A 00 00 00 00 18 00 40 00 0C 00 01 0A 01 02 00 00 00 00 01 01 01 00 17 02 0F 00 00 00 5C 46 4F 30 30 30 48 55 4D 3A 36 36 25 52 48 36 7A 5A A5 A5 A5 A5 A5 A5 A5 A5 01 00 00 80 00 00 00 00 00 00 FE 02 2C 00 A3 06 02 00 00 00 01 23 00 00 00 00 24 00 40 00 0C 00 01 0A 01 02 00 00 00 00 01 01 01 00 17 02 08 00 00 00 5C 46 4F 30 30 30 20 20 DA F6 5A'
    s2 = [ int('0x'+i, 16) for i in s1.split(' ')]
    s3 = bytes(s2)

    s4 = s3.hex()
    # s4=str::'a5a5a5a5a5a5a5a501000080000000000000fe023500a30602000000012c000000000c'

    print('Largo de payload={0}'.format(len(s3)))
    rsp = requests.post('http://127.0.0.1:80/cgi-bin/COMMS/spcomms.py?ID:Prueba;TYPE:OCEANUS;VER:1.0.1;', s3)

    # EL dato recibido por el server
    # Esta codificado UNICODE
    p1 = '\udca5\udca5\udca5\udca5\udca5\udca5\udca5\udca5\x01\x00\x00\udc80\x00\x00\x00\x00\x00\x00\udcfe\x025\x00\udca3\x06\x02\x00\x00\x00\x01,\x00\x00\x00\x00\x0c'
    p=p1.encode('ascii', 'surrogateescape')
    # p = b'\xa5\xa5\xa5\xa5\xa5\xa5\xa5\xa5\x01\x00\x00\x80\x00\x00\x00\x00\x00\x00\xfe\x025\x00\xa3\x06\x02\x00\x00\x00\x01,\x00\x00\x00\x00\x0c'

