#!/opt/anaconda3/envs/mlearn/bin/python3
'''
Script que manda al servidor apache caracteres tipo byte
Lo usamos para probar como hacer con la estacion OCEANUS
'''
import urllib.request

if __name__ == '__main__':

    url = 'http://127.0.0.1:80/cgi-bin/COMMS/spcomms.py?ID:TEST;TYPE:OCEANUS;VER:1.0.1;'

    payload = 'PRUEBA'  # es tipo str
    payload = b'PRUEBA' # es tipo bytes.

    url += payload
    #
    print('SENT: {0}'.format(url))

    try:
        req = urllib.request.Request(url)
    except:
        print('\nERROR al enviar frame {0}'.format(url))

    print('.', end='', flush=True)

    with urllib.request.urlopen(req) as response:
        response = response.read()

    print('RESP: {0}'.format(response))
