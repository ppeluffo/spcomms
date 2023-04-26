#!/home/pablo/Spymovil/www/cgi-bin/spcommsV3/venv/bin/python3
'''
Funciones donde implemento los objetos Responses.
'''

class Response:
    ''' Defino los objetos de respuestas que se manejan en todo el sistema '''
    def __init__(self):
        self.__status_code__ = 500
        self.__reason__ = ''
        self.__json__ = {}
        self.__dlgid__ = '00000'
        
    def set_status_code(self, status_code=0):
        self.__status_code__ = status_code

    def status_code(self):
        return self.__status_code__
    
    def set_json(self, p_dict={}):
        self.__json__ = p_dict

    def json(self):
        return self.__json__

    def set_reason(self, reason=''):
        self.__reason__ = reason
        
    def reason(self):
        return self.__reason__
    
    def set_dlgid(self, dlgid='00000'):
        self.__dlgid__ = dlgid
        
    def dlgid(self):
        return self.__dlgid__

    