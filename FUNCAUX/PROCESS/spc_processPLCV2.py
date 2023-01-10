#!/opt/anaconda3/envs/mlearn/bin/python3
#!/usr/bin/python3 -u

import os
import time
import random
import pickle

from FUNCAUX.UTILS.spc_log import log
from FUNCAUX.BD.spc_bd_redis import BD_REDIS
from FUNCAUX.BD.spc_bd_atvise import BD_ATVISE
from FUNCAUX.UTILS import spc_stats as stats

DATABOUNDLESIZE = 50

class ProcessPLCV2:

    def __init__(self, queue_name, tipo=None ):
        self.pid = os.getpid()
        self.tipo = tipo
        self.queue_name = queue_name
        self.start_time = time.perf_counter()
        self.elapsed_time = 0
        self.bdatvise = BD_ATVISE()
        self.rh = BD_REDIS()
        log(module=__name__, function='ProcessPLCV2', level='INFO', msg='Start Child: pid={0}, type={1}, queue={2}'.format(self.pid, self.tipo, self.queue_name))

    def process_queue(self):
        '''
        Lee un bundle de lineas de redis.
        Si esta vacia, espera
        '''
        while True:
            boundle = self.rh.lpop_lqueue(self.queue_name, DATABOUNDLESIZE)
            if boundle is not None:
                qsize = len(boundle)
                if qsize > 0:
                    # Hay datos para procesar: arranco un worker.
                    start = time.perf_counter()
                    stats.init()
                    log(module=__name__, function='process_queue', level='INFO', msg='{0}: ({1}) process_queue: RQsize={2}'.format(self.tipo, self.pid, qsize))
                    data = []
                    for pkline in boundle:
                        d = pickle.loads(pkline)
                        # Debo eliminar los TAGS: QUERY_STRING, CLASS, DATE, TIME
                        for key in [ 'QUERY_STRING', 'CLASS', 'DATE', 'TIME','VER']:
                            d.pop(key,None)
                        #
                        dlgid = d.get('ID', 'SPY000')
                        #log(module=__name__, function='process_queue', level='INFO', msg='pid={0},PKLINE={1}'.format(self.pid, pkline))
                        data.append({'dlgid': dlgid, 'data': d})
                    # Inserto
                    self.bdatvise.insert_plcR2_data(data)

                    # Fin del procesamiento del bundle
                    elapsed = time.perf_counter() - start
                    log(module=__name__, function='process_queue', level='INFO', msg='{0}: ({1}) process_queue: round elapsed={2}'.format(self.tipo, self.pid, elapsed))
                    stats.end()
            #
            else:
                # log(module=__name__, function='process_test', level='INFO', msg='No hay datos en qRedis. Espero 5s....')
                time.sleep(5)
