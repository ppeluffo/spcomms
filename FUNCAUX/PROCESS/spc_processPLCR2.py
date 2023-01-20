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

class ProcessPLCR2:

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
        No tengo el dlgid !!!.
        Las lineas solo tiene las key 'ID' y los nombres de las variables(muchas)
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
                    for pkline in boundle:
                        d = pickle.loads(pkline)
                        if not isinstance(d, dict):
                            continue
                        #
                        #log(module=__name__, function='process_queue', level='INFO', msg='pid={0},PKLINE={1}'.format(self.pid, pkline))
                        # Inserto
                        #log(module=__name__, function='process_queue', level='ERROR', msg='D={0}'.format(d))
                        self.bdatvise.insert_plcR2_data(d)

                    # Fin del procesamiento del bundle
                    elapsed = time.perf_counter() - start
                    log(module=__name__, function='process_queue', level='INFO', msg='{0}: ({1}) process_queue: round elapsed={2}'.format(self.tipo, self.pid, elapsed))
                    stats.end()
            #
            else:
                # log(module=__name__, function='process_test', level='INFO', msg='No hay datos en qRedis. Espero 5s....')
                time.sleep(5)
