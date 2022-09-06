#!/opt/anaconda3/envs/mlearn/bin/python3
#!/usr/bin/python3 -u

import os
import time
import random
import pickle

from FUNCAUX.UTILS.spc_log import log
from FUNCAUX.BD.spc_bd_redis import BD_REDIS
from FUNCAUX.BD.spc_bd_gda import BD_GDA
from FUNCAUX.UTILS import spc_stats as stats

DATABOUNDLESIZE = 50


class ProcessBASE:

    def __init__(self, queue_name, tipo=None ):
        self.pid = os.getpid()
        self.tipo = tipo
        self.queue_name = queue_name
        self.start_time = time.perf_counter()
        self.elapsed_time = 0
        self.gda = BD_GDA()
        self.rh = BD_REDIS()
        log(module=__name__, function='ProcessBASE', level='INFO', msg='Start Child: pid={0}, type={1}, queue={2}'.format(self.pid, self.tipo, self.queue_name))

    def test(self):
        # logger.info('process_child_plc START. pid={0}'.format(pid))
        log(module=__name__, function='test', level='INFO', msg='test. pid={0}'.format(self.pid))
        time.sleep(random.randint(1, 10))
        self.elapsed_time = time.perf_counter() - self.start_time
        # logger.info('process_child_plc END. pid={0}, elapsed={1}'.format(pid,elapsed))
        log(module=__name__, function='test', level='INFO', msg='test END. pid={0}, elapsed={1}'.format(self.pid, self.elapsed_time))
        exit(0)

    def insert_in_bd(self, data):
        # Inserto en todas las tablas
        # Si alguna da error salgo. No intento mas
        if not self.gda.insert_dlg_raw(data):
            log(module=__name__, function='insert_in_bd', level='ERROR', msg='{0}: ERROR: gda.insert_dlg_raw'.format(self.tipo))
            return

        if not self.gda.insert_dlg_data(data):
            log(module=__name__, function='insert_in_bd', level='ERROR', msg='{0}: ERROR: gda.insert_dlg_data'.format(self.tipo))
            return
        #
        # Inserto todo en una sola consulta
        all_configs = self.gda.read_dlg_insert_data(data)
        if all_configs is None:
            log(module=__name__, function='insert_in_bd', level='ERROR', msg='{0}: ERROR: all_configs is None.'.format(self.tipo))
            return

        if not self.gda.insert_spx_datos(data, all_configs):
            log(module=__name__, function='insert_in_bd', level='ERROR', msg='{0}: ERROR: gda.insert_spx_datos'.format(self.tipo))
            return

        if not self.gda.insert_spx_datos_online(data, all_configs):
            log(module=__name__, function='insert_in_bd', level='ERROR', msg='{0}: ERROR: gda.insert_spx_datos_online'.format(self.tipo))
            return

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
                        dlgid = d.get('ID', 'SPY000')
                        #log(module=__name__, function='process_queue', level='INFO', msg='pid={0},PKLINE={1}'.format(self.pid, pkline))
                        data.append({'dlgid': dlgid, 'data': d})
                    # Inserto en todas las tablas
                    self.insert_in_bd(data)

                    # Fin del procesamiento del bundle
                    elapsed = time.perf_counter() - start
                    log(module=__name__, function='process_queue', level='INFO', msg='{0}: ({1}) process_queue: round elapsed={2}'.format(self.tipo, self.pid, elapsed))
                    stats.end()
            #
            else:
                # log(module=__name__, function='process_test', level='INFO', msg='No hay datos en qRedis. Espero 5s....')
                time.sleep(5)
