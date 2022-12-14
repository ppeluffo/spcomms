#!/opt/anaconda3/envs/mlearn/bin/python3
#!/usr/bin/python3 -u

import time
import pickle

from FUNCAUX.UTILS import spc_stats as stats
from FUNCAUX.UTILS.spc_log import log
from FUNCAUX.PROCESS.spc_processBase import ProcessBASE
from FUNCAUX.UTILS.spc_utils import mbusWrite

DATABOUNDLESIZE = 50


class ProcessPLC(ProcessBASE):

    def procesar_reenvios(self, d_params):
        '''
        Lee si el dlgid tiene asociados otros para reenviarle los datos medidos.
        d_params={'GDA':gda,'REDIS':rh,'DLGID':dlgid,'DATOS':d }
        '''
        # Intento leer el diccionario con los datos de los reenvios de redis primero
        # log(module=__name__, function='procesar_reenvios', level='INFO', msg='DEBUG: D_PARAMS={0}'.format(d_params))
        dlgid = d_params['DLGID']
        d_data = d_params['DATOS']
        # log(module=__name__, function='procesar_reenvios', level='INFO', msg='DEBUG: D_DATA={0}'.format(d_data))

        # Leo la informacion de los reenvios.
        d_reenvios = self.rh.get_d_reenvios(dlgid)
        if d_reenvios is None:
            # No esta en Redis: intento leerlo de GDA y actualizar REDIS
            d_reenvios_gda = self.gda.get_d_reenvios(dlgid)
            if d_reenvios_gda is None:
                # No hay reenvios: salgo.
                return

            # Hay datos de reenvios en GDA: actualizo redis para la proxima vez
            self.rh.set_d_reenvios(dlgid, d_reenvios_gda)
            d_reenvios = d_reenvios_gda.copy()

        # log(module=__name__, function='procesar_reenvios', level='SELECT', dlgid=dlgid, msg='{0}: D_REENVIOS={1}'.format(self.tipo, d_reenvios))
        # log(module=__name__, function='procesar_reenvios', level='SELECT', dlgid=dlgid, msg='{0}: D_DATA={1}'.format(self.tipo, d_data))

        l_cmds_modbus = []
        for k_dlgid in d_reenvios:
            for magname in d_reenvios[k_dlgid]:
                if magname in d_data:
                    magval = float(d_data[magname])
                    regaddr = d_reenvios[k_dlgid][magname]['MBUS_REGADDR']
                    tipo = d_reenvios[k_dlgid][magname]['TIPO']
                    # Correcciones
                    tipo = ('float' if tipo == 'FLOAT' else 'integer')
                    if tipo == 'integer':
                        magval = int(magval)
                    # d_reenvios[dlgid][magname]['VALOR'] = magval
                    l_cmds_modbus.append((k_dlgid, regaddr, tipo, magval), )

        # l_cmds_modbus = [('PABLO1', '1965', 'float', 8.718), ('PABLO2', '1966', 'integer', 17)]
        #log(module=__name__, function='procesar_reenvios', level='INFO', dlgid=dlgid, msg='L_CMDS_MODBUS({0})={1}'.format(dlgid, l_cmds_modbus))

        ''''
        l_cmds_modbus = [('YCHTEST1', '1962', 'float', 1.68),
         ('YCHTEST1', '2032', 'float', 1.7),
         ('SJPERF021', '1962', 'float', 1.68),
         ('SJPERF021', '2032', 'float', 1.7),
         ('SJPERF001', '1962', 'float', 1.68),
         ('SJPERF001', '2032', 'float', 1.7),
         ('SJPERF002', '1962', 'float', 1.68),
         ('SJPERF002', '2032', 'float', 1.7) ]
        '''
        log(module=__name__, function='procesar_reenvios', level='INFO', dlgid=dlgid, msg='DEBUG: L_CMDS_MODBUS({0})={1}'.format(dlgid, l_cmds_modbus))
        # Recorro la lista e inserto en la redis de c/dlg la linea correspondiente.
        for t in l_cmds_modbus:
            (dlgid, register, dataType, value) = t
            # llamo a la funcion para escribir el key MODBUS de redis en cada ejecucion del for
            mbusWrite(dlgid, register, dataType, value)
            log(module=__name__, function='procesar_reenvios', level='INFO', dlgid=dlgid, msg='DEBUG: T={0}'.format(t))
        return

    def process_queue(self):
        '''
        Overload del metodo porque en los PLC debo procesar reenvios
        '''
        while True:
            boundle = self.rh.lpop_lqueue('LQ_PLCDATA', DATABOUNDLESIZE)
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
                        # log(module=__name__, function='process_queue', level='INFO', msg='DEBUG: D={0},PKLINE={1}, DLGID={2}'.format(d, pkline, dlgid))

                        data.append({'dlgid': dlgid, 'data': d})
                        # Automatismos
                        # Broadcasting / Reenvio de datos
                        d_params = {'GDA': self.gda, 'REDIS': self.rh, 'DLGID': dlgid, 'DATOS': d}
                        # log(module=__name__, function='process_queue', level='INFO', msg='DEBUG: D_PARAMS={0}'.format(d_params))
                        self.procesar_reenvios(d_params)

                    # Inserto en todas las tablas
                    self.insert_in_bd(data)

                    # Fin del procesamiento del bundle
                    elapsed = time.perf_counter() - start
                    log(module=__name__, function='process_queue', level='INFO', msg='{0}: ({1}) process_queue: round elapsed={2}'.format(self.tipo, self.pid, elapsed))
                    stats.end()
            #
            else:
                # log(module=__name__, function='process_test', level='INFO', msg='{0}: No hay datos en qRedis. Espero 5s....'.format(self.tipo))
                time.sleep(5)
