#!/opt/anaconda3/envs/mlearn/bin/python3
#!/usr/bin/python3 -u

'''
<<<<<<< HEAD
----------------------------------------------------------------------------------------------------
Version 2.1 @ 2022-10-26:
Problema: Cuando tenemos una red de automatismos, cuando el TANQUE envia los datos, no se hace el
          broadcast de las variables a la REDIS:MODBUS:DLG_REMOTOS. Por lo tanto los remotos no tienen datos
          para leer en sus frames de respuestas.
          El programa SI considera la REDIS:MODBUS al armar el frame ya que en los casos que tenemos
          un mix de tanques (spy.py) con perforaciones (spcomms.py) funciona. ( rh.get_modbusline() )
          El tema es en la RECEPCION y PROCESS del frame de spcomms.py que no hacemos el broadcast_local_vars().
Solucion: Copiamos el broadcast_local_vars() de spy.py  y lo adaptamos.
          Esto lo hacemos en el PROCESS de los PLC
----------------------------------------------------------------------------------------------------
=======
Version 2.1 @ 2022-10-05:
Corrijo el bug que no estaba definida la cola plist_oceanus por lo que no procesaba datos de OCEANUS

>>>>>>> e7d4e0be21f090dd19623ec972b93745c307553a
Version 2.0 @ 2022-08-02:
Ulises modifica para que se haga una insercion sola con todos los datos.
----------------------------------------------------------------------------------------------------
Version 1.0:
Servidor de procesamiento de frames recibidos de los PLC que están en la REDIS.
El __main__ invoca a un proceso master.
Este master crea un pool de processo child que quedan leyendo la REDIS.
Si hay datos la sacan y procesan.
----------------------------------------------------------------------------------------------------
'''
from multiprocessing import Process
import time
import signal
from FUNCAUX.UTILS.spc_log import config_logger, log
from FUNCAUX.UTILS.spc_config import Config
from FUNCAUX.PROCESS.spc_processPLC import ProcessPLC
from FUNCAUX.PROCESS.spc_processPLCPAY import ProcessPLCPAY
from FUNCAUX.PROCESS.spc_processSPX import ProcessSPX
from FUNCAUX.PROCESS.spc_processSP5K import ProcessSP5K
from FUNCAUX.PROCESS.spc_processOCEANUS import ProcessOCEANUS
from FUNCAUX.PROCESS.spc_processPLCV2 import ProcessPLCV2

MAXPOOLSIZE_SPX=2
MAXPOOLSIZE_SP5K=2
MAXPOOLSIZE_PLC=2
MAXPOOLSIZE_PLCPAY=2
MAXPOOLSIZE_OCEANUS=2
MAXPOOLSIZE_PLCV2=2

DATABOUNDLESIZE=50

# ------------------------------------------------------------------------------------

def process_child(child_type='PLC'):

    if child_type == 'PLC':
        p = ProcessPLC( 'LQ_PLCDATA', 'PLC')
    elif child_type == 'PLCPAY':
        p = ProcessPLCPAY('LQ_PLCPAYDATA', 'PLCPAY')
    elif child_type == 'SPX':
        p = ProcessSPX('LQ_SPXDATA', 'SPX')
    elif child_type == 'SP5K':
        p = ProcessSP5K('LQ_SP5KDATA', 'SP5K')
    elif child_type == 'OCEANUS':
        p = ProcessOCEANUS('LQ_OCEANUSDATA', 'OCEANUS')
    elif child_type == 'PLCV2':
        p = ProcessOCEANUS('LQ_PLCV2DATA', 'PLCV2')
    else:
        log(module=__name__, function='process_child', level='ERROR', msg='ERROR: child_type = {0}'.format(child_type))
        return
    #
    p.process_queue()
    return


def process_master_start_child(plist, child_type=None, poolsize=2):
    if child_type is None:
        log(module=__name__, function='process_master_start_child', level='ERROR', msg='ERROR: child_type = {0}'.format(child_type))
        return

    while len(plist) < poolsize:
        p = Process(target=process_child, args=(child_type,))
        p.start()
        plist.append(p)
        log(module=__name__, function='process_master_start_child', level='INFO', msg='Agrego nuevo proceso child para {0}'.format(child_type))


def process_master():
    '''
    https://stackoverflow.com/questions/25557686/python-sharing-a-lock-between-processes
    https://bentyeh.github.io/blog/20190722_Python-multiprocessing-progress.html
    Tengo childs para cada tipo de procesamiento (spx,sp5k,plc,plcpay)
    '''
    log(module=__name__, function='process_master', level='INFO', msg='process_master START')
    plist_spx = []
    plist_sp5k = []
    plist_plc = []
    plist_plcpay = []
    plist_oceanus = []
    plist_plcV2 = []
    process_list = [plist_spx, plist_sp5k, plist_plc, plist_plcpay, plist_oceanus, plist_plcV2 ]
    child_types = ['SPX','SP5K', 'PLC','PLCPAY','OCEANUS', 'PLCV2']
    process_poolsizes = [ MAXPOOLSIZE_SPX, MAXPOOLSIZE_SP5K, MAXPOOLSIZE_PLC, MAXPOOLSIZE_PLCPAY, MAXPOOLSIZE_OCEANUS, MAXPOOLSIZE_PLCV2 ]
    #logger.info(plist)
    # Creo todos los procesos child.
    for plist, child_type, poolsize in zip( process_list, child_types, process_poolsizes ):
        process_master_start_child(plist, child_type, poolsize, )

    '''
    Quedo monitoreando los procesos: si alguno termina ( por errores cae ), levanto uno que lo reemplaza
    '''
    while True:
        # Primero reviso si algun proceso de alguna lista termino: lo saco asi queda espacio para uno nuevo
        for plist,child_type in zip( process_list, child_types):
            # Saco de la plist todos los procesos child que no este vivos
            for i, p in enumerate(plist):
                if not p.is_alive():
                    plist.pop(i)
                    log(module=__name__, function='process_master', level='INFO', msg='process_master: {0} Proceso {1} not alive;removed. L={1}'.format(child_type, i,len(plist)))

        # Vuelvo a rellenar la lista de modo de tener el maximo de procesos corriendo siempre
        for plist, child_type, poolsize in zip(process_list, child_types, process_poolsizes):
            process_master_start_child(plist, child_type, poolsize, )

        log(module=__name__, function='process_master_', level='INFO', msg='process_master: Sleep 5s ...')
        time.sleep(5)


def clt_C_handler(signum, frame):
    exit(0)


if __name__ == '__main__':

    signal.signal(signal.SIGINT, clt_C_handler)
    #logger = log_to_stderr(logging.DEBUG)

    # Arranco el proceso que maneja los inits
    config_logger('XPROCESS')
    log(module=__name__, function='__init__', level='ALERT', msg='XPROCESS START')
    log(module=__name__, function='__init__', level='ALERT',
        msg='XPROCESS: Redis server={0}'.format(Config['REDIS']['host']))
    log(module=__name__, function='__init__', level='ALERT',
        msg='XPROCESS: GDA url={0}'.format(Config['BDATOS']['url_gda_spymovil']))
    p1 = Process(target=process_master)
    p1.start()

    # Espero para siempre
    while True:
        time.sleep(60)
