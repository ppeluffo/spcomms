#!/home/pablo/Spymovil/www/cgi-bin/spcommsV3/venv/bin/python3
'''
https://stackoverflow.com/questions/60100749/how-to-share-an-instance-of-a-class-across-an-entire-project
https://refactoring.guru/es/design-patterns/singleton/python/example#example-1
https://stackoverflow.com/questions/6760685/creating-a-singleton-in-python
https://www.geeksforgeeks.org/singleton-pattern-in-python-a-complete-guide/

La idea es usar variables globales entre los modulos.
https://stackoverflow.com/questions/13034496/using-global-variables-between-files

Voy a implementarlo como un modulo singleton.

Las estadisticas que nos interesan son:
- Contador de cada tipo de frame procesado
- Contador de c/acceso a c/BD
- Duracion del procesamiento de c/tipo de frame
- Tamaño de las colas
'''

import timeit

d_statistics = {'time_start':0.0,
                'time_end':0.0,
                'count_frame_ping':0,
                'count_frame_config_base':0,
                'count_frame_data':0,
                'count_accesos_SQL':0,
                'count_accesos_REDIS':0,
                'duracion_frame':0,
                'length_stats_queue':0,
                'length_data_queue':0,
                'accesos_GET':0,
                'accessos_POST':0,
                'protocolo_SPXR2':0,
                'protocolo_SPXR3':0,
                'protocolo_PLCR2':0,
                'protocolo_PLCR3':0
            }

def init_stats():
    d_statistics['time_start'] = timeit.default_timer()
    d_statistics['time_end'] = 0
    d_statistics['count_frame_config_base'] = 0
    d_statistics['count_frame_data'] = 0
    d_statistics['count_accesos_SQL'] = 0
    d_statistics['count_accesos_REDIS'] = 0
    d_statistics['stats_queue_length_REDIS'] = 0
    d_statistics['duracion_frame'] = 0
    d_statistics['length_stats_queue'] = 0
    d_statistics['length_data_queue'] = 0
    d_statistics['accesos_GET'] = 0
    d_statistics['accesos_POST'] = 0
    d_statistics['protocolo_SPXR2'] = 0
    d_statistics['protocolo_SPXR3'] = 0
    d_statistics['protocolo_PLCR2'] = 0
    d_statistics['protocolo_PLCR3'] = 0

    

def inc_count_frame_config_base():
    d_statistics['count_frame_config_base'] += 1

def inc_count_frame_data():
    d_statistics['count_frame_data'] += 1

def inc_count_accesos_SQL():
    d_statistics['count_accesos_SQL'] += 1

def inc_count_accesos_REDIS():
    d_statistics['count_accesos_REDIS'] += 1

def set_stats_queue_length(length):
    d_statistics['length_stats_queue'] = length

def set_data_queue_length(length):
    d_statistics['length_data_queue'] = length

def read_d_stats():
    return d_statistics

def end_stats():
    #
    # Termino las estadisticas.
    d_statistics['time_end'] = timeit.default_timer()
    d_statistics['duracion_frame'] = d_statistics['time_end'] - d_statistics['time_start']
    return d_statistics


