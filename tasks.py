import logging
import signal
from celery import Celery
from celery import platforms
from celery.signals import beat_init
from celery.signals import worker_process_init, worker_shutdown, worker_process_shutdown
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args

cluster = None
session = None

insert_to_daily_single_parameter_measurements_by_station_query = None
insert_to_hourly_single_parameter_measurements_by_station_query = None
insert_to_single_parameter_measurements_by_station_query = None
insert_to_daily_profile_measurements_by_station_time_query = None
insert_to_hourly_profile_measurements_by_station_time_query = None
insert_to_profile_measurements_by_station_time_query = None
insert_to_daily_parameter_group_measurements_by_station_query = None
insert_to_hourly_parameter_group_measurements_by_station_query = None
insert_to_parameter_group_measurements_by_station_query = None

log = logging.getLogger()
log.setLevel('INFO')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

app = Celery('tasks', broker_url = 'amqp://guest:guest@localhost:5672//')

@worker_process_shutdown.connect
def cassandra_teardown(**kwargs):
    log.info("Shutting down Cassandra session connection")
    session.shutdown()
    log.info("Shutting down Cassandra cluster connection")
    cluster.shutdown()

@worker_process_init.connect
@beat_init.connect
def cassandra_init(**kwargs):
    global cluster, session
    """ Initialize a clean Cassandra connection. """
    if cluster is not None:
        log.info("Shutting down Cassandra cluster connection")
        cluster.shutdown()
    if session is not None:
        log.info("Shutting down Cassandra session connection")
        session.shutdown()    
    log.info("Initializing Cassandra connection")
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect('hydroview_development')

@app.task
def insert_to_daily_single_parameter_measurements_by_station(measurements):   
    global insert_to_daily_single_parameter_measurements_by_station_query
    if insert_to_daily_single_parameter_measurements_by_station_query is None:    
        query = "INSERT INTO daily_single_parameter_measurements_by_station (station_id, parameter_id, qc_level, year, date, sensor_name, sensor_id, avg_value, unit) VALUES (?,?,?,?,?,?,?,?,?)"
        insert_to_daily_single_parameter_measurements_by_station_query = session.prepare(query)
    execute_concurrent_with_args(session, insert_to_daily_single_parameter_measurements_by_station_query, measurements, concurrency=50)

@app.task
def insert_to_hourly_single_parameter_measurements_by_station(measurements):  
    global insert_to_hourly_single_parameter_measurements_by_station_query
    if insert_to_hourly_single_parameter_measurements_by_station_query is None:    
        query = "INSERT INTO hourly_single_parameter_measurements_by_station (station_id, parameter_id, qc_level, year, date_hour, sensor_name, sensor_id, avg_value, unit) VALUES (?,?,?,?,?,?,?,?,?)"
        insert_to_hourly_single_parameter_measurements_by_station_query = session.prepare(query)
    execute_concurrent_with_args(session, insert_to_hourly_single_parameter_measurements_by_station_query, measurements, concurrency=50)

@app.task
def insert_to_single_parameter_measurements_by_station(measurements):   
    global insert_to_single_parameter_measurements_by_station_query
    if insert_to_single_parameter_measurements_by_station_query is None:    
        query = "INSERT INTO single_parameter_measurements_by_station (station_id, parameter_id, qc_level, month_first_day, timestamp, sensor_name, sensor_id, value, unit) VALUES (?,?,?,?,?,?,?,?,?)"
        insert_to_single_parameter_measurements_by_station_query = session.prepare(query)
    execute_concurrent_with_args(session, insert_to_single_parameter_measurements_by_station_query, measurements, concurrency=50)

@app.task
def insert_to_daily_profile_measurements_by_station_time(measurements):   
    global insert_to_daily_profile_measurements_by_station_time_query
    if insert_to_daily_profile_measurements_by_station_time_query is None:    
        query = "INSERT INTO daily_profile_measurements_by_station_time (station_id, parameter_id, qc_level, year, date, depth, sensor_name, sensor_id, avg_value, unit) VALUES (?,?,?,?,?,?,?,?,?,?)"
        insert_to_daily_profile_measurements_by_station_time_query = session.prepare(query)
    execute_concurrent_with_args(session, insert_to_daily_profile_measurements_by_station_time_query, measurements, concurrency=50)

@app.task
def insert_to_hourly_profile_measurements_by_station_time(measurements):   
    global insert_to_hourly_profile_measurements_by_station_time_query
    if insert_to_hourly_profile_measurements_by_station_time_query is None:    
        query = "INSERT INTO hourly_profile_measurements_by_station_time (station_id, parameter_id, qc_level, year, date_hour, depth, sensor_name, sensor_id, avg_value, unit) VALUES (?,?,?,?,?,?,?,?,?,?)"
        insert_to_hourly_profile_measurements_by_station_time_query = session.prepare(query)
    execute_concurrent_with_args(session, insert_to_hourly_profile_measurements_by_station_time_query, measurements, concurrency=50)

@app.task
def insert_to_profile_measurements_by_station_time(measurements):   
    global insert_to_profile_measurements_by_station_time_query
    if insert_to_profile_measurements_by_station_time_query is None:    
        query = "INSERT INTO profile_measurements_by_station_time (station_id, parameter_id, qc_level, month_first_day, timestamp, depth, sensor_name, sensor_id, value, unit) VALUES (?,?,?,?,?,?,?,?,?,?)"
        insert_to_profile_measurements_by_station_time_query = session.prepare(query)
    execute_concurrent_with_args(session, insert_to_profile_measurements_by_station_time_query, measurements, concurrency=50)

@app.task
def insert_to_daily_parameter_group_measurements_by_station(measurements):   
    global insert_to_daily_parameter_group_measurements_by_station_query
    if insert_to_daily_parameter_group_measurements_by_station_query is None:    
        query = "INSERT INTO daily_parameter_group_measurements_by_station (station_id, group_id, qc_level, year, date, parameter_name, sensor_name, parameter_id, sensor_id, avg_value, unit) VALUES (?,?,?,?,?,?,?,?,?,?,?)"
        insert_to_daily_parameter_group_measurements_by_station_query = session.prepare(query)
    execute_concurrent_with_args(session, insert_to_daily_parameter_group_measurements_by_station_query, measurements, concurrency=50)

@app.task
def insert_to_hourly_parameter_group_measurements_by_station(measurements):   
    global insert_to_hourly_parameter_group_measurements_by_station_query
    if insert_to_hourly_parameter_group_measurements_by_station_query is None:    
        query = "INSERT INTO hourly_parameter_group_measurements_by_station (station_id, group_id, qc_level, year, date_hour, parameter_name, sensor_name, parameter_id, sensor_id, avg_value, unit) VALUES (?,?,?,?,?,?,?,?,?,?,?)"
        insert_to_hourly_parameter_group_measurements_by_station_query = session.prepare(query)
    execute_concurrent_with_args(session, insert_to_hourly_parameter_group_measurements_by_station_query, measurements, concurrency=50)

@app.task
def insert_to_parameter_group_measurements_by_station(measurements):   
    global insert_to_parameter_group_measurements_by_station_query
    if insert_to_parameter_group_measurements_by_station_query is None:    
        query = "INSERT INTO parameter_group_measurements_by_station (station_id, group_id, qc_level, month_first_day, timestamp, parameter_name, sensor_name, parameter_id, sensor_id, value, unit) VALUES (?,?,?,?,?,?,?,?,?,?,?)"
        insert_to_parameter_group_measurements_by_station_query = session.prepare(query)
    execute_concurrent_with_args(session, insert_to_parameter_group_measurements_by_station_query, measurements, concurrency=50)

