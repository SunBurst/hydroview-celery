import logging
import signal
import uuid

from celery import Celery
from celery import platforms
from celery.signals import beat_init
from celery.signals import worker_process_init, worker_shutdown, worker_process_shutdown

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args

cluster = None
session = None

insert_to_daily_single_measurements_by_sensor_query = None
insert_to_hourly_single_measurements_by_sensor_query = None
insert_to_thirty_min_single_measurements_by_sensor_query = None
insert_to_twenty_min_single_measurements_by_sensor_query = None
insert_to_fifteen_min_single_measurements_by_sensor_query = None
insert_to_ten_min_single_measurements_by_sensor_query = None
insert_to_five_min_single_measurements_by_sensor_query = None
insert_to_one_min_single_measurements_by_sensor_query = None
insert_to_one_sec_single_measurements_by_sensor_query = None
insert_to_daily_profile_measurements_by_sensor_query = None
insert_to_hourly_profile_measurements_by_sensor_query = None
insert_to_thirty_min_profile_measurements_by_sensor_query = None
insert_to_twenty_min_profile_measurements_by_sensor_query = None
insert_to_fifteen_min_profile_measurements_by_sensor_query = None
insert_to_ten_min_profile_measurements_by_sensor_query = None
insert_to_five_min_profile_measurements_by_sensor_query = None
insert_to_one_min_profile_measurements_by_sensor_query = None
insert_to_one_sec_profile_measurements_by_sensor_query = None
insert_to_hourly_webcam_photos_by_sensor_query = None

log = logging.getLogger()
log.setLevel('DEBUG')
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
    cluster = Cluster([
        '192.168.50.10','192.168.50.11'
    ])
    session = cluster.connect('hydroview')
    session.default_consistency_level = ConsistencyLevel.LOCAL_QUORUM

@app.task
def insert_to_daily_single_measurements_by_sensor(measurements):   
    global insert_to_daily_single_measurements_by_sensor_query
    if insert_to_daily_single_measurements_by_sensor_query is None:    
        query = """
            INSERT INTO daily_single_measurements_by_sensor (sensor_id, parameter_id, 
                qc_level, year, timestamp, min_value, avg_value, max_value, unit) 
                    VALUES (?,?,?,?,?,?,?,?,?)"""
        insert_to_daily_single_measurements_by_sensor_query = session.prepare(query)
    for row in measurements:
        row[0] = uuid.UUID(row[0])
        row[1] = uuid.UUID(row[1])
    execute_concurrent_with_args(session, insert_to_daily_single_measurements_by_sensor_query, measurements, concurrency=50)

@app.task
def insert_to_hourly_single_measurements_by_sensor(measurements):  
    global insert_to_hourly_single_measurements_by_sensor_query
    if insert_to_hourly_single_measurements_by_sensor_query is None:    
        query = """
            INSERT INTO hourly_single_measurements_by_sensor (sensor_id, parameter_id, 
                qc_level, year, timestamp, min_value, avg_value, max_value, unit) 
                    VALUES (?,?,?,?,?,?,?,?,?)"""
        insert_to_hourly_single_measurements_by_sensor_query = session.prepare(query)
    for row in measurements:
        row[0] = uuid.UUID(row[0])
        row[1] = uuid.UUID(row[1])
        
    execute_concurrent_with_args(session, insert_to_hourly_single_measurements_by_sensor_query, measurements, concurrency=50)
    
@app.task
def insert_to_thirty_min_single_measurements_by_sensor(measurements):  
    global insert_to_hourly_single_measurements_by_sensor_query
    if insert_to_thirty_min_single_measurements_by_sensor_query is None:    
        query = """
            INSERT INTO thirty_min_single_measurements_by_sensor (sensor_id, parameter_id, 
                qc_level, year, timestamp, min_value, avg_value, max_value, unit) 
                    VALUES (?,?,?,?,?,?,?,?,?)"""
        insert_to_thirty_min_single_measurements_by_sensor_query = session.prepare(query)
    for row in measurements:
        row[0] = uuid.UUID(row[0])
        row[1] = uuid.UUID(row[1])
        
    execute_concurrent_with_args(session, insert_to_thirty_min_single_measurements_by_sensor_query, measurements, concurrency=50)
    
@app.task
def insert_to_twenty_min_single_measurements_by_sensor(measurements):  
    global insert_to_twenty_min_single_measurements_by_sensor_query
    if insert_to_twenty_min_single_measurements_by_sensor_query is None:    
        query = """
            INSERT INTO twenty_min_single_measurements_by_sensor (sensor_id, parameter_id, 
                qc_level, year, timestamp, min_value, avg_value, max_value, unit) 
                    VALUES (?,?,?,?,?,?,?,?,?)"""
        insert_to_twenty_min_single_measurements_by_sensor_query = session.prepare(query)
    for row in measurements:
        row[0] = uuid.UUID(row[0])
        row[1] = uuid.UUID(row[1])
        
    execute_concurrent_with_args(session, insert_to_twenty_min_single_measurements_by_sensor_query, measurements, concurrency=50)

@app.task
def insert_to_fifteen_min_single_measurements_by_sensor(measurements):  
    global insert_to_fifteen_min_single_measurements_by_sensor_query
    if insert_to_fifteen_min_single_measurements_by_sensor_query is None:    
        query = """
            INSERT INTO fifteen_min_single_measurements_by_sensor (sensor_id, parameter_id, 
                qc_level, month_first_day, timestamp, min_value, avg_value, 
                    max_value, unit) VALUES (?,?,?,?,?,?,?,?,?)"""
        insert_to_fifteen_min_single_measurements_by_sensor_query = session.prepare(query)
    for row in measurements:
        row[0] = uuid.UUID(row[0])
        row[1] = uuid.UUID(row[1])
        
    execute_concurrent_with_args(session, insert_to_fifteen_min_single_measurements_by_sensor_query, concurrency=50)

@app.task
def insert_to_ten_min_single_measurements_by_sensor(measurements):  
    global insert_to_ten_min_single_measurements_by_sensor_query
    if insert_to_ten_min_single_measurements_by_sensor_query is None:    
        query = """
            INSERT INTO ten_min_single_measurements_by_sensor (sensor_id, parameter_id, 
                qc_level, month_first_day, timestamp, min_value, avg_value, max_value, 
                    unit) VALUES (?,?,?,?,?,?,?,?,?)"""
        insert_to_ten_min_single_measurements_by_sensor_query = session.prepare(query)
    for row in measurements:
        row[0] = uuid.UUID(row[0])
        row[1] = uuid.UUID(row[1])
        
    execute_concurrent_with_args(session, insert_to_ten_min_single_measurements_by_sensor_query, concurrency=50)

@app.task
def insert_to_five_min_single_measurements_by_sensor(measurements):   
    global insert_to_five_min_single_measurements_by_sensor_query
    if insert_to_five_min_single_measurements_by_sensor_query is None:    
        query = """
            INSERT INTO five_min_single_measurements_by_sensor (sensor_id, parameter_id, 
                qc_level, month_first_day, timestamp, min_value, avg_value, max_value, unit) 
                    VALUES (?,?,?,?,?,?,?,?,?)"""
        insert_to_five_min_single_measurements_by_sensor_query = session.prepare(query)
    for row in measurements:
        row[0] = uuid.UUID(row[0])
        row[1] = uuid.UUID(row[1])
        
    execute_concurrent_with_args(session, insert_to_five_min_single_measurements_by_sensor_query, measurements, concurrency=50)

@app.task
def insert_to_one_min_single_measurements_by_sensor(measurements):   
    global insert_to_one_min_single_measurements_by_sensor_query
    if insert_to_one_min_single_measurements_by_sensor_query is None:    
        query = """
            INSERT INTO one_min_single_measurements_by_sensor (sensor_id, 
                parameter_id, qc_level, week_first_day, timestamp, min_value, avg_value, 
                    max_value, unit) VALUES (?,?,?,?,?,?,?,?,?)"""
        insert_to_one_min_single_measurements_by_sensor_query = session.prepare(query)
    for row in measurements:
        row[0] = uuid.UUID(row[0])
        row[1] = uuid.UUID(row[1])
        
    execute_concurrent_with_args(session, insert_to_one_min_single_measurements_by_sensor_query, measurements, concurrency=50)

@app.task
def insert_to_one_sec_single_measurements_by_sensor(measurements):   
    global insert_to_one_sec_single_measurements_by_sensor_query
    if insert_to_one_sec_single_measurements_by_sensor_query is None:    
        query = """
            INSERT INTO one_sec_single_measurements_by_sensor (sensor_id, parameter_id, 
                qc_level, date, timestamp, min_value, avg_value, max_value, unit) 
                    VALUES (?,?,?,?,?,?,?,?,?)"""
        insert_to_one_sec_single_measurements_by_sensor_query = session.prepare(query)
    for row in measurements:
        row[0] = uuid.UUID(row[0])
        row[1] = uuid.UUID(row[1])
        
    execute_concurrent_with_args(session, insert_to_one_sec_single_measurements_by_sensor_query, measurements, concurrency=50)

@app.task
def insert_to_daily_profile_measurements_by_sensor(measurements):   
    global insert_to_daily_profile_measurements_by_sensor_query
    if insert_to_daily_profile_measurements_by_sensor_query is None:    
        query = """
            INSERT INTO daily_profile_measurements_by_sensor (sensor_id, parameter_id, 
                qc_level, year, timestamp, vertical_position, min_value, avg_value, max_value, 
                    unit) VALUES (?,?,?,?,?,?,?,?,?,?)"""
        insert_to_daily_profile_measurements_by_sensor_query = session.prepare(query)
    for row in measurements:
        row[0] = uuid.UUID(row[0])
        row[1] = uuid.UUID(row[1])
    print(measurements)    
    execute_concurrent_with_args(session, insert_to_daily_profile_measurements_by_sensor_query, measurements, concurrency=50)

@app.task
def insert_to_hourly_profile_measurements_by_sensor(measurements):   
    global insert_to_hourly_profile_measurements_by_sensor_query
    if insert_to_hourly_profile_measurements_by_sensor_query is None:    
        query = """
            INSERT INTO hourly_profile_measurements_by_sensor (sensor_id, parameter_id, 
                qc_level, year, timestamp, vertical_position, min_value, avg_value, max_value, 
                    unit) VALUES (?,?,?,?,?,?,?,?,?,?)"""
        insert_to_hourly_profile_measurements_by_sensor_query = session.prepare(query)
    for row in measurements:
        row[0] = uuid.UUID(row[0])
        row[1] = uuid.UUID(row[1])
        
    execute_concurrent_with_args(session, insert_to_hourly_profile_measurements_by_sensor_query, measurements, concurrency=50)

@app.task
def insert_to_thirty_min_profile_measurements_by_sensor(measurements):   
    global insert_to_thirty_min_profile_measurements_by_sensor_query
    if insert_to_thirty_min_profile_measurements_by_sensor_query is None:    
        query = """INSERT INTO thirty_min_profile_measurements_by_sensor (sensor_id, 
            parameter_id, qc_level, month_first_day, timestamp, vertical_position, min_value, 
                avg_value, max_value, unit) VALUES (?,?,?,?,?,?,?,?,?,?)"""
        insert_to_thirty_min_profile_measurements_by_sensor_query = session.prepare(query)
    for row in measurements:
        row[0] = uuid.UUID(row[0])
        row[1] = uuid.UUID(row[1])
        
    execute_concurrent_with_args(session, insert_to_thirty_min_profile_measurements_by_sensor_query, measurements, concurrency=50)

@app.task
def insert_to_twenty_min_profile_measurements_by_sensor(measurements):   
    global insert_to_twenty_min_profile_measurements_by_sensor_query
    if insert_to_twenty_min_profile_measurements_by_sensor_query is None:    
        query = """
            INSERT INTO twenty_min_profile_measurements_by_sensor (sensor_id, parameter_id, 
                qc_level, year, month_first_day, vertical_position, min_value, avg_value, 
                    max_value, unit) VALUES (?,?,?,?,?,?,?,?,?,?)"""
        insert_to_twenty_min_profile_measurements_by_sensor_query = session.prepare(query)
    for row in measurements:
        row[0] = uuid.UUID(row[0])
        row[1] = uuid.UUID(row[1])
        
    execute_concurrent_with_args(session, insert_to_twenty_min_profile_measurements_by_sensor_query, measurements, concurrency=50)

@app.task
def insert_to_fifteen_min_profile_measurements_by_sensor(measurements):   
    global insert_to_fifteen_min_profile_measurements_by_sensor_query
    if insert_to_fifteen_min_profile_measurements_by_sensor_query is None:    
        query = """
            INSERT INTO fifteen_min_profile_measurements_by_sensor (sensor_id, 
                parameter_id, qc_level, year, month_first_day, vertical_position, min_value, 
                    avg_value, max_value, unit) VALUES (?,?,?,?,?,?,?,?,?,?)"""
        insert_to_fifteen_min_profile_measurements_by_sensor_query = session.prepare(query)
    for row in measurements:
        row[0] = uuid.UUID(row[0])
        row[1] = uuid.UUID(row[1])
        
    execute_concurrent_with_args(session, insert_to_fifteen_min_profile_measurements_by_sensor_query, measurements, concurrency=50)

@app.task
def insert_to_ten_min_profile_measurements_by_sensor(measurements):   
    global insert_to_ten_min_profile_measurements_by_sensor_query
    if insert_to_ten_min_profile_measurements_by_sensor_query is None:    
        query = """
            INSERT INTO ten_min_profile_measurements_by_sensor (sensor_id, parameter_id, 
                qc_level, year, month_first_day, vertical_position, min_value, avg_value, 
                    max_value, unit) VALUES (?,?,?,?,?,?,?,?,?,?)"""
        insert_to_ten_min_profile_measurements_by_sensor_query = session.prepare(query)
    execute_concurrent_with_args(session, insert_to_ten_min_profile_measurements_by_sensor_query, measurements, concurrency=50)

@app.task
def insert_to_five_min_profile_measurements_by_sensor(measurements):   
    global insert_to_five_min_profile_measurements_by_sensor_query
    if insert_to_five_min_profile_measurements_by_sensor_query is None:    
        query = """
            INSERT INTO five_min_profile_measurements_by_sensor (sensor_id, 
                parameter_id, qc_level, month_first_day, timestamp, vertical_position, min_value, 
                    avg_value, max_value, unit) VALUES (?,?,?,?,?,?,?,?,?,?)"""
        insert_to_five_min_profile_measurements_by_sensor_query = session.prepare(query)
    for row in measurements:
        row[0] = uuid.UUID(row[0])
        row[1] = uuid.UUID(row[1])
        
    execute_concurrent_with_args(session, insert_to_five_min_profile_measurements_by_sensor_query, measurements, concurrency=50)

@app.task
def insert_to_one_min_profile_measurements_by_sensor(measurements):   
    global insert_to_one_min_profile_measurements_by_sensor_query
    if insert_to_one_min_profile_measurements_by_sensor_query is None:    
        query = """
            INSERT INTO one_min_profile_measurements_by_sensor (sensor_id, parameter_id, 
                qc_level, week_first_day, timestamp, vertical_position, min_value, avg_value, 
                    max_value, unit) VALUES (?,?,?,?,?,?,?,?,?,?)"""
        insert_to_one_min_profile_measurements_by_sensor_query = session.prepare(query)
    for row in measurements:
        row[0] = uuid.UUID(row[0])
        row[1] = uuid.UUID(row[1])
        
    execute_concurrent_with_args(session, insert_to_one_min_profile_measurements_by_sensor_query, measurements, concurrency=50)

@app.task
def insert_to_one_sec_profile_measurements_by_sensor(measurements):   
    global insert_to_one_sec_profile_measurements_by_sensor_query
    if insert_to_one_sec_profile_measurements_by_sensor_query is None:    
        query = """
            INSERT INTO one_sec_profile_measurements_by_sensor (sensor_id, parameter_id, 
                qc_level, date, timestamp, vertical_position, min_value, avg_value, max_value, 
                    unit) VALUES (?,?,?,?,?,?,?,?,?,?)"""
        insert_to_one_sec_profile_measurements_by_sensor_query = session.prepare(query)
    for row in measurements:
        row[0] = uuid.UUID(row[0])
        row[1] = uuid.UUID(row[1])
        
    execute_concurrent_with_args(session, insert_to_one_sec_profile_measurements_by_sensor_query, measurements, concurrency=50)
    
@app.task
def insert_to_hourly_webcam_photos_by_station(photos):
    global insert_to_hourly_webcam_photos_by_station_query
    if insert_to_hourly_webcam_photos_by_station_query is None:    
        query = "INSERT INTO hourly_webcam_photos_by_station (station_id, date, timestamp, photo) VALUES (?,?,?,?)"
        insert_to_hourly_webcam_photos_by_station_query = session.prepare(query)
    for row in photos:
        row[0] = uuid.UUID(row[0])
        row[3] = bytearray.fromhex(row[3])
    execute_concurrent_with_args(session, insert_to_hourly_webcam_photos_by_station_query, photos, concurrency=50)
