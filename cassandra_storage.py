#!/usr/bin/env
# -*- coding: utf-8 -*-

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse
import datetime
import logging.config

import pytz

from campbellsciparser import cr

from tasks import insert_to_daily_single_measurements_by_sensor
from tasks import insert_to_hourly_single_measurements_by_sensor
from tasks import insert_to_thirty_min_single_measurements_by_sensor
from tasks import insert_to_twenty_min_single_measurements_by_sensor
from tasks import insert_to_fifteen_min_single_measurements_by_sensor
from tasks import insert_to_ten_min_single_measurements_by_sensor
from tasks import insert_to_five_min_single_measurements_by_sensor
from tasks import insert_to_one_min_single_measurements_by_sensor
from tasks import insert_to_one_sec_single_measurements_by_sensor
from tasks import insert_to_daily_profile_measurements_by_sensor
from tasks import insert_to_hourly_profile_measurements_by_sensor
from tasks import insert_to_thirty_min_profile_measurements_by_sensor
from tasks import insert_to_twenty_min_profile_measurements_by_sensor
from tasks import insert_to_fifteen_min_profile_measurements_by_sensor
from tasks import insert_to_ten_min_profile_measurements_by_sensor
from tasks import insert_to_five_min_profile_measurements_by_sensor
from tasks import insert_to_one_min_profile_measurements_by_sensor
from tasks import insert_to_one_sec_profile_measurements_by_sensor

import utils

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
APP_CONFIG_PATH = os.path.join(BASE_DIR, 'cfg/appconfig.yaml')
LOGGING_CONFIG_PATH = os.path.join(BASE_DIR, 'cfg/logging.yaml')

logging_conf = utils.load_config(LOGGING_CONFIG_PATH)
logging.config.dictConfig(logging_conf)
logger_info = logging.getLogger('cassandra_storage_info')
logger_debug = logging.getLogger('cassandra_storage_debug')


def process_daily_profile_measurements_by_sensor(station, file):
    if file.get('source') == 'profiles':
        num_of_new_rows = process_daily_profile_measurements_by_sensor_profile_source(station, file)
    elif file.get('source') == 'parameters':
        num_of_new_rows = process_daily_parameters_to_profile_measurements_by_sensor(station, file)
    else:
        raise TypeError("source must be either profiles or parameters, got {}".format(file.get('source')))
    
    return num_of_new_rows

def process_daily_profile_measurements_by_sensor_profile_source(station, file):
    path=file.get('path')
    #sensor_id = file.get('sensor_id')
    header_row=file.get('header_row')
    first_line_num = file.get('first_line_num', 0)
    time_format_args_library = file.get('time_format_args_library')
    time_zone = file.get('time_zone')
    to_utc = file.get('to_utc')
    vertical_position_column = file.get('vertical_position_column')
    vertical_position_correction_factor = file.get('vertical_position_correction_factor')
    time_columns = file.get('time_columns')        
    parse_time_columns = file.get('parse_time_columns')
    parameters = file.get('parameters')
    logger_debug.debug(file)
    data = cr.read_table_data(infile_path=path, header_row=header_row, first_line_num=first_line_num, parse_time_columns=parse_time_columns, time_zone=time_zone, time_format_args_library=time_format_args_library, time_parsed_column="timestamp", time_columns=time_columns, to_utc=to_utc)

    num_of_new_rows = len(data)

    for param, param_info in parameters.items():
        parameter_id = param_info.get('parameter_id')
        unit = param_info.get('unit')
        sensor_id = param_info.get('sensor_id')
        value_type = param_info.get('value_type')
        param_data = cr.extract_columns_data(data, "timestamp", vertical_position_column, param)
        param_formatted_data = []
        for row in param_data:
            ts = row.get('timestamp')
            day = datetime.datetime(ts.year, ts.month, ts.day)
            year = int(day.strftime("%Y"))
            vertical_position = float(row.get(vertical_position_column))
            if vertical_position_correction_factor is not None:
                vertical_position = utils.round_of_rating(vertical_position, vertical_position_correction_factor)
            min_value, avg_value, max_value = None, None, None
            if value_type == 'min_value':
                min_value = float(row.get(param))
            elif value_type == 'avg_value':
                avg_value = float(row.get(param))
            elif value_type == 'max_value':
                max_value = float(row.get(param))
            param_formatted_data.append((sensor_id, parameter_id, 0, year, int(day.timestamp()) * 1e3, vertical_position, min_value, avg_value, max_value, unit))

        insert_to_daily_profile_measurements_by_sensor.delay(param_formatted_data)
 
    return num_of_new_rows
    
def process_daily_parameters_to_profile_measurements_by_sensor(station, file):
    path=file.get('path')
    #sensor_id = file.get('sensor_id')
    header_row=file.get('header_row')
    first_line_num = file.get('first_line_num', 0)
    time_format_args_library = file.get('time_format_args_library')
    time_zone = file.get('time_zone')
    to_utc = file.get('to_utc')
    time_columns = file.get('time_columns')        
    parse_time_columns = file.get('parse_time_columns')
    parameters = file.get('parameters')
    logger_debug.debug(file)
    data = cr.read_table_data(infile_path=path, header_row=header_row, first_line_num=first_line_num, parse_time_columns=parse_time_columns, time_zone=time_zone, time_format_args_library=time_format_args_library, time_parsed_column="timestamp", time_columns=time_columns, to_utc=to_utc)

    num_of_new_rows = len(data)

    for param, param_info in parameters.items():
        parameter_id = param_info.get('parameter_id')
        unit = param_info.get('unit')
        sensor_id = param_info.get('sensor_id')
        value_type = param_info.get('value_type')
        vertical_position = float(param_info.get('vertical_position'))
        param_data = cr.extract_columns_data(data, "timestamp", param)
        param_formatted_data = []
        for row in param_data:
            ts = row.get('timestamp')
            day = datetime.datetime(ts.year, ts.month, ts.day)
            year = int(day.strftime("%Y"))
            min_value, avg_value, max_value = None, None, None
            if value_type == 'min_value':
                min_value = float(row.get(param))
            elif value_type == 'avg_value':
                avg_value = float(row.get(param))
            elif value_type == 'max_value':
                max_value = float(row.get(param))
            param_formatted_data.append((sensor_id, parameter_id, 0, year, int(day.timestamp()) * 1e3, vertical_position, min_value, avg_value, max_value, unit))

        insert_to_daily_profile_measurements_by_sensor.delay(param_formatted_data)
 
    return num_of_new_rows

def process_hourly_profile_measurements_by_sensor(station, file):
    if file.get('source') == 'profiles':
        num_of_new_rows = process_hourly_profile_measurements_by_sensor_profile_source(station, file)
    elif file.get('source') == 'parameters':
        num_of_new_rows = process_hourly_parameters_to_profile_measurements_by_sensor(station, file)
    else:
        raise TypeError("source must be either profiles or parameters, got {}".format(file.get('source')))
    
    return num_of_new_rows

def process_hourly_profile_measurements_by_sensor_profile_source(station, file):
    path=file.get('path')
    #sensor_id = file.get('sensor_id')
    header_row=file.get('header_row')
    first_line_num = file.get('first_line_num', 0)
    time_format_args_library = file.get('time_format_args_library')
    time_zone = file.get('time_zone')
    to_utc = file.get('to_utc')
    vertical_position_column = file.get('vertical_position_column')
    vertical_position_correction_factor = file.get('vertical_position_correction_factor')
    time_columns = file.get('time_columns')        
    parse_time_columns = file.get('parse_time_columns')
    parameters = file.get('parameters')
    logger_debug.debug(file)
    data = cr.read_table_data(infile_path=path, header_row=header_row, first_line_num=first_line_num, parse_time_columns=parse_time_columns, time_zone=time_zone, time_format_args_library=time_format_args_library, time_parsed_column="timestamp", time_columns=time_columns, to_utc=to_utc)

    num_of_new_rows = len(data)

    for param, param_info in parameters.items():
        parameter_id = param_info.get('parameter_id')
        unit = param_info.get('unit')
        sensor_id = param_info.get('sensor_id')
        param_data = cr.extract_columns_data(data, "timestamp", vertical_position_column, param)
        param_formatted_data = []
        for row in param_data:
            ts = row.get('timestamp')
            day = datetime.datetime(ts.year, ts.month, ts.day)
            year = int(day.strftime("%Y"))
            hour = datetime.datetime(ts.year, ts.month, ts.day, ts.hour, ts.minute, ts.second)
            vertical_position = float(row.get(vertical_position_column))
            value_type = param_info.get('value_type')
            if vertical_position_correction_factor is not None:
                vertical_position = utils.round_of_rating(vertical_position, vertical_position_correction_factor)
            min_value, avg_value, max_value = None, None, None
            if value_type == 'min_value':
                min_value = float(row.get(param))
            elif value_type == 'avg_value':
                avg_value = float(row.get(param))
            elif value_type == 'max_value':
                max_value = float(row.get(param))
            param_formatted_data.append((sensor_id, parameter_id, 0, year, int(hour.timestamp()) * 1e3, vertical_position, min_value, avg_value, max_value, unit))

        insert_to_hourly_profile_measurements_by_sensor.delay(param_formatted_data)
 
    return num_of_new_rows
    
def process_hourly_parameters_to_profile_measurements_by_sensor(station, file):
    path=file.get('path')
    #sensor_id = file.get('sensor_id')
    header_row=file.get('header_row')
    first_line_num = file.get('first_line_num', 0)
    time_format_args_library = file.get('time_format_args_library')
    time_zone = file.get('time_zone')
    time_columns = file.get('time_columns')
    to_utc = file.get('to_utc')
    parse_time_columns = file.get('parse_time_columns')
    parameters = file.get('parameters')
    logger_debug.debug(file)
    data = cr.read_table_data(infile_path=path, header_row=header_row, first_line_num=first_line_num, parse_time_columns=parse_time_columns, time_zone=time_zone, time_format_args_library=time_format_args_library, time_parsed_column="timestamp", time_columns=time_columns, to_utc=to_utc)

    num_of_new_rows = len(data)

    for param, param_info in parameters.items():
        parameter_id = param_info.get('parameter_id')
        unit = param_info.get('unit')
        sensor_id = param_info.get('sensor_id')
        value_type = param_info.get('value_type')
        vertical_position = float(param_info.get('vertical_position'))
        param_data = cr.extract_columns_data(data, "timestamp", param)
        param_formatted_data = []
        for row in param_data:
            ts = row.get('timestamp')
            day = datetime.datetime(ts.year, ts.month, ts.day)
            year = int(day.strftime("%Y"))
            hour = datetime.datetime(ts.year, ts.month, ts.day, ts.hour, ts.minute, ts.second)
            min_value, avg_value, max_value = None, None, None
            if value_type == 'min_value':
                min_value = float(row.get(param))
            elif value_type == 'avg_value':
                avg_value = float(row.get(param))
            elif value_type == 'max_value':
                max_value = float(row.get(param))
            param_formatted_data.append((sensor_id, parameter_id, 0, year, int(hour.timestamp()) * 1e3, vertical_position, min_value, avg_value, max_value, unit))

        insert_to_hourly_profile_measurements_by_sensor.delay(param_formatted_data)
 
    return num_of_new_rows

def process_thirty_min_profile_measurements_by_sensor(station, file):
    if file.get('source') == 'profiles':
        num_of_new_rows = process_thirty_min_profile_measurements_by_sensor_profile_source(station, file)
    elif file.get('source') == 'parameters':
        num_of_new_rows = process_thirty_min_parameters_to_profile_measurements_by_sensor(station, file)
    else:
        raise TypeError("source must be either profiles or parameters, got {}".format(file.get('source')))
    
    return num_of_new_rows

def process_thirty_min_profile_measurements_by_sensor_profile_source(station, file):
    path=file.get('path')
    #sensor_id = file.get('sensor_id')
    header_row=file.get('header_row')
    first_line_num = file.get('first_line_num', 0)
    time_format_args_library = file.get('time_format_args_library')
    time_zone = file.get('time_zone')
    to_utc = file.get('to_utc')
    vertical_position_column = file.get('vertical_position_column')
    vertical_position_correction_factor = file.get('vertical_position_correction_factor')
    time_columns = file.get('time_columns')        
    parse_time_columns = file.get('parse_time_columns')
    parameters = file.get('parameters')
    logger_debug.debug(file)
    data = cr.read_table_data(infile_path=path, header_row=header_row, first_line_num=first_line_num, parse_time_columns=parse_time_columns, time_zone=time_zone, time_format_args_library=time_format_args_library, time_parsed_column="timestamp", time_columns=time_columns, to_utc=to_utc)

    num_of_new_rows = len(data)

    for param, param_info in parameters.items():
        parameter_id = param_info.get('parameter_id')
        unit = param_info.get('unit')
        sensor_id = param_info.get('sensor_id')
        value_type = param_info.get('value_type')
        param_data = cr.extract_columns_data(data, "timestamp", vertical_position_column, param)
        param_formatted_data = []
        for row in param_data:
            ts = row.get('timestamp')
            month_first_day = datetime.datetime(ts.year, ts.month, 1).strftime("%Y-%m-%d")
            profile_ts = datetime.datetime(ts.year, ts.month, ts.day, ts.hour, ts.minute, ts.second)
            vertical_position = float(row.get(vertical_position_column))
            if vertical_position_correction_factor is not None:
                vertical_position = utils.round_of_rating(vertical_position, vertical_position_correction_factor)
            min_value, avg_value, max_value = None, None, None
            if value_type == 'min_value':
                min_value = float(row.get(param))
            elif value_type == 'avg_value':
                avg_value = float(row.get(param))
            elif value_type == 'max_value':
                max_value = float(row.get(param))
            param_formatted_data.append((sensor_id, parameter_id, 0, month_first_day, int(hour.timestamp()) * 1e3, vertical_position, min_value, avg_value, max_value, unit))

        insert_to_thirty_min_profile_measurements_by_sensor.delay(param_formatted_data)
 
    return num_of_new_rows

def process_thirty_min_parameters_to_profile_measurements_by_sensor(station, file):
    path=file.get('path')
    #sensor_id = file.get('sensor_id')
    header_row=file.get('header_row')
    first_line_num = file.get('first_line_num', 0)
    time_format_args_library = file.get('time_format_args_library')
    time_zone = file.get('time_zone')
    time_columns = file.get('time_columns')
    to_utc = file.get('to_utc')
    parse_time_columns = file.get('parse_time_columns')
    parameters = file.get('parameters')
    logger_debug.debug(file)
    data = cr.read_table_data(infile_path=path, header_row=header_row, first_line_num=first_line_num, parse_time_columns=parse_time_columns, time_zone=time_zone, time_format_args_library=time_format_args_library, time_parsed_column="timestamp", time_columns=time_columns, to_utc=to_utc)

    num_of_new_rows = len(data)

    for param, param_info in parameters.items():
        parameter_id = param_info.get('parameter_id')
        unit = param_info.get('unit')
        sensor_id = param_info.get('sensor_id')
        value_type = param_info.get('value_type')
        vertical_position = float(param_info.get('vertical_position'))
        param_data = cr.extract_columns_data(data, "timestamp", param)
        param_formatted_data = []
        for row in param_data:
            ts = row.get('timestamp')
            month_first_day = datetime.datetime(ts.year, ts.month, 1).strftime("%Y-%m-%d")
            profile_ts = datetime.datetime(ts.year, ts.month, ts.day, ts.hour, ts.minute, ts.second)
            min_value, avg_value, max_value = None, None, None
            if value_type == 'min_value':
                min_value = float(row.get(param))
            elif value_type == 'avg_value':
                avg_value = float(row.get(param))
            elif value_type == 'max_value':
                max_value = float(row.get(param))
            param_formatted_data.append((sensor_id, parameter_id, 0, month_first_day, int(profile_ts.timestamp()) * 1e3, vertical_position, min_value, avg_value, max_value, unit))

        insert_to_thirty_min_profile_measurements_by_sensor.delay(param_formatted_data)
 
    return num_of_new_rows

def process_twenty_min_profile_measurements_by_sensor(station, file):
    if file.get('source') == 'profiles':
        num_of_new_rows = process_twenty_min_profile_measurements_by_sensor_profile_source(station, file)
    elif file.get('source') == 'parameters':
        num_of_new_rows = process_twenty_min_parameters_to_profile_measurements_by_sensor(station, file)
    else:
        raise TypeError("source must be either profiles or parameters, got {}".format(file.get('source')))
    
    return num_of_new_rows

def process_twenty_min_profile_measurements_by_sensor_profile_source(station, file):
    path=file.get('path')
    #sensor_id = file.get('sensor_id')
    header_row=file.get('header_row')
    first_line_num = file.get('first_line_num', 0)
    time_format_args_library = file.get('time_format_args_library')
    time_zone = file.get('time_zone')
    to_utc = file.get('to_utc')
    vertical_position_column = file.get('vertical_position_column')
    vertical_position_correction_factor = file.get('vertical_position_correction_factor')
    time_columns = file.get('time_columns')        
    parse_time_columns = file.get('parse_time_columns')
    parameters = file.get('parameters')
    logger_debug.debug(file)
    data = cr.read_table_data(infile_path=path, header_row=header_row, first_line_num=first_line_num, parse_time_columns=parse_time_columns, time_zone=time_zone, time_format_args_library=time_format_args_library, time_parsed_column="timestamp", time_columns=time_columns, to_utc=to_utc)

    num_of_new_rows = len(data)

    for param, param_info in parameters.items():
        parameter_id = param_info.get('parameter_id')
        unit = param_info.get('unit')
        sensor_id = param_info.get('sensor_id')
        value_type = param_info.get('value_type')
        param_data = cr.extract_columns_data(data, "timestamp", vertical_position_column, param)
        param_formatted_data = []
        for row in param_data:
            ts = row.get('timestamp')
            month_first_day = datetime.datetime(ts.year, ts.month, 1).strftime("%Y-%m-%d")
            profile_ts = datetime.datetime(ts.year, ts.month, ts.day, ts.hour, ts.minute, ts.second)
            vertical_position = float(row.get(vertical_position_column))
            if vertical_position_correction_factor is not None:
                vertical_position = utils.round_of_rating(vertical_position, vertical_position_correction_factor)
            min_value, avg_value, max_value = None, None, None
            if value_type == 'min_value':
                min_value = float(row.get(param))
            elif value_type == 'avg_value':
                avg_value = float(row.get(param))
            elif value_type == 'max_value':
                max_value = float(row.get(param))
            param_formatted_data.append((sensor_id, parameter_id, 0, month_first_day, int(hour.timestamp()) * 1e3, vertical_position, min_value, avg_value, max_value, unit))

        insert_to_twenty_min_profile_measurements_by_sensor.delay(param_formatted_data)
 
    return num_of_new_rows

def process_twenty_min_parameters_to_profile_measurements_by_sensor(station, file):
    path=file.get('path')
    #sensor_id = file.get('sensor_id')
    header_row=file.get('header_row')
    first_line_num = file.get('first_line_num', 0)
    time_format_args_library = file.get('time_format_args_library')
    time_zone = file.get('time_zone')
    time_columns = file.get('time_columns')
    to_utc = file.get('to_utc')
    parse_time_columns = file.get('parse_time_columns')
    parameters = file.get('parameters')
    logger_debug.debug(file)
    data = cr.read_table_data(infile_path=path, header_row=header_row, first_line_num=first_line_num, parse_time_columns=parse_time_columns, time_zone=time_zone, time_format_args_library=time_format_args_library, time_parsed_column="timestamp", time_columns=time_columns, to_utc=to_utc)

    num_of_new_rows = len(data)

    for param, param_info in parameters.items():
        parameter_id = param_info.get('parameter_id')
        unit = param_info.get('unit')
        sensor_id = param_info.get('sensor_id')
        value_type = param_info.get('value_type')
        vertical_position = float(param_info.get('vertical_position'))
        param_data = cr.extract_columns_data(data, "timestamp", param)
        param_formatted_data = []
        for row in param_data:
            ts = row.get('timestamp')
            month_first_day = datetime.datetime(ts.year, ts.month, 1).strftime("%Y-%m-%d")
            profile_ts = datetime.datetime(ts.year, ts.month, ts.day, ts.hour, ts.minute, ts.second)
            min_value, avg_value, max_value = None, None, None
            if value_type == 'min_value':
                min_value = float(row.get(param))
            elif value_type == 'avg_value':
                avg_value = float(row.get(param))
            elif value_type == 'max_value':
                max_value = float(row.get(param))
            param_formatted_data.append((sensor_id, parameter_id, 0, month_first_day, int(profile_ts.timestamp()) * 1e3, vertical_position, min_value, avg_value, max_value, unit))

        insert_to_twenty_profile_measurements_by_sensor.delay(param_formatted_data)
 
    return num_of_new_rows

def process_fifteen_min_profile_measurements_by_sensor(station, file):
    if file.get('source') == 'profiles':
        num_of_new_rows = process_fifteen_min_profile_measurements_by_sensor_profile_source(station, file)
    elif file.get('source') == 'parameters':
        num_of_new_rows = process_fifteen_min_parameters_to_profile_measurements_by_sensor(station, file)
    else:
        raise TypeError("source must be either profiles or parameters, got {}".format(file.get('source')))
    
    return num_of_new_rows

def process_fifteen_min_profile_measurements_by_sensor_profile_source(station, file):
    path=file.get('path')
    #sensor_id = file.get('sensor_id')
    header_row=file.get('header_row')
    first_line_num = file.get('first_line_num', 0)
    time_format_args_library = file.get('time_format_args_library')
    time_zone = file.get('time_zone')
    to_utc = file.get('to_utc')
    vertical_position_column = file.get('vertical_position_column')
    vertical_position_correction_factor = file.get('vertical_position_correction_factor')
    time_columns = file.get('time_columns')        
    parse_time_columns = file.get('parse_time_columns')
    parameters = file.get('parameters')
    logger_debug.debug(file)
    data = cr.read_table_data(infile_path=path, header_row=header_row, first_line_num=first_line_num, parse_time_columns=parse_time_columns, time_zone=time_zone, time_format_args_library=time_format_args_library, time_parsed_column="timestamp", time_columns=time_columns, to_utc=to_utc)

    num_of_new_rows = len(data)

    for param, param_info in parameters.items():
        parameter_id = param_info.get('parameter_id')
        unit = param_info.get('unit')
        sensor_id = param_info.get('sensor_id')
        value_type = param_info.get('value_type')
        param_data = cr.extract_columns_data(data, "timestamp", vertical_position_column, param)
        param_formatted_data = []
        for row in param_data:
            ts = row.get('timestamp')
            month_first_day = datetime.datetime(ts.year, ts.month, 1).strftime("%Y-%m-%d")
            profile_ts = datetime.datetime(ts.year, ts.month, ts.day, ts.hour, ts.minute, ts.second)
            vertical_position = float(row.get(vertical_position_column))
            if vertical_position_correction_factor is not None:
                vertical_position = utils.round_of_rating(vertical_position, vertical_position_correction_factor)
            min_value, avg_value, max_value = None, None, None
            if value_type == 'min_value':
                min_value = float(row.get(param))
            elif value_type == 'avg_value':
                avg_value = float(row.get(param))
            elif value_type == 'max_value':
                max_value = float(row.get(param))
            param_formatted_data.append((sensor_id, parameter_id, 0, month_first_day, int(hour.timestamp()) * 1e3, vertical_position, min_value, avg_value, max_value, unit))

        insert_to_fifteen_min_profile_measurements_by_sensor.delay(param_formatted_data)
 
    return num_of_new_rows

def process_fifteen_min_parameters_to_profile_measurements_by_sensor(station, file):
    path=file.get('path')
    #sensor_id = file.get('sensor_id')
    header_row=file.get('header_row')
    first_line_num = file.get('first_line_num', 0)
    time_format_args_library = file.get('time_format_args_library')
    time_zone = file.get('time_zone')
    time_columns = file.get('time_columns')
    to_utc = file.get('to_utc')
    parse_time_columns = file.get('parse_time_columns')
    parameters = file.get('parameters')
    logger_debug.debug(file)
    data = cr.read_table_data(infile_path=path, header_row=header_row, first_line_num=first_line_num, parse_time_columns=parse_time_columns, time_zone=time_zone, time_format_args_library=time_format_args_library, time_parsed_column="timestamp", time_columns=time_columns, to_utc=to_utc)

    num_of_new_rows = len(data)

    for param, param_info in parameters.items():
        parameter_id = param_info.get('parameter_id')
        unit = param_info.get('unit')
        sensor_id = param_info.get('sensor_id')
        value_type = param_info.get('value_type')
        vertical_position = float(param_info.get('vertical_position'))
        param_data = cr.extract_columns_data(data, "timestamp", param)
        param_formatted_data = []
        for row in param_data:
            ts = row.get('timestamp')
            month_first_day = datetime.datetime(ts.year, ts.month, 1).strftime("%Y-%m-%d")
            profile_ts = datetime.datetime(ts.year, ts.month, ts.day, ts.hour, ts.minute, ts.second)
            min_value, avg_value, max_value = None, None, None
            if value_type == 'min_value':
                min_value = float(row.get(param))
            elif value_type == 'avg_value':
                avg_value = float(row.get(param))
            elif value_type == 'max_value':
                max_value = float(row.get(param))
            param_formatted_data.append((sensor_id, parameter_id, 0, month_first_day, int(profile_ts.timestamp()) * 1e3, vertical_position, min_value, avg_value, max_value, unit))

        insert_to_fifteen_profile_measurements_by_sensor.delay(param_formatted_data)
 
    return num_of_new_rows

def process_ten_min_profile_measurements_by_sensor(station, file):
    if file.get('source') == 'profiles':
        num_of_new_rows = process_ten_min_profile_measurements_by_sensor_profile_source(station, file)
    elif file.get('source') == 'parameters':
        num_of_new_rows = process_ten_min_parameters_to_profile_measurements_by_sensor(station, file)
    else:
        raise TypeError("source must be either profiles or parameters, got {}".format(file.get('source')))
    
    return num_of_new_rows

def process_ten_min_profile_measurements_by_sensor_profile_source(station, file):
    path=file.get('path')
    #sensor_id = file.get('sensor_id')
    header_row=file.get('header_row')
    first_line_num = file.get('first_line_num', 0)
    time_format_args_library = file.get('time_format_args_library')
    time_zone = file.get('time_zone')
    to_utc = file.get('to_utc')
    vertical_position_column = file.get('vertical_position_column')
    vertical_position_correction_factor = file.get('vertical_position_correction_factor')
    time_columns = file.get('time_columns')        
    parse_time_columns = file.get('parse_time_columns')
    parameters = file.get('parameters')
    logger_debug.debug(file)
    data = cr.read_table_data(infile_path=path, header_row=header_row, first_line_num=first_line_num, parse_time_columns=parse_time_columns, time_zone=time_zone, time_format_args_library=time_format_args_library, time_parsed_column="timestamp", time_columns=time_columns, to_utc=to_utc)

    num_of_new_rows = len(data)

    for param, param_info in parameters.items():
        parameter_id = param_info.get('parameter_id')
        unit = param_info.get('unit')
        sensor_id = param_info.get('sensor_id')
        value_type = param_info.get('value_type')
        param_data = cr.extract_columns_data(data, "timestamp", vertical_position_column, param)
        param_formatted_data = []
        for row in param_data:
            ts = row.get('timestamp')
            month_first_day = datetime.datetime(ts.year, ts.month, 1).strftime("%Y-%m-%d")
            profile_ts = datetime.datetime(ts.year, ts.month, ts.day, ts.hour, ts.minute, ts.second)
            vertical_position = float(row.get(vertical_position_column))
            if vertical_position_correction_factor is not None:
                vertical_position = utils.round_of_rating(vertical_position, vertical_position_correction_factor)
            min_value, avg_value, max_value = None, None, None
            if value_type == 'min_value':
                min_value = float(row.get(param))
            elif value_type == 'avg_value':
                avg_value = float(row.get(param))
            elif value_type == 'max_value':
                max_value = float(row.get(param))
            param_formatted_data.append((sensor_id, parameter_id, 0, month_first_day, int(hour.timestamp()) * 1e3, vertical_position, min_value, avg_value, max_value, unit))

        insert_to_ten_min_profile_measurements_by_sensor.delay(param_formatted_data)
 
    return num_of_new_rows

def process_ten_min_parameters_to_profile_measurements_by_sensor(station, file):
    path=file.get('path')
    #sensor_id = file.get('sensor_id')
    header_row=file.get('header_row')
    first_line_num = file.get('first_line_num', 0)
    time_format_args_library = file.get('time_format_args_library')
    time_zone = file.get('time_zone')
    time_columns = file.get('time_columns')
    to_utc = file.get('to_utc')
    parse_time_columns = file.get('parse_time_columns')
    parameters = file.get('parameters')
    logger_debug.debug(file)
    data = cr.read_table_data(infile_path=path, header_row=header_row, first_line_num=first_line_num, parse_time_columns=parse_time_columns, time_zone=time_zone, time_format_args_library=time_format_args_library, time_parsed_column="timestamp", time_columns=time_columns, to_utc=to_utc)

    num_of_new_rows = len(data)

    for param, param_info in parameters.items():
        parameter_id = param_info.get('parameter_id')
        unit = param_info.get('unit')
        sensor_id = param_info.get('sensor_id')
        value_type = param_info.get('value_type')
        vertical_position = float(param_info.get('vertical_position'))
        param_data = cr.extract_columns_data(data, "timestamp", param)
        param_formatted_data = []
        for row in param_data:
            ts = row.get('timestamp')
            month_first_day = datetime.datetime(ts.year, ts.month, 1).strftime("%Y-%m-%d")
            profile_ts = datetime.datetime(ts.year, ts.month, ts.day, ts.hour, ts.minute, ts.second)
            min_value, avg_value, max_value = None, None, None
            if value_type == 'min_value':
                min_value = float(row.get(param))
            elif value_type == 'avg_value':
                avg_value = float(row.get(param))
            elif value_type == 'max_value':
                max_value = float(row.get(param))
            param_formatted_data.append((sensor_id, parameter_id, 0, month_first_day, int(profile_ts.timestamp()) * 1e3, vertical_position, min_value, avg_value, max_value, unit))

        insert_to_ten_profile_measurements_by_sensor.delay(param_formatted_data)
 
    return num_of_new_rows

def process_five_min_profile_measurements_by_sensor(station, file):
    if file.get('source') == 'profiles':
        num_of_new_rows = process_five_min_profile_measurements_by_sensor_profile_source(station, file)
    elif file.get('source') == 'parameters':
        num_of_new_rows = process_five_min_parameters_to_profile_measurements_by_sensor(station, file)
    else:
        raise TypeError("source must be either profiles or parameters, got {}".format(file.get('source')))
    
    return num_of_new_rows

def process_five_min_profile_measurements_by_sensor_profile_source(station, file):
    path=file.get('path')
    #sensor_id = file.get('sensor_id')
    eader_row=file.get('header_row')
    first_line_num = file.get('first_line_num', 0)
    time_format_args_library = file.get('time_format_args_library')
    time_zone = file.get('time_zone')
    to_utc = file.get('to_utc')
    vertical_position_column = file.get('vertical_position_column')
    vertical_position_correction_factor = file.get('vertical_position_correction_factor')
    time_columns = file.get('time_columns')        
    parse_time_columns = file.get('parse_time_columns')
    parameters = file.get('parameters')
    logger_debug.debug(file)
    data = cr.read_table_data(infile_path=path, header_row=header_row, first_line_num=first_line_num, parse_time_columns=parse_time_columns, time_zone=time_zone, time_format_args_library=time_format_args_library, time_parsed_column="timestamp", time_columns=time_columns, to_utc=to_utc)

    num_of_new_rows = len(data)

    for param, param_info in parameters.items():
        parameter_id = param_info.get('parameter_id')
        unit = param_info.get('unit')
        sensor_id = param_info.get('sensor_id')
        value_type = param_info.get('value_type')
        param_data = cr.extract_columns_data(data, "timestamp", vertical_position_column, param)
        param_formatted_data = []
        for row in param_data:
            ts = row.get('timestamp')
            month_first_day = datetime.datetime(ts.year, ts.month, 1).strftime("%Y-%m-%d")
            profile_ts = datetime.datetime(ts.year, ts.month, ts.day, ts.hour, ts.minute, ts.second)
            vertical_position = float(row.get(vertical_position_column))
            if vertical_position_correction_factor is not None:
                vertical_position = utils.round_of_rating(vertical_position, vertical_position_correction_factor)
            min_value, avg_value, max_value = None, None, None
            if value_type == 'min_value':
                min_value = float(row.get(param))
            elif value_type == 'avg_value':
                avg_value = float(row.get(param))
            elif value_type == 'max_value':
                max_value = float(row.get(param))
            param_formatted_data.append((sensor_id, parameter_id, 0, month_first_day, int(profile_ts.timestamp()) * 1e3, vertical_position, min_value, avg_value, max_value, unit))

        insert_to_five_min_profile_measurements_by_sensor.delay(param_formatted_data)
 
    return num_of_new_rows
    
def process_five_min_parameters_to_profile_measurements_by_sensor(station, file):
    path=file.get('path')
    #sensor_id = file.get('sensor_id')
    header_row=file.get('header_row')
    first_line_num = file.get('first_line_num', 0)
    time_format_args_library = file.get('time_format_args_library')
    time_zone = file.get('time_zone')
    to_utc = file.get('to_utc')
    time_columns = file.get('time_columns')        
    parse_time_columns = file.get('parse_time_columns')
    parameters = file.get('parameters')
    logger_debug.debug(file)
    data = cr.read_table_data(infile_path=path, header_row=header_row, first_line_num=first_line_num, parse_time_columns=parse_time_columns, time_zone=time_zone, time_format_args_library=time_format_args_library, time_parsed_column="timestamp", time_columns=time_columns, to_utc=to_utc)

    num_of_new_rows = len(data)

    for param, param_info in parameters.items():
        parameter_id = param_info.get('parameter_id')
        unit = param_info.get('unit')
        sensor_id = param_info.get('sensor_id')
        value_type = param_info.get('value_type')
        vertical_position = float(param_info.get('vertical_position'))
        param_data = cr.extract_columns_data(data, "timestamp", param)
        param_formatted_data = []
        for row in param_data:
            ts = row.get('timestamp')
            month_first_day = datetime.datetime(ts.year, ts.month, 1).strftime("%Y-%m-%d")
            profile_ts = datetime.datetime(ts.year, ts.month, ts.day, ts.hour, ts.minute, ts.second)
            min_value, avg_value, max_value = None, None, None
            if value_type == 'min_value':
                min_value = float(row.get(param))
            elif value_type == 'avg_value':
                avg_value = float(row.get(param))
            elif value_type == 'max_value':
                max_value = float(row.get(param))
            param_formatted_data.append((sensor_id, parameter_id, 0, month_first_day, int(profile_ts.timestamp()) * 1e3, depth, min_value, avg_value, max_value, unit))

        insert_to_five_min_profile_measurements_by_sensor.delay(param_formatted_data)
 
    return num_of_new_rows

def process_one_min_profile_measurements_by_sensor(station, file):
    if file.get('source') == 'profiles':
        num_of_new_rows = process_one_min_profile_measurements_by_sensor_profile_source(station, file)
    elif file.get('source') == 'parameters':
        num_of_new_rows = process_one_min_parameters_to_profile_measurements_by_sensor(station, file)
    else:
        raise TypeError("source must be either profiles or parameters, got {}".format(file.get('source')))
    
    return num_of_new_rows

def process_one_min_profile_measurements_by_sensor_profile_source(station, file):
    path=file.get('path')
    #sensor_id = file.get('sensor_id')
    header_row=file.get('header_row')
    first_line_num = file.get('first_line_num', 0)
    time_format_args_library = file.get('time_format_args_library')
    time_zone = file.get('time_zone')
    to_utc = file.get('to_utc')
    vertical_position_column = file.get('vertical_position_column')
    vertical_position_correction_factor = file.get('vertical_position_correction_factor')
    time_columns = file.get('time_columns')        
    parse_time_columns = file.get('parse_time_columns')
    parameters = file.get('parameters')
    logger_debug.debug(file)
    data = cr.read_table_data(infile_path=path, header_row=header_row, first_line_num=first_line_num, parse_time_columns=parse_time_columns, time_zone=time_zone, time_format_args_library=time_format_args_library, time_parsed_column="timestamp", time_columns=time_columns, to_utc=to_utc)

    num_of_new_rows = len(data)

    for param, param_info in parameters.items():
        parameter_id = param_info.get('parameter_id')
        unit = param_info.get('unit')
        sensor_id = param_info.get('sensor_id')
        value_type = param_info.get('value_type')
        param_data = cr.extract_columns_data(data, "timestamp", vertical_position_column, param)
        param_formatted_data = []
        for row in param_data:
            ts = row.get('timestamp')
            year, week_number, weekday = ts.isocalendar()
            week_first_day = (datetime.strptime('{} {} 1'.format(year, week_number), '%Y %W %w')).strftime("%Y-%m-%d")
            profile_ts = datetime.datetime(ts.year, ts.month, ts.day, ts.hour, ts.minute, ts.second)
            vertical_position = float(row.get(vertical_position_column))
            if vertical_position_correction_factor is not None:
                vertical_position = utils.round_of_rating(vertical_position, vertical_position_correction_factor)
            min_value, avg_value, max_value = None, None, None
            if value_type == 'min_value':
                min_value = float(row.get(param))
            elif value_type == 'avg_value':
                avg_value = float(row.get(param))
            elif value_type == 'max_value':
                max_value = float(row.get(param))
            param_formatted_data.append((sensor_id, parameter_id, 0, week_first_day, int(hour.timestamp()) * 1e3, vertical_position, min_value, avg_value, max_value, unit))

        insert_to_one_min_profile_measurements_by_sensor.delay(param_formatted_data)
 
    return num_of_new_rows

def process_one_min_parameters_to_profile_measurements_by_sensor(station, file):
    path=file.get('path')
    #sensor_id = file.get('sensor_id')
    header_row=file.get('header_row')
    first_line_num = file.get('first_line_num', 0)
    time_format_args_library = file.get('time_format_args_library')
    time_zone = file.get('time_zone')
    time_columns = file.get('time_columns')
    to_utc = file.get('to_utc')
    parse_time_columns = file.get('parse_time_columns')
    parameters = file.get('parameters')
    logger_debug.debug(file)
    data = cr.read_table_data(infile_path=path, header_row=header_row, first_line_num=first_line_num, parse_time_columns=parse_time_columns, time_zone=time_zone, time_format_args_library=time_format_args_library, time_parsed_column="timestamp", time_columns=time_columns, to_utc=to_utc)

    num_of_new_rows = len(data)

    for param, param_info in parameters.items():
        parameter_id = param_info.get('parameter_id')
        unit = param_info.get('unit')
        sensor_id = param_info.get('sensor_id')
        value_type = param_info.get('value_type')
        vertical_position = float(param_info.get('vertical_position'))
        param_data = cr.extract_columns_data(data, "timestamp", param)
        param_formatted_data = []
        for row in param_data:
            ts = row.get('timestamp')
            year, week_number, weekday = ts.isocalendar()
            week_first_day = (datetime.strptime('{} {} 1'.format(year, week_number), '%Y %W %w')).strftime("%Y-%m-%d")
            profile_ts = datetime.datetime(ts.year, ts.month, ts.day, ts.hour, ts.minute, ts.second)
            min_value, avg_value, max_value = None, None, None
            if value_type == 'min_value':
                min_value = float(row.get(param))
            elif value_type == 'avg_value':
                avg_value = float(row.get(param))
            elif value_type == 'max_value':
                max_value = float(row.get(param))
            param_formatted_data.append((sensor_id, parameter_id, 0, week_first_day, int(profile_ts.timestamp()) * 1e3, vertical_position, min_value, avg_value, max_value, unit))

        insert_to_one_min_profile_measurements_by_sensor.delay(param_formatted_data)
 
    return num_of_new_rows

def process_one_sec_profile_measurements_by_sensor(station, file):
    if file.get('source') == 'profiles':
        num_of_new_rows = process_one_sec_profile_measurements_by_sensor_profile_source(station, file)
    elif file.get('source') == 'parameters':
        num_of_new_rows = process_one_sec_parameters_to_profile_measurements_by_sensor(station, file)
    else:
        raise TypeError("source must be either profiles or parameters, got {}".format(file.get('source')))
    
    return num_of_new_rows

def process_one_sec_profile_measurements_by_sensor_profile_source(station, file):
    path=file.get('path')
    #sensor_id = file.get('sensor_id')
    header_row=file.get('header_row')
    first_line_num = file.get('first_line_num', 0)
    time_format_args_library = file.get('time_format_args_library')
    time_zone = file.get('time_zone')
    to_utc = file.get('to_utc')
    vertical_position_column = file.get('vertical_position_column')
    vertical_position_correction_factor = file.get('vertical_position_correction_factor')
    time_columns = file.get('time_columns')        
    parse_time_columns = file.get('parse_time_columns')
    parameters = file.get('parameters')
    logger_debug.debug(file)
    data = cr.read_table_data(infile_path=path, header_row=header_row, first_line_num=first_line_num, parse_time_columns=parse_time_columns, time_zone=time_zone, time_format_args_library=time_format_args_library, time_parsed_column="timestamp", time_columns=time_columns, to_utc=to_utc)

    num_of_new_rows = len(data)

    for param, param_info in parameters.items():
        parameter_id = param_info.get('parameter_id')
        unit = param_info.get('unit')
        sensor_id = param_info.get('sensor_id')
        value_type = param_info.get('value_type')
        param_data = cr.extract_columns_data(data, "timestamp", vertical_position_column, param)
        param_formatted_data = []
        for row in param_data:
            ts = row.get('timestamp')
            date_dt = datetime.datetime(ts.year, ts.month, ts.day).strftime("%Y-%m-%d")
            profile_ts = datetime.datetime(ts.year, ts.month, ts.day, ts.hour, ts.minute, ts.second)
            vertical_position = float(row.get(vertical_position_column))
            if vertical_position_correction_factor is not None:
                vertical_position = utils.round_of_rating(vertical_position, vertical_position_correction_factor)
            min_value, avg_value, max_value = None, None, None
            if value_type == 'min_value':
                min_value = float(row.get(param))
            elif value_type == 'avg_value':
                avg_value = float(row.get(param))
            elif value_type == 'max_value':
                max_value = float(row.get(param))
            param_formatted_data.append((sensor_id, parameter_id, 0, date_dt, int(hour.timestamp()) * 1e3, vertical_position, min_value, avg_value, max_value, unit))

        insert_to_one_sec_profile_measurements_by_sensor.delay(param_formatted_data)
 
    return num_of_new_rows

def process_one_sec_parameters_to_profile_measurements_by_sensor(station, file):
    path=file.get('path')
    #sensor_id = file.get('sensor_id')
    header_row=file.get('header_row')
    first_line_num = file.get('first_line_num', 0)
    time_format_args_library = file.get('time_format_args_library')
    time_zone = file.get('time_zone')
    time_columns = file.get('time_columns')
    to_utc = file.get('to_utc')
    parse_time_columns = file.get('parse_time_columns')
    parameters = file.get('parameters')
    logger_debug.debug(file)
    data = cr.read_table_data(infile_path=path, header_row=header_row, first_line_num=first_line_num, parse_time_columns=parse_time_columns, time_zone=time_zone, time_format_args_library=time_format_args_library, time_parsed_column="timestamp", time_columns=time_columns, to_utc=to_utc)

    num_of_new_rows = len(data)

    for param, param_info in parameters.items():
        parameter_id = param_info.get('parameter_id')
        unit = param_info.get('unit')
        sensor_id = param_info.get('sensor_id')
        value_type = param_info.get('value_type')
        vertical_position = float(param_info.get('vertical_position'))
        param_data = cr.extract_columns_data(data, "timestamp", param)
        param_formatted_data = []
        for row in param_data:
            ts = row.get('timestamp')
            date_dt = datetime.datetime(ts.year, ts.month, ts.day).strftime("%Y-%m-%d")
            profile_ts = datetime.datetime(ts.year, ts.month, ts.day, ts.hour, ts.minute, ts.second)
            min_value, avg_value, max_value = None, None, None
            if value_type == 'min_value':
                min_value = float(row.get(param))
            elif value_type == 'avg_value':
                avg_value = float(row.get(param))
            elif value_type == 'max_value':
                max_value = float(row.get(param))
            param_formatted_data.append((sensor_id, parameter_id, 0, date_dt, int(profile_ts.timestamp()) * 1e3, vertical_position, min_value, avg_value, max_value, unit))

        insert_to_one_sec_profile_measurements_by_sensor.delay(param_formatted_data)
 
    return num_of_new_rows

def process_daily_single_measurements_by_sensor(station, file):
    path=file.get('path')
    #sensor_id = file.get('sensor_id')
    header_row=file.get('header_row')
    first_line_num = file.get('first_line_num', 0)
    time_format_args_library = file.get('time_format_args_library')
    time_zone = file.get('time_zone')
    to_utc = file.get('to_utc')
    time_columns = file.get('time_columns')        
    parse_time_columns = file.get('parse_time_columns')
    parameters = file.get('parameters')

    data = cr.read_table_data(infile_path=path, header_row=header_row, first_line_num=first_line_num, parse_time_columns=parse_time_columns, time_zone=time_zone, time_format_args_library=time_format_args_library, time_parsed_column="timestamp", time_columns=time_columns, to_utc=to_utc)

    num_of_new_rows = len(data)

    for param, param_info in parameters.items():
        parameter_id = param_info.get('parameter_id')
        unit = param_info.get('unit')
        sensor_id = param_info.get('sensor_id')
        value_type = param_info.get('value_type')
        param_data = cr.extract_columns_data(data, "timestamp", param)
        param_formatted_data = []
        for row in param_data:
            ts = row.get('timestamp')
            day = datetime.datetime(ts.year, ts.month, ts.day)
            year = int(day.strftime("%Y"))
            min_value, avg_value, max_value = None, None, None
            if value_type == 'min_value':
                min_value = float(row.get(param))
            elif value_type == 'avg_value':
                avg_value = float(row.get(param))
            elif value_type == 'max_value':
                max_value = float(row.get(param))
            param_formatted_data.append((sensor_id, parameter_id, 0, year, int(day.timestamp()) * 1e3, min_value, avg_value, max_value, unit))

        insert_to_daily_single_measurements_by_sensor.delay(param_formatted_data)
 
    return num_of_new_rows

def process_hourly_single_measurements_by_sensor(station, file):
    path=file.get('path')
    #sensor_id = file.get('sensor_id')
    header_row=file.get('header_row')
    first_line_num = file.get('first_line_num', 0)
    time_format_args_library = file.get('time_format_args_library')
    time_zone = file.get('time_zone')
    to_utc = file.get('to_utc')
    time_columns = file.get('time_columns')        
    parse_time_columns = file.get('parse_time_columns')
    parameters = file.get('parameters')

    data = cr.read_table_data(infile_path=path, header_row=header_row, first_line_num=first_line_num, parse_time_columns=parse_time_columns, time_zone=time_zone, time_format_args_library=time_format_args_library, time_parsed_column="timestamp", time_columns=time_columns, to_utc=to_utc)

    num_of_new_rows = len(data)

    for param, param_info in parameters.items():
        parameter_id = param_info.get('parameter_id')
        unit = param_info.get('unit')
        sensor_id = param_info.get('sensor_id')
        value_type = param_info.get('value_type')
        param_data = cr.extract_columns_data(data, "timestamp", param)
        param_formatted_data = []
        for row in param_data:
            ts = row.get('timestamp')
            day = datetime.datetime(ts.year, ts.month, ts.day)
            year = int(day.strftime("%Y"))
            min_value, avg_value, max_value = None, None, None
            if value_type == 'min_value':
                min_value = float(row.get(param))
            elif value_type == 'avg_value':
                avg_value = float(row.get(param))
            elif value_type == 'max_value':
                max_value = float(row.get(param))
            param_formatted_data.append((sensor_id, parameter_id, 0, year, int(ts.timestamp()) * 1e3, min_value, avg_value, max_value, unit))

        insert_to_hourly_single_measurements_by_sensor.delay(param_formatted_data)
 
    return num_of_new_rows
    
def process_twenty_min_single_measurements_by_sensor(station, file):
    path=file.get('path')
    #sensor_id = file.get('sensor_id')
    header_row=file.get('header_row')
    first_line_num = file.get('first_line_num', 0)
    time_format_args_library = file.get('time_format_args_library')
    time_zone = file.get('time_zone')
    to_utc = file.get('to_utc')
    time_columns = file.get('time_columns')        
    parse_time_columns = file.get('parse_time_columns')
    parameters = file.get('parameters')

    data = cr.read_table_data(infile_path=path, header_row=header_row, first_line_num=first_line_num, parse_time_columns=parse_time_columns, time_zone=time_zone, time_format_args_library=time_format_args_library, time_parsed_column="timestamp", time_columns=time_columns, to_utc=to_utc)

    num_of_new_rows = len(data)

    for param, param_info in parameters.items():
        parameter_id = param_info.get('parameter_id')
        unit = param_info.get('unit')
        sensor_id = param_info.get('sensor_id')
        value_type = param_info.get('value_type')
        param_data = cr.extract_columns_data(data, "timestamp", param)
        param_formatted_data = []
        for row in param_data:
            ts = row.get('timestamp')
            day = datetime.datetime(ts.year, ts.month, ts.day)
            year = int(day.strftime("%Y"))
            min_value, avg_value, max_value = None, None, None
            if value_type == 'min_value':
                min_value = float(row.get(param))
            elif value_type == 'avg_value':
                avg_value = float(row.get(param))
            elif value_type == 'max_value':
                max_value = float(row.get(param))
            param_formatted_data.append((sensor_id, parameter_id, 0, year, int(ts.timestamp()) * 1e3, min_value, avg_value, max_value, unit))

        insert_to_thirty_min_single_measurements_by_sensor.delay(param_formatted_data)
 
    return num_of_new_rows

def process_twenty_min_single_measurements_by_sensor(station, file):
    path=file.get('path')
    #sensor_id = file.get('sensor_id')
    header_row=file.get('header_row')
    first_line_num = file.get('first_line_num', 0)
    time_format_args_library = file.get('time_format_args_library')
    time_zone = file.get('time_zone')
    to_utc = file.get('to_utc')
    time_columns = file.get('time_columns')        
    parse_time_columns = file.get('parse_time_columns')
    parameters = file.get('parameters')

    data = cr.read_table_data(infile_path=path, header_row=header_row, first_line_num=first_line_num, parse_time_columns=parse_time_columns, time_zone=time_zone, time_format_args_library=time_format_args_library, time_parsed_column="timestamp", time_columns=time_columns, to_utc=to_utc)

    num_of_new_rows = len(data)

    for param, param_info in parameters.items():
        parameter_id = param_info.get('parameter_id')
        unit = param_info.get('unit')
        sensor_id = param_info.get('sensor_id')
        value_type = param_info.get('value_type')
        param_data = cr.extract_columns_data(data, "timestamp", param)
        param_formatted_data = []
        for row in param_data:
            ts = row.get('timestamp')
            day = datetime.datetime(ts.year, ts.month, ts.day)
            year = int(day.strftime("%Y"))
            min_value, avg_value, max_value = None, None, None
            if value_type == 'min_value':
                min_value = float(row.get(param))
            elif value_type == 'avg_value':
                avg_value = float(row.get(param))
            elif value_type == 'max_value':
                max_value = float(row.get(param))
            param_formatted_data.append((sensor_id, parameter_id, 0, year, int(ts.timestamp()) * 1e3, min_value, avg_value, max_value, unit))

        insert_to_twenty_min_single_measurements_by_sensor.delay(param_formatted_data)
 
    return num_of_new_rows

def process_fifteen_min_single_measurements_by_sensor(station, file):
    path=file.get('path')
    #sensor_id = file.get('sensor_id')
    header_row=file.get('header_row')
    first_line_num = file.get('first_line_num', 0)
    time_format_args_library = file.get('time_format_args_library')
    time_zone = file.get('time_zone')
    to_utc = file.get('to_utc')
    time_columns = file.get('time_columns')        
    parse_time_columns = file.get('parse_time_columns')
    parameters = file.get('parameters')

    data = cr.read_table_data(infile_path=path, header_row=header_row, first_line_num=first_line_num, parse_time_columns=parse_time_columns, time_zone=time_zone, time_format_args_library=time_format_args_library, time_parsed_column="timestamp", time_columns=time_columns, to_utc=to_utc)

    num_of_new_rows = len(data)

    for param, param_info in parameters.items():
        parameter_id = param_info.get('parameter_id')
        unit = param_info.get('unit')
        sensor_id = param_info.get('sensor_id')
        value_type = param_info.get('value_type')
        param_data = cr.extract_columns_data(data, "timestamp", param)
        param_formatted_data = []
        for row in param_data:
            ts = row.get('timestamp')
            month_first_day = datetime.datetime(ts.year, ts.month, 1).strftime("%Y-%m-%d")
            min_value, avg_value, max_value = None, None, None
            if value_type == 'min_value':
                min_value = float(row.get(param))
            elif value_type == 'avg_value':
                avg_value = float(row.get(param))
            elif value_type == 'max_value':
                max_value = float(row.get(param))
            param_formatted_data.append((sensor_id, parameter_id, 0, month_first_day, int(ts.timestamp()) * 1e3, min_value, avg_value, max_value, unit))

        insert_to_fifteen_min_single_measurements_by_sensor.delay(param_formatted_data)
 
    return num_of_new_rows

def process_ten_min_single_measurements_by_sensor(station, file):
    path=file.get('path')
    #sensor_id = file.get('sensor_id')
    header_row=file.get('header_row')
    first_line_num = file.get('first_line_num', 0)
    time_format_args_library = file.get('time_format_args_library')
    time_zone = file.get('time_zone')
    to_utc = file.get('to_utc')
    time_columns = file.get('time_columns')        
    parse_time_columns = file.get('parse_time_columns')
    parameters = file.get('parameters')

    data = cr.read_table_data(infile_path=path, header_row=header_row, first_line_num=first_line_num, parse_time_columns=parse_time_columns, time_zone=time_zone, time_format_args_library=time_format_args_library, time_parsed_column="timestamp", time_columns=time_columns, to_utc=to_utc)

    num_of_new_rows = len(data)

    for param, param_info in parameters.items():
        parameter_id = param_info.get('parameter_id')
        unit = param_info.get('unit')
        sensor_id = param_info.get('sensor_id')
        value_type = param_info.get('value_type')
        param_data = cr.extract_columns_data(data, "timestamp", param)
        param_formatted_data = []
        for row in param_data:
            ts = row.get('timestamp')
            month_first_day = datetime.datetime(ts.year, ts.month, 1).strftime("%Y-%m-%d")
            min_value, avg_value, max_value = None, None, None
            if value_type == 'min_value':
                min_value = float(row.get(param))
            elif value_type == 'avg_value':
                avg_value = float(row.get(param))
            elif value_type == 'max_value':
                max_value = float(row.get(param))
            param_formatted_data.append((sensor_id, parameter_id, 0, month_first_day, int(ts.timestamp()) * 1e3, min_value, avg_value, max_value, unit))

        insert_to_ten_min_single_measurements_by_sensor.delay(param_formatted_data)
 
    return num_of_new_rows

def process_five_min_single_measurements_by_sensor(station, file):
    path=file.get('path')
    #sensor_id = file.get('sensor_id')
    header_row=file.get('header_row')
    first_line_num = file.get('first_line_num', 0)
    time_format_args_library = file.get('time_format_args_library')
    time_zone = file.get('time_zone')
    to_utc = file.get('to_utc')
    time_columns = file.get('time_columns')        
    parse_time_columns = file.get('parse_time_columns')
    parameters = file.get('parameters')

    data = cr.read_table_data(infile_path=path, header_row=header_row, first_line_num=first_line_num, parse_time_columns=parse_time_columns, time_zone=time_zone, time_format_args_library=time_format_args_library, time_parsed_column="timestamp", time_columns=time_columns, to_utc=to_utc)

    num_of_new_rows = len(data)

    for param, param_info in parameters.items():
        parameter_id = param_info.get('parameter_id')
        unit = param_info.get('unit')
        sensor_id = param_info.get('sensor_id')
        value_type = param_info.get('value_type')
        param_data = cr.extract_columns_data(data, "timestamp", param)
        param_formatted_data = []
        for row in param_data:
            ts = row.get('timestamp')
            month_first_day = datetime.datetime(ts.year, ts.month, 1).strftime("%Y-%m-%d")
            min_value, avg_value, max_value = None, None, None
            if value_type == 'min_value':
                min_value = float(row.get(param))
            elif value_type == 'avg_value':
                avg_value = float(row.get(param))
            elif value_type == 'max_value':
                max_value = float(row.get(param))
            param_formatted_data.append((sensor_id, parameter_id, 0, month_first_day, int(ts.timestamp()) * 1e3, min_value, avg_value, max_value, unit))

        insert_to_five_min_single_measurements_by_sensor.delay(param_formatted_data)
 
    return num_of_new_rows

def process_one_min_single_measurements_by_sensor(station, file):
    path=file.get('path')
    #sensor_id = file.get('sensor_id')
    header_row=file.get('header_row')
    first_line_num = file.get('first_line_num', 0)
    time_format_args_library = file.get('time_format_args_library')
    time_zone = file.get('time_zone')
    to_utc = file.get('to_utc')
    time_columns = file.get('time_columns')        
    parse_time_columns = file.get('parse_time_columns')
    parameters = file.get('parameters')

    data = cr.read_table_data(infile_path=path, header_row=header_row, first_line_num=first_line_num, parse_time_columns=parse_time_columns, time_zone=time_zone, time_format_args_library=time_format_args_library, time_parsed_column="timestamp", time_columns=time_columns, to_utc=to_utc)

    num_of_new_rows = len(data)

    for param, param_info in parameters.items():
        parameter_id = param_info.get('parameter_id')
        unit = param_info.get('unit')
        sensor_id = param_info.get('sensor_id')
        value_type = param_info.get('value_type')
        param_data = cr.extract_columns_data(data, "timestamp", param)
        param_formatted_data = []
        for row in param_data:
            ts = row.get('timestamp')
            year, week_number, weekday = ts.isocalendar()
            week_first_day = (datetime.strptime('{} {} 1'.format(year, week_number), '%Y %W %w')).strftime("%Y-%m-%d")
            min_value, avg_value, max_value = None, None, None
            if value_type == 'min_value':
                min_value = float(row.get(param))
            elif value_type == 'avg_value':
                avg_value = float(row.get(param))
            elif value_type == 'max_value':
                max_value = float(row.get(param))
            param_formatted_data.append((sensor_id, parameter_id, 0, week_first_day, int(ts.timestamp()) * 1e3, min_value, avg_value, max_value, unit))

        insert_to_one_min_single_measurements_by_sensor.delay(param_formatted_data)
 
    return num_of_new_rows
    
def process_one_sec_single_measurements_by_sensor(station, file):
    path=file.get('path')
    #sensor_id = file.get('sensor_id')
    header_row=file.get('header_row')
    first_line_num = file.get('first_line_num', 0)
    time_format_args_library = file.get('time_format_args_library')
    time_zone = file.get('time_zone')
    to_utc = file.get('to_utc')
    time_columns = file.get('time_columns')        
    parse_time_columns = file.get('parse_time_columns')
    parameters = file.get('parameters')

    data = cr.read_table_data(infile_path=path, header_row=header_row, first_line_num=first_line_num, parse_time_columns=parse_time_columns, time_zone=time_zone, time_format_args_library=time_format_args_library, time_parsed_column="timestamp", time_columns=time_columns, to_utc=to_utc)

    num_of_new_rows = len(data)

    for param, param_info in parameters.items():
        parameter_id = param_info.get('parameter_id')
        unit = param_info.get('unit')
        sensor_id = param_info.get('sensor_id')
        value_type = param_info.get('value_type')
        param_data = cr.extract_columns_data(data, "timestamp", param)
        param_formatted_data = []
        for row in param_data:
            ts = row.get('timestamp')
            date_dt = datetime.datetime(ts.year, ts.month, ts.day).strftime("%Y-%m-%d")
            min_value, avg_value, max_value = None, None, None
            if value_type == 'min_value':
                min_value = float(row.get(param))
            elif value_type == 'avg_value':
                avg_value = float(row.get(param))
            elif value_type == 'max_value':
                max_value = float(row.get(param))
            param_formatted_data.append((sensor_id, parameter_id, 0, date_dt, int(ts.timestamp()) * 1e3, min_value, avg_value, max_value, unit))

        insert_to_one_sec_single_measurements_by_sensor.delay(param_formatted_data)
 
    return num_of_new_rows

def run_update(config_file, args):
    file = config_file[args.sensor][args.file]
    if (file.get('table') == 'daily_single_measurements_by_sensor'):
        num_of_new_rows = process_daily_single_measurements_by_sensor(args.sensor, file)
    elif (file.get('table') == 'hourly_single_measurements_by_sensor'):
        num_of_new_rows = process_hourly_single_measurements_by_sensor(args.sensor, file)
    elif (file.get('table') == 'thirty_min_single_measurements_by_sensor'):
        num_of_new_rows = process_thirty_min_single_measurements_by_sensor(args.sensor, file)
    elif (file.get('table') == 'twenty_min_single_measurements_by_sensor'):
        num_of_new_rows = process_twenty_min_single_measurements_by_sensor(args.sensor, file)
    elif (file.get('table') == 'fifteen_min_single_measurements_by_sensor'):
        num_of_new_rows = process_fifteen_min_single_measurements_by_sensor(args.sensor, file)
    elif (file.get('table') == 'ten_min_single_measurements_by_sensor'):
        num_of_new_rows = process_ten_min_single_measurements_by_sensor(args.sensor, file)
    elif (file.get('table') == 'five_min_single_measurements_by_sensor'):
        num_of_new_rows = process_five_min_single_measurements_by_sensor(args.sensor, file)    
    elif (file.get('table') == 'one_min_single_measurements_by_sensor'):
        num_of_new_rows = process_one_min_single_measurements_by_sensor(args.sensor, file)    
    elif (file.get('table') == 'one_sec_single_measurements_by_sensor'):
        num_of_new_rows = process_one_sec_single_measurements_by_sensor(args.sensor, file)   
    elif (file.get('table') == 'daily_profile_measurements_by_sensor'):
        num_of_new_rows = process_daily_profile_measurements_by_sensor(args.sensor, file)
    elif (file.get('table') == 'hourly_profile_measurements_by_sensor'):
        num_of_new_rows = process_hourly_profile_measurements_by_sensor(args.sensor, file)
    elif (file.get('table') == 'thirty_min_profile_measurements_by_sensor'):
        num_of_new_rows = process_thirty_min_profile_measurements_by_sensor(args.sensor, file)
    elif (file.get('table') == 'twenty_min_profile_measurements_by_sensor'):
        num_of_new_rows = process_twenty_min_profile_measurements_by_sensor(args.sensor, file)
    elif (file.get('table') == 'fifteen_profile_measurements_by_sensor'):
        num_of_new_rows = process_fifteen_min_profile_measurements_by_sensor(args.sensor, file)
    elif (file.get('table') == 'ten_profile_measurements_by_sensor'):
        num_of_new_rows = process_ten_profile_measurements_by_sensor(args.sensor, file)
    elif (file.get('table') == 'five_min_profile_measurements_by_sensor'):
        num_of_new_rows = process_five_min_profile_measurements_by_sensor(args.sensor, file)
    elif (file.get('table') == 'one_min_profile_measurements_by_sensor'):
        num_of_new_rows = process_one_min_profile_measurements_by_sensor(args.sensor, file)
    elif (file.get('table') == 'one_sec_profile_measurements_by_sensor'):
        num_of_new_rows = process_one_sec_profile_measurements_by_sensor(args.sensor, file)
        
    if args.track:
        if num_of_new_rows > 0:
            first_line_num = file.get('first_line_num', 0)
            new_line_num = first_line_num + num_of_new_rows
            logger_info.info("Updated up to line number {num}".format(num=new_line_num))
            config_file[args.sensor][args.file]['first_line_num'] = new_line_num

    logger_info.info("Done processing table {table}".format(table=file.get('table')))

    if args.track:
        logger_info.info("Updating config file.")
        utils.save_config(APP_CONFIG_PATH, config_file)

def main():
    """Parses and validates arguments from the command line. """
    parser = argparse.ArgumentParser(
        prog='CassandraFormatter',
        description='Program for formatting and storing logger data to Cassandra database.'
    )
    parser.add_argument('-s', '--sensor', action='store', dest='station',
                        help='Station to process.')
    parser.add_argument('-f', '--file', action='store', dest='file',
                        help='File to process.')
    parser.add_argument(
        '-t', '--track',
        help='Track file line number.',
        dest='track',
        action='store_true',
        default=False
    )

    args = parser.parse_args()

    if not args.sensor or not args.file:
        parser.error("--sensor and --file is required.")

    app_cfg = utils.load_config(APP_CONFIG_PATH)

    run_update(app_cfg, args)
    

if __name__=='__main__':
    main()

