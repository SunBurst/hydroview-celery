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

from tasks import insert_to_daily_single_parameter_measurements_by_station
from tasks import insert_to_single_parameter_measurements_by_station
from tasks import insert_to_hourly_profile_measurements_by_station_time
from tasks import insert_to_daily_profile_measurements_by_station_time
from tasks import insert_to_hourly_single_parameter_measurements_by_station
from tasks import insert_to_profile_measurements_by_station_time
from tasks import insert_to_daily_parameter_group_measurements_by_station
from tasks import insert_to_hourly_parameter_group_measurements_by_station
from tasks import insert_to_parameter_group_measurements_by_station

import utils

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
APP_CONFIG_PATH = os.path.join(BASE_DIR, 'cfg/appconfig.yaml')
LOGGING_CONFIG_PATH = os.path.join(BASE_DIR, 'cfg/logging.yaml')

logging_conf = utils.load_config(LOGGING_CONFIG_PATH)
logging.config.dictConfig(logging_conf)
logger_info = logging.getLogger('cassandra_storage_info')
logger_debug = logging.getLogger('cassandra_storage_debug')


def process_daily_profile_measurements_by_station(station, file):
    path=file.get('path')
    header_row=file.get('header_row')
    first_line_num = file.get('first_line_num', 0)
    time_format_args_library = file.get('time_format_args_library')
    time_zone = file.get('time_zone')
    to_utc = file.get('to_utc')
    depth_column = file.get('depth_column')
    depth_correction_factor = file.get('depth_correction_factor')
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
        sensor_name = param_info.get('sensor_name')
        param_data = cr.extract_columns_data(data, "timestamp", depth_column, param)
        param_formatted_data = []
        for row in param_data:
            ts = row.get('timestamp')
            day = datetime.datetime(ts.year, ts.month, ts.day)
            year = int(day.strftime("%Y"))
            depth = float(row.get(depth_column))
            if depth_correction_factor is not None:
                depth = utils.round_of_rating(depth, depth_correction_factor)
            param_formatted_data.append((station, parameter_id, 0, year, int(day.timestamp()) * 1e3, depth, sensor_name, sensor_id, float(row.get(param)), unit))

        insert_to_daily_profile_measurements_by_station_time.delay(param_formatted_data)
 
    return num_of_new_rows

def process_hourly_profile_measurements_by_station(station, file):
    path=file.get('path')
    header_row=file.get('header_row')
    first_line_num = file.get('first_line_num', 0)
    time_format_args_library = file.get('time_format_args_library')
    time_zone = file.get('time_zone')
    to_utc = file.get('to_utc')
    depth_column = file.get('depth_column')
    depth_correction_factor = file.get('depth_correction_factor')
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
        sensor_name = param_info.get('sensor_name')
        param_data = cr.extract_columns_data(data, "timestamp", depth_column, param)
        param_formatted_data = []
        for row in param_data:
            ts = row.get('timestamp')
            day = datetime.datetime(ts.year, ts.month, ts.day)
            year = int(day.strftime("%Y"))
            hour = datetime.datetime(ts.year, ts.month, ts.day, ts.hour, 0, 0)
            depth = float(row.get(depth_column))
            if depth_correction_factor is not None:
                depth = utils.round_of_rating(depth, depth_correction_factor)
            param_formatted_data.append((station, parameter_id, 0, year, int(hour.timestamp()) * 1e3, depth, sensor_name, sensor_id, float(row.get(param)), unit))

        insert_to_hourly_profile_measurements_by_station_time.delay(param_formatted_data)
 
    return num_of_new_rows
        
def process_profile_measurements_by_station(station, file):
    path=file.get('path')
    header_row=file.get('header_row')
    first_line_num = file.get('first_line_num', 0)
    time_format_args_library = file.get('time_format_args_library')
    time_zone = file.get('time_zone')
    to_utc = file.get('to_utc')
    depth_column = file.get('depth_column')
    depth_correction_factor = file.get('depth_correction_factor')
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
        sensor_name = param_info.get('sensor_name')
        param_data = cr.extract_columns_data(data, "timestamp", depth_column, param)
        param_formatted_data = []
        for row in param_data:
            ts = row.get('timestamp')
            month_first_day = datetime.datetime(ts.year, ts.month, 1).strftime("%Y-%m-%d")
            profile_ts = datetime.datetime(ts.year, ts.month, ts.day, ts.hour, ts.minute, ts.second)
            depth = float(row.get(depth_column))
            if depth_correction_factor is not None:
                depth = utils.round_of_rating(depth, depth_correction_factor)
            param_formatted_data.append((station, parameter_id, 0, month_first_day, int(profile_ts.timestamp()) * 1e3, depth, sensor_name, sensor_id, float(row.get(param)), unit))

        insert_to_profile_measurements_by_station_time.delay(param_formatted_data)
 
    return num_of_new_rows

def process_daily_single_parameter_measurements_by_station(station, file):
    path=file.get('path')
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
        sensor_name = param_info.get('sensor_name')
        param_data = cr.extract_columns_data(data, "timestamp", param)
        param_formatted_data = []
        for row in param_data:
            ts = row.get('timestamp')
            day = datetime.datetime(ts.year, ts.month, ts.day)
            year = int(day.strftime("%Y"))
            param_formatted_data.append((station, parameter_id, 0, year, int(day.timestamp()) * 1e3, sensor_name, sensor_id, float(row.get(param)), unit))

        insert_to_daily_single_parameter_measurements_by_station.delay(param_formatted_data)
 
    return num_of_new_rows

def process_single_parameter_measurements_by_station(station, file):
    path=file.get('path')
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
        sensor_name = param_info.get('sensor_name')
        param_data = cr.extract_columns_data(data, "timestamp", param)
        param_formatted_data = []
        for row in param_data:
            ts = row.get('timestamp')
            month_first_day = datetime.datetime(ts.year, ts.month, 1).strftime("%Y-%m-%d")
            param_formatted_data.append((station, parameter_id, 0, month_first_day, int(ts.timestamp()) * 1e3, sensor_name, sensor_id, float(row.get(param)), unit))

        insert_to_single_parameter_measurements_by_station.delay(param_formatted_data)
 
    return num_of_new_rows

def process_hourly_single_parameter_measurements_by_station(station, file):
    path=file.get('path')
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
        sensor_name = param_info.get('sensor_name')
        param_data = cr.extract_columns_data(data, "timestamp", param)
        param_formatted_data = []
        for row in param_data:
            ts = row.get('timestamp')
            day = datetime.datetime(ts.year, ts.month, ts.day)
            year = int(day.strftime("%Y"))
            param_formatted_data.append((station, parameter_id, 0, year, int(ts.timestamp()) * 1e3, sensor_name, sensor_id, float(row.get(param)), unit))

        insert_to_hourly_single_parameter_measurements_by_station.delay(param_formatted_data)
 
    return num_of_new_rows

def process_daily_parameter_group_measurements_by_station(station, file):
    path = file.get('path')
    group_id = file.get('group_id')
    header_row = file.get('header_row')
    first_line_num = file.get('first_line_num', 0)
    time_format_args_library = file.get('time_format_args_library')
    time_zone = file.get('time_zone')
    to_utc = file.get('to_utc')
    time_columns = file.get('time_columns')        
    parse_time_columns = file.get('parse_time_columns')
    parameters = file.get('parameters')

    data = cr.read_table_data(infile_path=path, header_row=header_row, first_line_num=first_line_num, parse_time_columns=parse_time_columns, time_zone=time_zone, time_format_args_library=time_format_args_library, time_parsed_column="timestamp", time_columns=time_columns, to_utc=to_utc)

    num_of_new_rows = len(data)

    parameter_group_formatted_data = []
    
    for param, param_info in parameters.items():
        parameter_id = param_info.get('parameter_id')
        parameter_name = param_info.get('parameter_name')
        unit = param_info.get('unit')
        sensor_id = param_info.get('sensor_id')
        sensor_name = param_info.get('sensor_name')
        param_data = cr.extract_columns_data(data, "timestamp", param)
        
        for row in param_data:
            ts = row.get('timestamp')
            day = datetime.datetime(ts.year, ts.month, ts.day)
            year = int(day.strftime("%Y"))
            parameter_group_formatted_data.append((station, group_id, 0, year, int(day.timestamp()) * 1e3, parameter_name, sensor_name, parameter_id, sensor_id, float(row.get(param)), unit))
    
    insert_to_daily_parameter_group_measurements_by_station.delay(parameter_group_formatted_data)
 
    return num_of_new_rows
    
def process_hourly_parameter_group_measurements_by_station(station, file):
    path = file.get('path')
    group_id = file.get('group_id')
    header_row = file.get('header_row')
    first_line_num = file.get('first_line_num', 0)
    time_format_args_library = file.get('time_format_args_library')
    time_zone = file.get('time_zone')
    to_utc = file.get('to_utc')
    time_columns = file.get('time_columns')        
    parse_time_columns = file.get('parse_time_columns')
    parameters = file.get('parameters')

    data = cr.read_table_data(infile_path=path, header_row=header_row, first_line_num=first_line_num, parse_time_columns=parse_time_columns, time_zone=time_zone, time_format_args_library=time_format_args_library, time_parsed_column="timestamp", time_columns=time_columns, to_utc=to_utc)

    num_of_new_rows = len(data)

    parameter_group_formatted_data = []
    
    for param, param_info in parameters.items():
        parameter_id = param_info.get('parameter_id')
        parameter_name = param_info.get('parameter_name')
        unit = param_info.get('unit')
        sensor_id = param_info.get('sensor_id')
        sensor_name = param_info.get('sensor_name')
        param_data = cr.extract_columns_data(data, "timestamp", param)
        
        for row in param_data:
            ts = row.get('timestamp')
            day = datetime.datetime(ts.year, ts.month, ts.day)
            year = int(day.strftime("%Y"))
            parameter_group_formatted_data.append((station, group_id, 0, year, int(day.timestamp()) * 1e3, parameter_name, sensor_name, parameter_id, sensor_id, float(row.get(param)), unit))
    
    insert_to_hourly_parameter_group_measurements_by_station.delay(parameter_group_formatted_data)
 
    return num_of_new_rows
    
def process_parameter_group_measurements_by_station(station, file):
    path = file.get('path')
    group_id = file.get('group_id')
    header_row = file.get('header_row')
    first_line_num = file.get('first_line_num', 0)
    time_format_args_library = file.get('time_format_args_library')
    time_zone = file.get('time_zone')
    to_utc = file.get('to_utc')
    time_columns = file.get('time_columns')        
    parse_time_columns = file.get('parse_time_columns')
    parameters = file.get('parameters')

    data = cr.read_table_data(infile_path=path, header_row=header_row, first_line_num=first_line_num, parse_time_columns=parse_time_columns, time_zone=time_zone, time_format_args_library=time_format_args_library, time_parsed_column="timestamp", time_columns=time_columns, to_utc=to_utc)

    num_of_new_rows = len(data)

    parameter_group_formatted_data = []
    
    for param, param_info in parameters.items():
        parameter_id = param_info.get('parameter_id')
        parameter_name = param_info.get('parameter_name')
        unit = param_info.get('unit')
        sensor_id = param_info.get('sensor_id')
        sensor_name = param_info.get('sensor_name')
        param_data = cr.extract_columns_data(data, "timestamp", param)
        
        for row in param_data:
            ts = row.get('timestamp')
            month_first_day = datetime.datetime(ts.year, ts.month, 1).strftime("%Y-%m-%d")
            parameter_group_formatted_data.append((station, group_id, 0, month_first_day, int(ts.timestamp()) * 1e3, parameter_name, sensor_name, parameter_id, sensor_id, float(row.get(param)), unit))
    
    insert_to_parameter_group_measurements_by_station.delay(parameter_group_formatted_data)
 
    return num_of_new_rows

def run_update(config_file, args):
    file = config_file[args.station][args.file]
    if (file.get('table') == 'daily_single_parameter_measurements_by_station'):
        num_of_new_rows = process_daily_single_parameter_measurements_by_station(args.station, file)
    elif (file.get('table') == 'single_parameter_measurements_by_station'):
        num_of_new_rows = process_single_parameter_measurements_by_station(args.station, file)
    elif (file.get('table') == 'hourly_single_parameter_measurements_by_station'):
        num_of_new_rows = process_hourly_single_parameter_measurements_by_station(args.station, file)
    elif (file.get('table') == 'daily_profile_measurements_by_station'):
        num_of_new_rows = process_daily_profile_measurements_by_station(args.station, file)
    elif (file.get('table') == 'hourly_profile_measurements_by_station'):
        num_of_new_rows = process_hourly_profile_measurements_by_station(args.station, file)
    elif (file.get('table') == 'profile_measurements_by_station'):
        num_of_new_rows = process_profile_measurements_by_station(args.station, file)
    elif (file.get('table') == 'daily_parameter_group_measurements_by_station'):
        num_of_new_rows = process_daily_parameter_group_measurements_by_station(args.station, file)
    elif (file.get('table') == 'hourly_parameter_group_measurements_by_station'):
        num_of_new_rows = process_hourly_parameter_group_measurements_by_station(args.station, file)
    elif (file.get('table') == 'parameter_group_measurements_by_station'):
        num_of_new_rows = process_parameter_group_measurements_by_station(args.station, file)
        
    if args.track:
        if num_of_new_rows > 0:
            first_line_num = file.get('first_line_num', 0)
            new_line_num = first_line_num + num_of_new_rows
            logger_info.info("Updated up to line number {num}".format(num=new_line_num))
            config_file[args.station][args.file]['first_line_num'] = new_line_num

    logger_info.info("Done processing table {table}".format(table=file.get('table')))

    if args.track:
        logger_info.info("Updating config file.")
        utils.save_config(APP_CONFIG_PATH, config_file)

def main():
    """Parses and validates arguments from the command line. """
    parser = argparse.ArgumentParser(
        prog='LoggerFilesFormatter',
        description='Program for formatting and storing logger data to Cassandra database.'
    )
    parser.add_argument('-s', '--station', action='store', dest='station',
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

    if not args.station or not args.file:
        parser.error("--station and --file is required.")

    app_cfg = utils.load_config(APP_CONFIG_PATH)

    run_update(app_cfg, args)
    

if __name__=='__main__':
    main()

