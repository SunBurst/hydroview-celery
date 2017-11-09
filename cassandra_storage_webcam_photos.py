#!/usr/bin/env
# -*- coding: utf-8 -*-

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse
import binascii
import datetime
import logging.config
import uuid
import pytz

from tasks import insert_to_hourly_webcam_photos_by_station

import utils

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
#APP_CONFIG_PATH = os.path.join(BASE_DIR, 'cfg/cassandra_webcam_photos_inserter_config.yaml')
LOGGING_CONFIG_PATH = os.path.join(BASE_DIR, 'cfg/logging.yaml')

logging_conf = utils.load_config(LOGGING_CONFIG_PATH)
logging.config.dictConfig(logging_conf)
logger_info = logging.getLogger('cassandra_webcam_photos_storage_info')
logger_debug = logging.getLogger('cassandra_webcam_photos_storage_debug')


def process_webcam_photo_file(args):
    station_id = args.station
    photo_file = args.photo_file
    dt_modified = datetime.datetime.utcfromtimestamp(os.path.getmtime(photo_file))
    dt_date_modified = datetime.datetime(dt_modified.year, dt_modified.month, dt_modified.day).strftime("%Y-%m-%d")
    dt_hour_modified = datetime.datetime(dt_modified.year, dt_modified.month, dt_modified.day, dt_modified.hour)
    with open(photo_file, 'rb') as f:
        content = f.read()
        content_hex_binary = binascii.hexlify(content)
        content_hex_string = content_hex_binary.decode('ascii')
        row = (station_id, dt_date_modified, int(dt_hour_modified.replace(tzinfo=pytz.utc).timestamp()) * 1e3, content_hex_string,)
        insert_to_hourly_webcam_photos_by_station.delay([row])
    
def process_webcam_photo_directory(photo_directory):
    pass

def run_update(args):
    if args.photo_file:
        process_webcam_photo_file(args)
    elif args.photo_directory:
        process_webcam_photo_directory(args)

def main():
    """Parses and validates arguments from the command line. """
    parser = argparse.ArgumentParser(
        prog='HydroviewCeleryWebcamPhotosInserter',
        description='Program for storing webcam photos to Cassandra database.'
    )
    parser.add_argument('-s', '--station', action='store', dest='station',
                        help='Station to process.')
    parser.add_argument('-f', '--file', action='store', dest='photo_file',
                        help='File to process.')
    parser.add_argument('-d', '--directory', action='store', dest='photo_directory',
                        help='Directory to process.')

    args = parser.parse_args()

    if not args.station:
        parser.error("--station is required.")
    if not args.photo_file:
        if not args.photo_directory:
            parser.error("--file OR directory is required.")
    if not args.photo_directory:
        if not args.photo_file:
            parser.error("--file OR directory is required.")

    #app_cfg = utils.load_config(APP_CONFIG_PATH)

    run_update(args)

if __name__=='__main__':
    main()
