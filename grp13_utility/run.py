#!/usr/bin/env python3
import datetime
import logging
import os
import re
import shutil
import time
import traceback
import zipfile

import requests
import flywheel
import deid_file


log = logging.getLogger(__name__)


def create_gear_logger(log_level, log_name):
    log_format = '%(asctime)s.%(msecs)03d %(levelname)-8s [%(name)s]: %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'
    logging.Formatter.converter = time.gmtime
    logging.basicConfig(level=log_level, format=log_format, datefmt=date_format)
    log = logging.getLogger(log_name)
    return log


def ensure_filename_safety(filename):
    """
    A function for removing characters that are not alphanumeric, '.', '-', or '_' from an input string.
    :param filename: an input string
    :return: safe_filename, a string without characters that are not alphanumeric, '.', '-', or '_'
    """
    safe_filename = re.sub(r'[^A-Za-z0-9\-\_\.]+', '', filename)
    if filename != safe_filename:
        log.info(f'Renaming {filename} to {safe_filename}')

    return safe_filename


def append_to_destination_origin_list(gear_context):

    origin = gear_context.config.get('origin')
    if origin:
        new_origin_list = [origin]
        destination_id = gear_context.destination.get('id')
        if destination_id == 'aex':
            destination_id = '5dc9e6d7bd690a002adaa1f4'
        destination_obj = gear_context.client.get(destination_id)
        current_deid_origin_list = destination_obj.info.get('deid_origin')
        if isinstance(current_deid_origin_list, list):
            new_origin_list = current_deid_origin_list.extend(new_origin_list)
        gear_context.update_destination_metadata({'info': {'deid_origin': new_origin_list}})
        gear_context.write_metadata()
    return None


def write_deid_file_metadata(gear_context, input_key, output_file):
    original_metadata = gear_context.get_input(input_key).get('object')

    output_metadata = dict()
    if original_metadata.get('type'):
        output_metadata['type'] = original_metadata.get('type')
    if zipfile.is_zipfile(output_file):
        with zipfile.ZipFile(output_file, 'r') as zobj:
            zip_len = len([zip_item for zip_item in zobj.infolist() if not zip_item.filename.endswith(os.path.sep)])
        output_metadata['zip_member_count'] = zip_len
    if output_metadata:
        gear_context.update_file_metadata(os.path.basename(output_file), output_metadata)
        gear_context.write_metadata()


def main(gear_context):

    exit_status = 0
    input_file_dict = gear_context.get_input('input_file')
    dest_id = gear_context.destination.get('id')
    if dest_id == 'aex':
        dest_id = '5dc9e6d7bd690a002adaa1f4'
    output_filename = gear_context.config.get('output_filename')
    overwrite = gear_context.config.get('force_overwrite')
    fw = gear_context.client

    # Make the output_filename safe
    safe_filename = ensure_filename_safety(filename=output_filename)
    # Exit if no safe characters were provided
    if not safe_filename:
        error_msg = f'No safe characters in filename {output_filename}. Exiting...'
        log.error(error_msg)
        return None, error_msg
    else:
        output_filename = safe_filename

    # Exit if destination contains a file with the same name (prevent inadvertent overwrite)
    if output_filename in [file.name for file in fw.get(dest_id).files]:
        if not overwrite:
            error_msg = f'A file named {output_filename} is already in {dest_id}! No files will be de-identified.'
            log.error(error_msg)
            return None, error_msg
        else:
            log.info(
                f'A file named {output_filename} is already in {dest_id}. This file will be overwritten if no'
                ' exceptions are encountered.'
            )

    file_path = gear_context.get_input_path('input_file')

    # Copy the input_file to a new path if the filenames differ. It is intentional that the name will be changed
    # for input files that contain unsafe characters, even though the user intended to copy the filename of the original
    if os.path.basename(file_path) != output_filename:
        new_path = os.path.join(os.path.dirname(file_path), output_filename)
        log.info(f'Renaming {file_path} to {new_path}')
        shutil.copy2(file_path, new_path)
        file_path = new_path

    profile_path = gear_context.get_input_path('deid_profile')

    deid_filepath = deid_file.deidentify_path(
        input_file_path=file_path,
        profile_path=profile_path,
        output_directory=gear_context.output_dir
    )
    if deid_filepath:
        write_deid_file_metadata(gear_context, 'input_file', deid_filepath)
    return deid_filepath, None


if __name__ == '__main__':
    log = create_gear_logger('INFO', 'grp-13-deid-file')
    with flywheel.GearContext() as gear_context:
        exit_status = None
        try:
            deid_filepath, error_msg = main(gear_context)
            if deid_filepath:
                if os.path.exists(deid_filepath):
                    log.info(f'Successfully processed {deid_filepath}')
                    exit_status = 0
                    #gear_context.update_file_metadata(os.path.basename(deid_filepath), )
            else:
                exit_status = 1
        except Exception as e:
            error_msg = f'An exception occurred when attempting to de-identify:\n {type(e).__name__}: {e}\n'
            log.error(error_msg, exc_info=True)
            exit_status = 1

    log.info(f'Exit status is {exit_status}')
    log.info(f'Exit time::{datetime.datetime.now(datetime.timezone.utc)}')
    os.sys.exit(exit_status)

