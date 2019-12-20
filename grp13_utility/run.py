#!/usr/bin/env python3

import logging
import os
import re
import shutil

import flywheel
import deid_file
log = logging.getLogger(__name__)
log.setLevel('INFO')

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


def main(gear_context):
    exit_status = 0
    input_file_dict = gear_context.get_input('input_file')
    dest_id = gear_context.destination.get('id')
    if dest_id == 'aex':
        dest_id = '5dc9e6d7bd690a002adaa1f4'
    output_filename = gear_context.config.get('output_filename')
    fw = gear_context.client

    # Make the output_filename safe
    safe_filename = ensure_filename_safety(filename=output_filename)
    # Exit if no safe characters were provided
    if not safe_filename:
        log.error(f'No safe characters in filename {output_filename}. Exiting...')
        exit_status = 1
        return None, exit_status
    else:
        output_filename = safe_filename

    # Exit if destination contains a file with the same name (prevent inadvertant overwrite)
    if output_filename in [file.name for file in fw.get(dest_id).files]:
        log.error(f'A file named {output_filename} is already in {dest_id}!')
        exit_status = 1
        return None, exit_status

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
    return deid_filepath, exit_status


if __name__ == '__main__':
    with flywheel.GearContext() as gear_context:

        try:
            deid_filepath, exit_status = main(gear_context)
        except Exception as e:
            log.error(f'An exception occurred when attempting to de-identify {e}')
            exit_status = 1
        finally:

            append_to_destination_origin_list(gear_context)

    if os.path.exists(deid_filepath):
        log.info(f'Successfully processed {deid_filepath}')

    log.info(f'Exit status is {exit_status}')
    os.sys.exit(exit_status)


