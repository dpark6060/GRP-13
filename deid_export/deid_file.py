import argparse
import contextlib
import filecmp
import fs
from fs import osfs
import json
import logging
import os
import shutil
import tempfile
import zipfile


import yaml

from flywheel_migration import deidentify
from flywheel_migration.deidentify.deid_profile import DeIdProfile

log = logging.getLogger(__name__)


@contextlib.contextmanager
def make_temp_directory():
    temp_dir = tempfile.mkdtemp()
    try:
        yield temp_dir
    finally:
        shutil.rmtree(temp_dir)


def extract_files(zip_path, output_directory):
    """
    extracts the files in a zip to an output directory
    :param zip_path: path to the zip to extract
    :param output_directory: directory to which to extract the files
    :return: file_list, a list to the paths of the extracted files and comment, the archive comment
    """
    with zipfile.ZipFile(zip_path, 'r') as zipf:
        zipf.extractall(output_directory)
        file_list = zipf.namelist()
        # Get full paths and remove directories from list
        file_list = [os.path.join(output_directory, file) for file in file_list if not file.endswith('/')]
        real_files = [fp for fp in file_list if os.path.isfile(fp)]
        assert file_list == real_files
    return file_list


def recreate_zip(dest_zip, file_directory, output_directory=None):
    """
    Given a dest_zip and file_directory that contains extracted(modified) files from dest_zip,
    this function will replace dest_zip with a zip that contains files from file_directory that
    match the original zip's filename. If output_directory is provided, the resulting zip will be saved to
    output_directory rather than overwriting dest_zip.
    :param dest_zip: path to the zip archive to be modified
    :param file_directory: path to the directory that contains files to replace those in dest_zip
    :param output_directory: directory to which to save output zip, if None, will overwrite dest_zip
    :return: output_path, a path to the resultant zip
    """
    # temporary directory context
    with make_temp_directory() as temp_dir:
        # temporary file context
        _, tmp_zip_path = tempfile.mkstemp(dir=temp_dir)
        # read zip context
        with zipfile.ZipFile(dest_zip, 'r') as zin:
            # write zip context
            with zipfile.ZipFile(tmp_zip_path, 'w') as zout:
                # preserve the archive comment
                zout.comment = zin.comment
                for zip_item in zin.infolist():
                    file_path = os.path.join(file_directory, zip_item.filename)
                    # If the file exists, in file_directory, add that, otherwise, add from dest_zip
                    if os.path.exists(file_path):
                        zout.write(file_path, zip_item.filename)
                    else:
                        log.warning(f'Extracted file {file_path} does not exist! Copying from original archive.')
                        zout.writestr(zip_item.filename, zin.read(zip_item.filename))

        if output_directory:
            output_path = os.path.join(output_directory, os.path.basename(dest_zip))
        else:
            output_path = dest_zip
            # replace dest_zip with the tmp_zip_path
            os.remove(dest_zip)

        shutil.move(tmp_zip_path, output_path)
        return output_path


def parse_deid_template(template_filepath):
    """
    Load the de-identification profile at template_filepath
    :param template_filepath: path to the de-identification template
    :type template_filepath: str
    :return: profile
    :rtype: flywheel_migration.deidentify.deid_profile.DeIdProfile
    """
    _, ext = os.path.splitext(template_filepath.lower())

    config = None
    try:
        if ext == '.json':
            with open(template_filepath, 'r') as f:
                config = json.load(f)
        elif ext in ['.yml', '.yaml']:
            with open(template_filepath, 'r') as f:
                config = yaml.load(f)
    except ValueError:
        log.exception('Unable to load config at: %s', template_filepath)

    if not config:
        raise ValueError('Could not load config at: {}'.format(template_filepath))

    profile = DeIdProfile()
    profile.load_config(config)

    return profile


def load_dicom_deid_profile(template_filepath):
    """
    Instantiates an instance of flywheel_migration.deidentify.dicom_file_profile.DicomFileProfile,
    given a path to a de-identification template
    :param template_filepath: the path to the YAML/JSON deidentification profile
    :type template_filepath: str
    :return: dicom_deid_profile
    :rtype: flywheel_migration.deidentify.dicom_file_profile.DicomFileProfile
    """

    deid_profile = parse_deid_template(template_filepath)
    dicom_deid_profile = deid_profile.get_file_profile('dicom')
    return dicom_deid_profile


def return_diff_files(original_dir, modified_dir):
    """
    Recursively compares files between two directories using filecmp.dircmp and returns a list of
    files that differ (including subdirectory
    :param original_dir: path to the original directory
    :param modified_dir: path to the modified directory
    :return: diff_files
    """
    diff_files = list()
    dir_compared = filecmp.dircmp(original_dir, modified_dir)
    diff_files.extend(dir_compared.diff_files)
    for subdir in dir_compared.common_dirs:
        subdir_diff_files = return_diff_files(os.path.join(original_dir, subdir), os.path.join(modified_dir, subdir))
        for diff_file in subdir_diff_files:
            diff_files.append(os.path.join(subdir, diff_file))

    return diff_files


def deidentify_files(profile_path, input_directory, profile_name='dicom', file_list=None,
                     output_directory=None, date_increment=None):
    """
    Given profile_path to a valid flywheel de-id profile with a "dicom" namespace, this function
    replaces original files with de-identified copies of DICOM files .
    Returns a list of paths to the deidentified files. If no changes were imposed by the profile,
    no files will be modified
    :param profile_path: path to the de-id profile to apply
    :param input_directory: directory containing the dicoms to be de-identified
    :param profile_name: name of the profile to pass .get_file_profile()
    :param output_directory: directory to which to save de-identified files. If not provided, originals will be replaced
    :param date_increment: date offset to apply to the profile
    :param file_list: optional list of relative paths of files to process, if not provided,
    will work on all files in the input_directory
    :return: deid_paths, list of paths to deidentified files or None if no files are de-identified
    :rtype: list
    """
    with make_temp_directory() as tmp_deid_dir:
        # Load the de-id profile from a file
        deid_profile = deidentify.load_profile(profile_path)
        if date_increment:
            deid_profile.date_increment = date_increment

        # OSFS setup
        src_fs = osfs.OSFS(input_directory)
        dst_fs = osfs.OSFS(tmp_deid_dir)

        if not output_directory:
            output_directory = input_directory

        if not file_list:
            # Get list of files (dicom files do not always have an extension in the wild)
            file_list = [match.path for match in src_fs.glob('**/*', case_sensitive=False) if not match.info.is_dir]

        # Monkey-patch get_dest_path to return the original path
        # This necessitates creating any missing subdirectories
        def default_path(state, record, path):
            dst_fs.makedirs(fs.path.dirname(path), recreate=True)
            return path
        
        # Get the dicom profile from the de-id profile
        file_profile = deid_profile.get_file_profile(profile_name)
        file_profile.get_dest_path = default_path
        file_profile.process_files(src_fs, dst_fs, file_list)

        # get list of modified files in tmp_deid_dir
        deid_files = [match.path for match in dst_fs.glob('**/*', case_sensitive=False) if not match.info.is_dir]
        deid_paths = list()
        for deid_file in deid_files:
            deid_file = deid_file.lstrip(os.path.sep)
            # Create list of de-identified files
            deid_path = os.path.join(output_directory, deid_file)
            deid_paths.append(deid_path)

            tmp_filepath = os.path.join(tmp_deid_dir, deid_file)
            replace_filepath = os.path.join(output_directory, deid_file)
            shutil.move(tmp_filepath, replace_filepath)

        if not deid_paths:
            return None

    return deid_paths


def deid_archive(zip_path, profile_path, output_directory=None, date_increment=None):
    with make_temp_directory() as temp_dir:
        file_list = extract_files(zip_path=zip_path, output_directory=temp_dir)
        deid_file_list = deidentify_files(
            input_directory=temp_dir, 
            profile_path=profile_path,
            date_increment=date_increment
        )
        output_zip_path = recreate_zip(dest_zip=zip_path, file_directory=temp_dir, output_directory=output_directory)
    return output_zip_path


def deidentify_path(input_file_path, profile_path, output_directory=None, date_increment=None):
    if output_directory and not os.path.exists(output_directory):
        log.info(f'{output_directory} does not exist, creating...')
        os.makedirs(output_directory)
    if zipfile.is_zipfile(input_file_path):
        log.info(f'Applying profile {os.path.basename(profile_path)} to archive {input_file_path}')
        deid_outpath = deid_archive(
            zip_path=input_file_path,
            profile_path=profile_path,
            output_directory=output_directory
        )
        return deid_outpath
    elif os.path.isfile(input_file_path):
        log.info(f'Applying profile {os.path.basename(profile_path)} to file {input_file_path}')
        deid_file_list = deidentify_files(
            input_directory=os.path.dirname(input_file_path),
            profile_path=profile_path,
            file_list=[os.path.basename(input_file_path)],
            output_directory=output_directory,
            date_increment=date_increment
        )
        return deid_file_list[0]
    elif os.path.isdir(input_file_path) and os.listdir(input_file_path):
        log.info(f'Applying profile {os.path.basename(profile_path)} to directory {input_file_path}')
        deid_file_list = deidentify_files(
            input_directory=input_file_path,
            profile_path=profile_path,
            output_directory=output_directory, 
            date_increment=date_increment
        )
        return deid_file_list
        
    else:
        log.error(f'{input_file_path} is not a file or a directory. No files will be de-identified.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('input_file_path', help='path to the file to de-identify')
    parser.add_argument('deid_profile', help='de-identification profile to apply')
    parser.add_argument('--output_directory', help='path to which to save de-identified files')
    parser.add_argument('--date_increment', help='days to offset template fields where specified')

    args = parser.parse_args()

    deidentify_path(
        input_file_path=args.input_file_path,
        profile_path=args.deid_profile,
        output_directory=args.output_directory,
        date_increment=args.date_increment
    )