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


def extract_files(zip_path, output_directory):
    """Extracts the files in a zip to an output directory

    Args:
        zip_path (str): Path to the zip to extract
        output_directory (str): directory to which to extract the files

    Returns:
        list: A list to the paths of the extracted files and comment, the archive comment
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
    """Return path to resultant zip given dest_zip and file_directory

    Given a dest_zip and file_directory that contains extracted(modified) files from dest_zip,
    this function will replace dest_zip with a zip that contains files from file_directory that
    match the original zip's filename. If output_directory is provided, the resulting zip will be saved to
    output_directory rather than overwriting dest_zip.

    Args:
        dest_zip (str): Path to the zip archive to be modified
        file_directory (str): Path to the directory that contains files to replace those in dest_zip
        output_directory (str): Directory to which to save output zip, if None, will overwrite dest_zip

    Returns:
        (str): A path to the resultant zip
    """
    # temporary directory context
    with tempfile.TemporaryDirectory() as temp_dir:
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
    """Load the de-identification profile at template_filepath

    Args:
        template_filepath (str): Path to the de-identification template

    Returns:
        dict: Profile
    """
    _, ext = os.path.splitext(template_filepath.lower())

    template = None
    try:
        if ext == '.json':
            with open(template_filepath, 'r') as f:
                template = json.load(f)
        elif ext in ['.yml', '.yaml']:
            with open(template_filepath, 'r') as f:
                template = yaml.load(f, Loader=yaml.FullLoader)
    except ValueError:
        log.exception('Unable to load config at: %s', template_filepath)

    if not template:
        raise ValueError('Could not load config at: {}'.format(template_filepath))

    return template


def load_dicom_deid_profile(template_filepath):
    """Instantiates an instance of flywheel_migration.deidentify.dicom_file_profile.DicomFileProfile,
    given a path to a de-identification template

    Args:
        template_filepath (str): the path to the YAML/JSON deidentification profile

    Returns:
        object: A flywheel_migration.deidentify.dicom_file_profile.DicomFileProfile instance
    """

    deid_profile = parse_deid_template(template_filepath)
    dicom_deid_profile = deid_profile.get_file_profile('dicom')
    return dicom_deid_profile


def return_diff_files(original_dir, modified_dir):
    """Recursively compares files between two directories using filecmp.dircmp and returns a list of
    files that differ (including subdirectory

    Args:
        original_dir (str): Path to the original directory
        modified_dir (str): Path to the modified directory

    Returns:
        list: List of paths
    """
    diff_files = list()
    dir_compared = filecmp.dircmp(original_dir, modified_dir)
    diff_files.extend(dir_compared.diff_files)
    for subdir in dir_compared.common_dirs:
        subdir_diff_files = return_diff_files(os.path.join(original_dir, subdir), os.path.join(modified_dir, subdir))
        for diff_file in subdir_diff_files:
            diff_files.append(os.path.join(subdir, diff_file))

    return diff_files


def deidentify_file(deid_profile, file_path, output_directory):
    """

    Args:
        deid_profile(DeIdProfile): the de-identification profile to use to process the file
        file_path: the path to the file to be de-identified
        output_directory(str): the directory to which to output the de-identified file

    Returns:
        str: path to the de-identified file
    """
    dirname, basename = os.path.split(file_path)
    with osfs.OSFS(dirname) as src_fs:
        with osfs.OSFS(output_directory) as dst_fs:
            deid_profile.process_file(src_fs=src_fs, src_file=basename, dst_fs=dst_fs)
            deid_files = [dst_fs.getsyspath(fp) for fp in dst_fs.walk.files()]
    deid_path = deid_files[0]

    return deid_path


def deidentify_files(profile_path, input_directory, profile_name='dicom', file_list=None,
                     output_directory=None, date_increment=None):
    """
    Given profile_path to a valid flywheel de-id profile with a "dicom" namespace, this function
    replaces original files with de-identified copies of DICOM files .
    Returns a list of paths to the deidentified files. If no changes were imposed by the profile,
    no files will be modified

    Args:
        profile_path (str): Path to the de-id profile to apply
        input_directory (str): Directory containing the dicoms to be de-identified
        profile_name (str): Name of the profile to pass .get_file_profile()
        output_directory (str): Directory to which to save de-identified files. If not provided, originals will be
            replaced
        date_increment (str): Date offset to apply to the profile
        file_list (list, optional): Optional list of relative paths of files to process, if not provided, will work
            on all files in the input_directory

    Returns:
        list: list of paths to deidentified files or None if no files are de-identified
    """
    with tempfile.TemporaryDirectory() as tmp_deid_dir:
        # Load the de-id profile from a file
        deid_profile = deidentify.load_profile(profile_path)

        # Load the de-id profile as a dictionary
        template_dict = parse_deid_template(profile_path)

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
        if template_dict.get(profile_name):
            file_dict = template_dict.get(profile_name)
            if file_dict.get('remove_private_tags') == True:
                file_profile.remove_private_tags = True

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
    with tempfile.TemporaryDirectory() as temp_dir:
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