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
from flywheel_migration.deidentify.deid_profile import DeIdProfile

log = logging.getLogger('deidentify_dicom')


@contextlib.contextmanager
def make_temp_directory():
    temp_dir = tempfile.mkdtemp()
    try:
        yield temp_dir
    finally:
        shutil.rmtree(temp_dir)


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
                    file_path = os.path.join(file_directory,zip_item.filename)
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
        os.rename(tmp_zip_path, output_path)
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


def deidentify_dicoms_inplace(profile_path, input_directory):
    """
    Given profile_path to a valid flywheel de-id profile with a "dicom" namespace, this function
    replaces original files with de-identified copies of DICOM files .
    Returns a list of paths to the deidentified files. If no changes were imposed by the profile,
    no files will be modified
    :param profile_path: path to the de-id profile to apply
    :param input_directory: directory containing the dicoms to be de-identified
    :return: deidentified_file_list, list of paths to deidentified files or None if no files are de-identified
    :rtype: list
    """
    with make_temp_directory() as tmp_deid_dir:

        # OSFS setup
        src_fs = osfs.OSFS(input_directory)
        dst_fs = osfs.OSFS(tmp_deid_dir)

        # Get list of files (dicom files do not always have an extension in the wild)
        file_list = [match.path for match in src_fs.glob('**/*', case_sensitive=False) if not match.info.is_dir]

        # Monkey-patch get_dest_path to return the original path
        # This necessitates creating any missing subdirectories
        def default_path(state, record, path):
            dst_fs.makedirs(fs.path.dirname(path), recreate=True)
            return path

        # Get the dicom profile from the de-id profile
        dcm_profile = load_dicom_deid_profile(profile_path)
        dcm_profile.get_dest_path = default_path
        dcm_profile.process_files(src_fs, dst_fs, file_list)

        # get list of modified files in tmp_deid_dir
        diff_files = return_diff_files(input_directory, tmp_deid_dir)
        deid_files = [match.path for match in dst_fs.glob('**/*', case_sensitive=False) if not match.info.is_dir]
        deid_paths = list()
        for deid_file in deid_files:
            deid_path = os.path.join(input_directory, deid_file.lstrip(os.path.sep))
            deid_paths.append(deid_path)
        if diff_files:
            deidentified_file_list = list()
            log.info(f'Saving {len(diff_files)} deidentified files')
            for diff_file in diff_files:
                tmp_filepath = os.path.join(tmp_deid_dir, diff_file)
                replace_filepath = os.path.join(input_directory, diff_file)
                shutil.move(tmp_filepath, replace_filepath)
                deidentified_file_list.append(replace_filepath)
        else:
            log.info(f'DICOMS already conform to {profile_path}. No files will be deidentified')
            deidentified_file_list = None
    return deidentified_file_list

