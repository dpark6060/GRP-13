import json
import logging
import os

import yaml
from flywheel_migration.deidentify.deid_profile import DeIdProfile

log = logging.getLogger('deidentify_dicom')


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