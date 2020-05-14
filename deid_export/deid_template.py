#!/usr/bin/env python3

import argparse
import re
import logging
from pathlib import Path
import tempfile
from dotty_dict import Dotty

import pandas as pd
from ruamel.yaml import load, safe_dump, Loader, dump
from jinja2 import Environment
from flywheel_migration import deidentify

DEFAULT_REQUIRED_COLUMNS = ['subject.label']
DEFAULT_SUBJECT_CODE_COL = 'subject.label'
DEFAULT_NEW_SUBJECT_LOC = 'export.subject.label'
ACTIONS_LIST = ['replace-with', 'remove', 'increment-date', 'hash', 'hashuid']

logger = logging.getLogger(__name__)


def find_profile_element(d, target):
    """Traverse dictionary following target and return matching element

    Args:
        d (dict): Dictionary from a deid profile template

        target (str): Period separated path in dictionary tree (e.g. dicom.filename.destination). If field action
            is targeted, format must match <filetype>.fields.<fieldname>.<actionname>
            (e.g. dicom.fields.PatientID.replace-with)

    Returns:
        element: Final element in the dictionary tree matching target (not the value) or list if is_fields=True
        target: Final key
        is_fields (bool): True is element is the list founds as value for key='fields'
    """
    tps = target.split('.')
    if len(tps) == 1:
        return d, target, False
    else:
        if tps[0] == 'fields':
            return d['fields'], '.'.join(tps[1:]), True
        if tps[0] == 'groups':
            return d['groups'], '.'.join(tps[1:]), True
        else:
            if isinstance(d, list):
                return find_profile_element(d[int(tps[0])], '.'.join(tps[1:]))
            else:
                return find_profile_element(d[tps[0]], '.'.join(tps[1:]))


def _add_zip_member_validation(deid_template):
    if 'zip' in deid_template.keys():
        if 'validate-zip-members' not in deid_template['zip'].keys():
            deid_template['zip']['validate-zip-members'] = True
    return deid_template


def update_deid_profile(deid_template_path, updates, dest_path=None):
    """Return the updated deid profile

    Args:
        deid_template_path (Path-like): Path to deid profile template
        updates (dict): A dictionary of key/value to be updated (e.g. a row from a csv file)
        dest_path (Path-like): Path where update template is saved
    """

    load_path = deid_template_path

    # update jinja2 variable
    if updates:
        with open(load_path, 'r') as fp:
            deid_template_str = fp.read()
            # remove quote around jinja var to allow for casting infered from dataframe
            deid_template_str = deid_template_str.replace('"{{', '{{')
            deid_template_str = deid_template_str.replace('}}"', '}}')
        env = Environment()
        jinja_template = env.from_string(deid_template_str)
        with open(dest_path, 'w') as fp:
            fp.write(jinja_template.render(**updates))
        load_path = dest_path

    # ensure zip members are present
    with open(load_path, 'r') as fid:
        deid_template = load(fid, Loader=Loader)
    if 'only-config-profiles' not in deid_template.keys():
        deid_template['only-config-profiles'] = True
    deid_template = _add_zip_member_validation(deid_template)

    with open(dest_path, 'w') as fid:
        dump(deid_template, fid, default_flow_style=False)

    return dest_path


def validate(deid_template_path,
             csv_path,
             subject_label_col=DEFAULT_SUBJECT_CODE_COL,
             new_subject_label_loc=DEFAULT_NEW_SUBJECT_LOC,
             required_cols=None):
    """Validate consistency of the deid template profile and a dataframe

    Checks that:

    * df contains some required columns
    * the subject code columns have unique values

    Logs warning if:
    *  columns of dataframe does not match deid profile template

    Args:
        deid_template_path (Path-like): Path to Deid template .yml profile
        csv_path (Path-like): Path to csv file
        subject_label_col (str): Subject code column name
        new_subject_label_loc (str): New subject location in template (dotty dict notation)
        required_cols (list): List of column name required

    Raises:
        ValueError: When checks do not pass

    Returns:
        (pandas.DataFrame): a DataFrame generated from parsing of the CSV at csv_path
    """

    if required_cols is None:
        required_cols = DEFAULT_REQUIRED_COLUMNS

    df = pd.read_csv(csv_path, dtype=str)

    # Get jinja variables (e.g. defined as {{ arg }})
    with open(deid_template_path, 'r') as fid:
        deid_template_str = fid.read()
    jinja_vars = re.findall(r'{{.*}}', deid_template_str)
    jinja_vars = [v.strip('{} ') for v in jinja_vars]
    required_cols += jinja_vars
    required_cols = set(required_cols)

    # Check for uniqueness of subject columns
    if subject_label_col in df:
        if not df[subject_label_col].is_unique:
            raise ValueError(f'{subject_label_col} is not unique in csv')

    with open(deid_template_path, 'r') as fid:
        deid_template = load(fid, Loader=Loader)
    new_subject_col = Dotty(deid_template).get(new_subject_label_loc, '').strip('{} ')
    if new_subject_col in df:
        if not df[new_subject_col].is_unique:
            raise ValueError(f'{new_subject_col} is not unique in csv')

    for c in required_cols:
        if c not in df:
            raise ValueError(f'columns {c} is missing from dataframe')

    for c in df:
        if c not in required_cols:
            logger.warning(f'Column `{c}` not found in DeID template')

    return df


def load_deid_profile(template_dict):
    """
    Load the flywheel.migration DeIdProfile at the profile_path

    Args:
        template_dict(dict): a dictionary loaded from the de-identification template file that will be provided as
            config to DeIdProfile

    Returns:
        flywheel_migration.deidentify.DeIdProfile, export_config
    """
    deid_profile = deidentify.DeIdProfile()
    deid_profile.load_config(template_dict)
    export_config = template_dict.get('export', dict())
    return deid_profile, export_config


def get_updated_template(df,
                         deid_template_path,
                         subject_code=None,
                         subject_code_col=DEFAULT_SUBJECT_CODE_COL,
                         dest_template_path=None):
    """Return path to updated DeID profile

    Args:
        df (pandas.DataFrame): Dataframe representation of some mapping info
        subject_code (str): value matching subject_code_col in row used to update the template
        deid_template_path (path-like): Path to a deid template
        subject_code_col (str): Subject code column name
        dest_template_path (Path-like): Path to output DeID profile

    Returns:
        (str): Path to output DeID profile
    """

    series = df[df[subject_code_col] == subject_code].squeeze()
    series.pop(subject_code_col)
    if series.empty:
        raise ValueError(f'{subject_code} not found in csv')
    else:
        if dest_template_path is None:
            dest_template_path = tempfile.NamedTemporaryFile().name
        update_deid_profile(deid_template_path, series.to_dict(), dest_path=dest_template_path)

    return dest_template_path


def process_csv(csv_path, deid_template_path, subject_code_col=DEFAULT_SUBJECT_CODE_COL, output_dir='/tmp'):
    """Generate patient specific deid profile

    Args:
        csv_path (Path-like): Path to CSV file
        deid_template_path (Path-like): Path to the deid profile template
        output_dir (Path-like): Path to ouptut dir where yml are saved
        subject_code_col (str): Subject code column name

    Returns:
        dict: Dictionary with key/value = subject.code/path to updated deid profile
    """

    validate(deid_template_path, csv_path)

    df = pd.read_csv(csv_path, dtype=str)

    deids_paths = {}
    for subject_code in df[subject_code_col]:
        dest_template_path = Path(output_dir) / f'{subject_code}.yml'
        deids_paths[subject_code] = get_updated_template(df,
                                                         deid_template_path,
                                                         subject_code=subject_code,
                                                         subject_code_col=subject_code_col,
                                                         dest_template_path=dest_template_path)
    return deids_paths


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('csv_path', help='path to the CSV file')
    parser.add_argument('deid_template_path', help='Path to source de-identification profile to modify')
    parser.add_argument('--output_directory', help='path to which to save de-identified template')
    parser.add_argument('--subject_code_col', help='Name of the column containing subject codes')

    args = parser.parse_args()

    res = process_csv(args.csv_path,
                      args.deid_template_path,
                      subject_code_col=args.subject_code_col,
                      output_dir=args.output_directory)

    print(res)
