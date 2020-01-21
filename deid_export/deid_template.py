#!/usr/bin/env python
import tempfile
import argparse
import copy
import pandas as pd
from pathlib import Path
from ruamel.yaml import load, safe_dump, Loader, dump
import logging

DEFAULT_REQUIRED_COLUMNS = ['subject.code']
DEFAULT_SUBJECT_CODE_COL = 'subject.code'
DEFAULT_NEW_SUBJECT_CODE_COL = 'export.subject.code'
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
        else:
            return find_profile_element(d[tps[0]], '.'.join(tps[1:]))


def update_deid_profile(deid_template, updates):
    """Return the updated deid profile

    Args:
        deid_template (dict): Deid profile template in dictionary form (load from YML file)
        updates (dict): A dictionary of key/value to be updated (e.g. a row from a csv file)

    Returns:
        (dict): The updated deid profile dictionary
    """

    new_deid = copy.deepcopy(deid_template)

    for k in updates.keys():
        try:
            el, key_or_fieldinfo, is_fields = find_profile_element(new_deid, k)
            if is_fields:  # fields value is a list
                field_name, field_action = key_or_fieldinfo.split('.')
                for f in el:
                    if f.get('name') == field_name:
                        r_type = type(f.get(field_action))
                        f[field_action] = r_type(updates.get(k, f[field_action]))
            else:
                r_type = type(el[key_or_fieldinfo])  # used for dumping to yml consistently with template values
                el[key_or_fieldinfo] = r_type(updates.get(k, el[key_or_fieldinfo]))
        except KeyError:
            logger.info(f'{k} did not match anything in template')

    return new_deid


def validate(deid_template_path,
             csv_path,
             subject_code_col=DEFAULT_SUBJECT_CODE_COL,
             new_subject_code_col=DEFAULT_NEW_SUBJECT_CODE_COL,
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
        subject_code_col (str): Subject code column name
        new_subject_code_col (str): New subject code column name
        required_cols (list): List of column name required

    Raises:
        ValueError: When checks do not pass

    Returns:
        (pandas.DataFrame): a DataFrame generated from parsing of the CSV at csv_path
    """

    if required_cols is None:
        required_cols = DEFAULT_REQUIRED_COLUMNS

    with open(deid_template_path, 'r') as fid:
        deid_template = load(fid, Loader=Loader)

    df = pd.read_csv(csv_path, dtype=str)
    if new_subject_code_col != DEFAULT_NEW_SUBJECT_CODE_COL:
        if new_subject_code_col not in df:
            raise ValueError(f'columns {new_subject_code_col} is missing from dataframe')
        else:
            df[DEFAULT_NEW_SUBJECT_CODE_COL] = df[new_subject_code_col]
    for c in required_cols:
        if c not in df:
            raise ValueError(f'columns {c} is missing from dataframe')

    if not df[subject_code_col].is_unique:
        raise ValueError(f'{subject_code_col} is not unique in dataframe')

    if not df[new_subject_code_col].is_unique:
        raise ValueError(f'{new_subject_code_col} is not unique in dataframe')

    # Log warning if columns is not matching deid profile template
    cols = list(df.columns)
    cols.remove(subject_code_col)
    for k in cols:
        try:
            el, key_or_fieldinfo, is_fields = find_profile_element(deid_template, k)
            if is_fields:  # fields value is a list
                field_name, field_action = key_or_fieldinfo.split('.')
                log_field_mismatch = True
                for f in el:
                    if f.get('name') == field_name and field_action in f.keys():
                        log_field_mismatch = False
                if log_field_mismatch:
                    logger.warning(f'Column `{k}` not found in DeID template')

            else:
                _ = el[key_or_fieldinfo]
        except KeyError:
            logger.warning(f'Column `{k}` not found in DeID template')

    return df


def get_updated_template(df,
                         deid_template,
                         subject_code=None,
                         subject_code_col=DEFAULT_SUBJECT_CODE_COL,
                         dest_template_path=None):
    """Return path to updated DeID profile

    Args:
        df (pandas.DataFrame): Dataframe representation of some mapping info
        subject_code (str): value matching subject_code_col in row used to update the template
        deid_template (dict): Dictionary representation of the deid profile
        subject_code_col (str): Subject code column name
        dest_template_path (Path-like): Path to output DeID profile

    Returns:
        (str): Path to output DeID profile
    """

    series = df[df[subject_code_col] == subject_code].squeeze()
    logger.critical(f'subject series: {series}')
    series.pop(subject_code_col)
    if series.empty:
        raise ValueError(f'{subject_code} not found in csv')
    else:
        new_deid = update_deid_profile(deid_template, series.to_dict())
        if dest_template_path is None:
            dest_template_path = tempfile.NamedTemporaryFile().name
        with open(dest_template_path, 'w+') as fid:
            dump(new_deid, fid, default_flow_style=False)
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

    with open(deid_template_path, 'r') as fid:
        deid_template = load(fid, Loader=Loader)

    df = pd.read_csv(csv_path, dtype=str)

    deids_paths = {}
    for subject_code in df[subject_code_col]:
        dest_template_path = Path(output_dir) / f'{subject_code}.yml'
        deids_paths[subject_code] = get_updated_template(df, deid_template,
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
