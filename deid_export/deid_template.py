#!/usr/bin/env python
import argparse
import copy
import pandas as pd
from pathlib import Path
from ruamel.yaml import load, safe_dump, Loader, dump
import logging


REQUIRED_COLUMNS = ['subject.code']
SUBJECT_CODE = 'subject.code'
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


def validate(deid_template, df):
    """Validate consistency of the deid template profile and a dataframe

    Checks that:
    - df contains some required columns
    - the patient code column has unique values
    Logs warning if:
    -  columns of dataframe does not match deid profile template

    Args:
        deid_template (dict): Deid profile template in dictionary form (load from YML file)
        df (pandas.Dataframe): A dataframe with columns matching the deid_template keys

    Raises:
        ValueError: When checks do not pass
    """
    for c in REQUIRED_COLUMNS:
        if c not in df:
            raise ValueError(f'columns {c} is missing from dataframe')

    if not df[SUBJECT_CODE].is_unique:
        raise ValueError(f'{SUBJECT_CODE} is not unique in dataframe')

    # Log warning if columns is not matching deid profile template
    cols = list(df.columns)
    cols.remove(SUBJECT_CODE)
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


def process_csv(csv_path, deid_template_path, output_dir='/tmp'):
    """Generate patient specific deid profile

    Args:
        csv_path (Path-like): Path to CSV file
        deid_template_path (Path-like): Path to the deid profile template
        output_dir (Path-like): Path to ouptut dir where yml are saved

    Returns:
        dict: Dictionary with key/value = subject.code/path to updated deid profile
    """
    with open(deid_template_path, 'r') as fid:
        deid_template = load(fid, Loader=Loader)

    df = pd.read_csv(csv_path, dtype=str)

    validate(deid_template, df)
    deids_paths = {}
    for i, r in df.iterrows():
        patient_label = r.pop(SUBJECT_CODE)
        new_deid = update_deid_profile(deid_template, r.to_dict())
        dest_path = Path(output_dir)/f'deid_{patient_label}.yml'
        with open(dest_path, 'w+') as fid:
            dump(new_deid, fid, default_flow_style=False)
        deids_paths[patient_label] = dest_path

    return deids_paths


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('csv_path', help='path to the CSV file')
    parser.add_argument('deid_template_path', help='Path to source de-identification profile to modify')
    parser.add_argument('--output_directory', help='path to which to save de-identified template')

    args = parser.parse_args()

    process_csv(args.csv_path,
                args.deid_template_path,
                output_dir=args.output_directory)
