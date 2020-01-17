#!/usr/bin/env python
import pandas as pd
from pathlib import Path
# from yaml import Loader, load, dump, Dumper ## > not using ISSUE WITH DUMPING STRING WITHOUT QUOTE
from ruamel.yaml import load, safe_dump, Loader, dump  # Use by migrationg-toolkit anyway
import logging


CSV_PATH = Path(__file__).parent/'dummy.csv'
DEID_TEMPLATE_PATH = Path(__file__).parent/'template-profile.yml'
OUTPUT_DIR = '/tmp'


REQUIRED_COLUMNS = ['patient_label']
PATIENT_LABEL = 'patient_label'
ACTIONS_LIST = ['replace-with', 'remove', 'increment-date', 'hash', 'hashuid']

logger = logging.getLogger(__name__)


def update_deid_profile(deid_template, updates):
    """Return the updated deid profile

    Args:
        deid_template (dict): Deid profile template in dictionary form (load from YML file)
        updates (dict): A dictionary of key/value to be updated (e.g. a row from a csv file)

    Returns:
        (dict): The updated deid profile dictionary
    """
    new_deid = deid_template.copy()

    # update dicom values
    dk_updates = [k for k in new_deid.get('dicom', {}).keys() if k in updates.keys()]
    for k in dk_updates:
        r_type = type(deid_template['dicom'][k])
        new_deid['dicom'][k] = updates.get(r_type(updates[k]), deid_template['dicom'][k])

    # update fields values
    for i, x in enumerate(new_deid.get('dicom', {}).get('fields', [])):
        if x.get('name') in updates.keys():
            ks = list(x.keys())
            ks.remove('name')
            action = ks[0]  # should be only one based on validation step
            r_type = type(x[action])
            x[action] = r_type(updates[x.get('name')])

    return deid_template


def validate(deid_template, df):
    """Validate consistency of the dedi template profile and a dataframe

    Checks that:
    - df contains some required columns
    - 'fields' is not a column
    - the patient_label column has unique values
    - fields action are supported and correctly formatted

    Log warning if:
    -  columns of dataframe does not match any of the keys of the deid profile template

    Args:
        deid_template (dict): Deid profile template in dictionary form (load from YML file)
        df (pandas.Dataframe): A dataframe with columns matching the deid_template keys

    Raises:
        ValueError: When checks do not pass

    """
    for c in REQUIRED_COLUMNS:
        if c not in df:
            raise ValueError(f'columns {c} header is missing from dataframe')

    if 'fields' in df:
        raise ValueError(f'`fields` cannot be a column name')

    if not df.patient_label.is_unique:
        raise ValueError(f'{PATIENT_LABEL} is not unique')

    # validate that fields action are supported and correctly formatted
    for i, x in enumerate(deid_template.get('dicom', {}).get('fields', [])):
        ks = list(x.keys())
        ks.remove('name')
        if len(ks) > 2 or ks[0] not in ACTIONS_LIST:
            raise ValueError(f'Field with name {x["name"]} is not supported')

    # warns if columns is not matching deid profile template
    cols_to_check = list(df.columns)
    cols_to_check.remove(PATIENT_LABEL)
    dicom_keys = list(deid_template.get('dicom', {}).keys())
    name_keys = [x.get('name') for x in deid_template.get('dicom', {}).get('fields', [])]
    for c in cols_to_check:
        # check if key in Dicom dict, or a name of a field, warns otherwise
        if not (c in dicom_keys or c in name_keys):
            logger.warning(f'{c} not defined in template deid profile')


def process_csv(csv_path=None, deid_template_path=None, output_dir=None):
    """Generate patient specific deid profile

    Args:
        csv_path (Path-like): Path to CSV file
        deid_template_path (Path-like): Path to the deid profile template
        output_dir (Path-like): Path to ouptut dir where yml are saved
    """
    with open(deid_template_path, 'r') as fid:
        deid_template = load(fid, Loader=Loader)

    df = pd.read_csv(csv_path, dtype=str)

    validate(deid_template, df)

    for i, r in df.iterrows():
        patient_label = r.pop('patient_label')
        new_deid = update_deid_profile(deid_template, r.to_dict())

        with open(Path(output_dir)/f'deid_{patient_label}.yml', 'w+') as fid:
            dump(new_deid, fid, default_flow_style=False)


if __name__ == '__main__':
    process_csv(csv_path=CSV_PATH,
                deid_template_path=DEID_TEMPLATE_PATH,
                output_dir=OUTPUT_DIR)
