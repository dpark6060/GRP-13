import pytest
import pandas as pd
import tempfile
from pathlib import Path
from ruamel import yaml
from deid_export.deid_template import update_deid_profile, validate, process_csv, DEFAULT_REQUIRED_COLUMNS
import logging

DATA_ROOT = Path(__file__).parent/'data'


def test_can_update_deid_dicom_profile():

    with tempfile.NamedTemporaryFile(suffix='.yaml') as tmpfile:

        replace_with = {
            'DATE_INCREMENT': -20,
            'SUBJECT_ID': 'TEST',
            'PATIENT_BD_BOOL': False,
        }

        update_deid_profile(DATA_ROOT/'example4-deid-profile-jinja.yaml',
                            replace_with,
                            dest_path=tmpfile.name)

        with open(tmpfile.name, 'r') as fid:
            new_config = yaml.load(fid, Loader=yaml.SafeLoader)

        assert new_config['only-config-profiles'] is True
        assert new_config['zip']['validate-zip-members'] is True
        assert new_config['dicom']['date-increment'] == -20
        assert new_config['dicom']['fields'][0]['remove'] is False
        assert new_config['dicom']['fields'][1]['replace-with'] == 'TEST'
        assert new_config['export']['subject']['label'] == 'TEST'


def test_validate_raises_if_missing_required_columns():
    profile_path = DATA_ROOT/'example4-deid-profile-jinja.yaml'

    df = pd.read_csv(DATA_ROOT/'example-csv-mapping.csv')
    df.iloc[2, 0] = df.iloc[1, 0]
    with tempfile.NamedTemporaryFile(mode='w') as fp:
        df.to_csv(fp, index=False)
        with pytest.raises(ValueError) as exc:
            validate(profile_path, fp.name, required_cols=DEFAULT_REQUIRED_COLUMNS)
            assert 'not unique' in exc.value.args[0]

    df = pd.read_csv(DATA_ROOT/'example-csv-mapping.csv')
    df = df.drop('subject.label', axis=1)
    with tempfile.NamedTemporaryFile(mode='w') as fp:
        df.to_csv(fp, index=False)
        with pytest.raises(ValueError) as exc:
            validate(profile_path, fp.name, required_cols=DEFAULT_REQUIRED_COLUMNS)
            assert 'subject.label' in exc.value.args[0]


def test_validate_log_warning_for_inconsistencies(caplog):
    df = pd.read_csv(DATA_ROOT/'example-csv-mapping.csv')
    # add a column that won't match the template
    df['NotMatchingTemplate'] = 1
    with tempfile.NamedTemporaryFile(suffix='.csv') as tmpfile:
        df.to_csv(tmpfile.name, index=False)

        with caplog.at_level(logging.DEBUG, logger="deid_export.deid_template"):
            validate(DATA_ROOT/'example4-deid-profile-jinja.yaml',
                     tmpfile.name)
            assert 'NotMatchingTemplate' in caplog.messages[0]


def test_process_csv():
    with tempfile.TemporaryDirectory() as tmp_dir:

        res = process_csv(DATA_ROOT/'example-csv-mapping.csv',
                          DATA_ROOT/'example4-deid-profile-jinja.yaml',
                          output_dir=tmp_dir)

        assert '001' in res.keys() and '002' in res.keys() and '003' in res.keys()
        with open(res['001'], 'r') as fid:
            profile = yaml.load(fid, Loader=yaml.SafeLoader)
            assert profile['dicom']['fields'][1]['replace-with'] == 'IDA'
            assert profile['export']['subject']['label'] == 'IDA'
            assert profile['dicom']['date-increment'] == -15
            assert profile['dicom']['fields'][0]['remove'] is False


def test_can_update_deid_dicom_profile_filename_section():
    updates = {'SUBJECT_ID': 'TEST'}
    with tempfile.NamedTemporaryFile(suffix='.yaml') as tmpfile:
        update_file = update_deid_profile(DATA_ROOT/'example4-deid-profile-jinja.yaml',
                                          updates,
                                          dest_path=tmpfile.name)
        with open(update_file, 'r') as fid:
            template = yaml.load(fid, Loader=yaml.SafeLoader)
            assert template['dicom']['filenames'][0]['groups'][0]['replace-with'] == 'TEST'
