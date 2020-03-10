import pytest
import pandas as pd
import tempfile
from pathlib import Path
from ruamel import yaml
from deid_export.deid_template import update_deid_profile, validate, process_csv, DEFAULT_REQUIRED_COLUMNS
import logging

DATA_ROOT = Path(__file__).parent/'data'


def test_can_update_deid_dicom_profile():

    with open(DATA_ROOT/'example1-deid-profile.yaml') as fid:
        config = yaml.load(fid, Loader=yaml.SafeLoader)
        config['zip'] = {}
    replace_with = {
        'dicom.date-increment': -20,
        'dicom.fields.PatientID.replace-with': 'TEST',
        'dicom.fields.PatientBirthDate.remove': False,
        'export.subject.code': 'TEST'
    }

    new_config = update_deid_profile(config, replace_with)
    assert new_config['only-config-profiles'] is True
    assert new_config['zip']['validate-zip-members'] is True
    assert new_config['dicom']['date-increment'] == -20
    assert new_config['dicom']['fields'][0]['remove'] is False
    assert new_config['dicom']['fields'][1]['replace-with'] == 'TEST'
    assert new_config['export']['subject']['code'] == 'TEST'


def test_update_deid_dicom_profile_log_if_no_match_found(caplog):
    with open(DATA_ROOT/'example1-deid-profile.yaml') as fid:
        config = yaml.load(fid, Loader=yaml.SafeLoader)
    replace_with = {'dicom.does-not-exist': -20}
    with caplog.at_level(logging.INFO, logger="deid_export.deid_template"):
        update_deid_profile(config, replace_with)
        assert 'dicom.does-not-exist' in caplog.messages[0]


def test_validate_raises_if_missing_required_columns():
    profile_path = DATA_ROOT/'example1-deid-profile.yaml'

    df = pd.read_csv(DATA_ROOT/'example-csv-mapping.csv')
    df.iloc[2, 0] = df.iloc[1, 0]
    with tempfile.NamedTemporaryFile(mode='w') as fp:
        df.to_csv(fp, index=False)
        with pytest.raises(ValueError) as exc:
            validate(profile_path, fp.name, required_cols=DEFAULT_REQUIRED_COLUMNS)
            assert 'not unique' in exc.value.args[0]

    df = pd.read_csv(DATA_ROOT/'example-csv-mapping.csv')
    df = df.drop('subject.code', axis=1)
    with tempfile.NamedTemporaryFile(mode='w') as fp:
        df.to_csv(fp, index=False)
        with pytest.raises(ValueError) as exc:
            validate(profile_path, fp.name, required_cols=DEFAULT_REQUIRED_COLUMNS)
            assert 'subject.code' in exc.value.args[0]


def test_validate_log_warning_for_inconsistencies(caplog):
    with open(DATA_ROOT/'example1-deid-profile.yaml') as fid:
        profile = yaml.load(fid, Loader=yaml.SafeLoader)
    profile['dicom']['fields'] = profile['dicom']['fields'][:1]

    with tempfile.NamedTemporaryFile(mode='w') as fp:
        yaml.dump(profile, fp, Dumper=yaml.SafeDumper)
        with caplog.at_level(logging.DEBUG, logger="deid_export.deid_template"):
            validate(fp.name, DATA_ROOT/'example-csv-mapping.csv', required_cols=DEFAULT_REQUIRED_COLUMNS)
            assert 'dicom.fields.PatientID.replace-with' in caplog.messages[0]


def test_process_csv():
    with tempfile.TemporaryDirectory() as tmp_dir:

        res = process_csv(DATA_ROOT/'example-csv-mapping.csv',
                          DATA_ROOT/'example1-deid-profile.yaml',
                          output_dir=tmp_dir)

        assert '001' in res.keys() and '002' in res.keys() and '003' in res.keys()
        with open(res['001'], 'r') as fid:
            profile = yaml.load(fid, Loader=yaml.SafeLoader)
            assert profile['dicom']['fields'][1]['replace-with'] == 'IDA'
            assert profile['export']['subject']['code'] == 'FLYWHEEL'
            assert profile['dicom']['date-increment'] == -15
            assert profile['dicom']['fields'][0]['remove'] is True
