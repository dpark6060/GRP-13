import pytest
from pathlib import Path
from deid_export.container_export import load_template_dict, quote_numeric_string, matches_file
from deid_export.deid_template import load_deid_profile

DATA_ROOT = Path(__file__).parent/'data'


def test_can_load_template_file(template_file):
    template_path = template_file('example1-deid-profile.yaml')
    res = load_template_dict(str(template_path))
    assert res is not None
    assert 'dicom' in res
    assert 'export' in res


def test_load_template_file_raises_with_path_issue():
    with pytest.raises(FileNotFoundError):
        load_template_dict(str(DATA_ROOT / 'does-not-exist.yaml'))


def test_quote_numeric_string():
    in_str = 'test'
    out_str = quote_numeric_string(in_str)
    assert out_str == 'test'

    in_str = '1'
    out_str = quote_numeric_string(in_str)
    assert out_str == '"1"'

    in_str = '1.1'
    out_str = quote_numeric_string(in_str)
    assert out_str == '"1.1"'

    in_str = '1.1.1'
    out_str = quote_numeric_string(in_str)
    assert out_str == '1.1.1'


def test_matches_file(template_file):
    template_path = template_file('example-3-deid-profile.yaml')
    template_dict = load_template_dict(str(template_path))
    template_dict['dicom']['file-filter'] = []
    deid_profile, _ = load_deid_profile(template_dict)
    assert matches_file(deid_profile, {'type': 'dicom'})
    assert matches_file(deid_profile, {'name': 'test.jpg'})
    template_dict['jpg']['file-filter'] = []
    deid_profile, _ = load_deid_profile(template_dict)
    assert not matches_file(deid_profile, {'name': 'test.jpg'})
    template_dict.pop('dicom')
    deid_profile, _ = load_deid_profile(template_dict)
    assert not matches_file(deid_profile, {'type': 'dicom', 'name': 'test.dcm'})