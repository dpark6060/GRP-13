import pytest
from pathlib import Path
from deid_export.container_export import hash_string, load_template_file, quote_numeric_string

DATA_ROOT = Path(__file__).parent/'data'


def test_hash_string():
    input_str = 'just a test'
    output_hash = hash_string('just a test')
    assert len(output_hash) == 40
    assert not input_str == output_hash


def test_can_load_template_file():
    res = load_template_file(str(DATA_ROOT / 'example1-deid-profile.yaml'))
    assert res is not None
    assert 'dicom' in res
    assert 'export' in res


def test_load_template_file_raises_with_path_issue():
    with pytest.raises(FileNotFoundError):
        load_template_file(str(DATA_ROOT / 'does-not-exist.yaml'))


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

