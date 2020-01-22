import pytest
from pathlib import Path
from deid_export.container_export import hash_string, load_template_file, quote_numeric_string, create_metadata_dict, \
    META_WHITELIST_DICT

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


def test_create_metadata_dict():
    mock_info = {
        'spam': 2,
        'eggs': 'yolk'
    }

    container_type_list = list(META_WHITELIST_DICT.keys())
    for container_type in container_type_list:
        mock_metadata = {'info': mock_info, 'id': 'e99ad2d8959a261b56f6162aa2171825a9907a2a'}
        for key in META_WHITELIST_DICT.get(container_type):
            mock_metadata[key] = 'Flywheel'

        # test partial info
        config = {
            'whitelist': {
                'info': ['spam'],
                'metadata': [key for key in mock_metadata.keys() if key != 'info']
            }
        }
        export_dict = {'origin_id': hash_string(mock_metadata.get('id'))}
        return_dict = {'info': {'spam': 2, 'export': export_dict}}
        for key, value in mock_metadata.items():
            if key not in ['info', 'id']:
                return_dict[key] = value
        output_dict = create_metadata_dict(mock_metadata, container_type=container_type, container_config=config)
        assert output_dict == return_dict

        # test all
        config = {'whitelist': {'info': 'all', 'metadata': 'all'}}
        return_dict = dict()
        for key, value in mock_metadata.items():
            if key not in ['id']:
                return_dict[key] = value
        return_dict['info']['export'] = export_dict
        output_dict = create_metadata_dict(mock_metadata, container_type=container_type, container_config=config)
        assert output_dict == return_dict

        # test none
        return_dict = {'info': {'export': export_dict}}
        output_dict = create_metadata_dict(mock_metadata, container_type=container_type)
        assert output_dict == return_dict