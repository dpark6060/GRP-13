import flywheel
from deid_export.metadata_export import *


def test_get_whitelist_dict():
    test_dict = {'one': {'two': {'three': 3, 'five': 5}, 'spam': 'eggs'}, 'eggs': {'spam': 42}}
    expected_output = {'one': {'two': {'three': 3, 'five': 5}}, 'eggs': {'spam': 42}}
    output_dict = get_whitelist_dict(test_dict, ['one.two.three', 'eggs.spam', 'one.two', 'one.spam', 'python.snake'],
                                     blacklist=['one.spam'])
    assert output_dict == expected_output

    expected_output = {'one': {'two': {'three': 3}}, 'eggs': {'spam': 42}}
    output_dict = get_whitelist_dict(test_dict, ['one.two.three', 'eggs.spam', 'one.spam', 'one.two', 'python.snake'],
                                     blacklist=['one.spam', 'one.two.five'])
    assert output_dict == expected_output


def test_export_config_to_whitelist():
    export_config = {'whitelist': {'info': ['spam'], 'metadata': ['eggs']}}
    expected_output = ['info.spam', 'eggs']
    output = export_config_to_whitelist(export_config)
    assert output == expected_output
    export_config = {'whitelist': expected_output}
    output = export_config_to_whitelist(export_config)
    assert output == expected_output


def test_filter_metadata_list():
    input_list = ['python', 'classification.custom', 'info.python', 'modality']
    expected_output = ['classification.custom', 'info.python', 'modality']
    output = filter_metadata_list(container_type='file', metadata_list=input_list)
    assert output == expected_output
    assert filter_metadata_list(container_type='python', metadata_list=input_list) == list()


def test_get_container_metadata():
    info_dict = {'python': {'spam': 'eggs'}, 'header': {'dicom': {'PatientID': 'FLYWHEEL'}}}
    file = flywheel.FileEntry(type='file', id='test_id', info=info_dict, modality='MR')
    file._parent = flywheel.Acquisition(parents=flywheel.ContainerParents(project='project_id'))
    export_dict = {'file': {'whitelist': ['info.python.spam', 'modality', 'info.header.dicom.PatientID']}}
    expected_output = {'info': {'python': {'spam': 'eggs'},
                                'export': {'origin_id': util.hash_value('test_id', salt='project_id')}},
                       'modality': 'MR'}
    output = get_container_metadata(origin_container=file, export_dict=export_dict)
    assert output == expected_output
