import hashlib

import dotty_dict

META_WHITELIST_DICT = {
    'file': ('classification', 'info', 'modality', 'type'),
    'acquisition': ('timestamp', 'timezone', 'uid', 'info'),
    'subject': ('firstname', 'lastname', 'sex', 'cohort', 'ethnicity', 'race', 'species', 'strain', 'info'),
    'session': ('age', 'operator', 'timestamp', 'timezone', 'uid', 'weight', 'info')
}

BLACKLIST = {'info.header'}


def hash_string(input_str):
    """
    Hashes an input string using sha1
    Args:
        input_str (str): a string to be hashed

    Returns:
        (str): the output of sha1 hashing on the hexdigest of input_str
    """
    output_hash = hashlib.sha1(input_str.encode()).hexdigest()
    return output_hash


def get_whitelist_dict(input_dict, whitelist, blacklist=None):
    if blacklist is None:
        blacklist = BLACKLIST
    else:
        pass
    # dotty allows nested dictionaries to be represented as '.'-delimited string
    input_dotty_dict = dotty_dict.dotty(input_dict)
    output_dotty_dict = dotty_dict.dotty()
    # exclude keys that are not in the input dict or are in the blacklist
    key_list = [key for key in whitelist if key not in blacklist and key in input_dotty_dict]
    for key in key_list:
        output_dotty_dict[key] = input_dotty_dict[key]

    # handle potential parents of whitelist fields in blacklist
    for bl_key in blacklist:
        if bl_key in output_dotty_dict:
            del output_dotty_dict[bl_key]

    return output_dotty_dict.to_dict()


def export_config_to_whitelist(export_config):
    whitelist_obj = export_config.get('whitelist')
    whitelist = list()
    # allow for separate handling of info and metadata
    if isinstance(whitelist_obj, dict):
        info_list = whitelist_obj.get('info')
        metdadata_list = whitelist_obj.get('metadata')
        if isinstance(info_list, list):
            whitelist.extend([f'info.{key}' for key in info_list])
        if isinstance(metdadata_list, list):
            whitelist.extend(metdadata_list)
    # allow all fields provided as a list with info fields prefixed with 'info.'
    elif isinstance(whitelist, list):
        whitelist.extend(whitelist_obj)
    else:
        pass
    return whitelist


def filter_metadata_list(container_type, metadata_list, metadata_wl_dict=None):
    if metadata_wl_dict is None:
        metadata_wl_dict = META_WHITELIST_DICT
    container_wl = metadata_wl_dict.get(container_type)
    if isinstance(container_wl, (tuple, list)):
        starts_with_tup = tuple([field + '.' for field in container_wl])
        metadata_list = [field for field in metadata_list if field.startswith(starts_with_tup) or field in container_wl]
    else:
        metadata_list = list()
    return metadata_list


def get_container_metadata(origin_container, export_dict):
    container_type = origin_container.get('container_type')
    container_config = export_dict.get(container_type)
    metadata_dot_dict = dotty_dict.dotty()
    if isinstance(container_config, dict):
        metadata_list = export_config_to_whitelist(container_config)
        if metadata_list:
            metadata_list = filter_metadata_list(container_type=container_type, metadata_list=metadata_list)
        if metadata_list:
            metadata_dict = get_whitelist_dict(input_dict=origin_container, whitelist=metadata_list)
            if metadata_dict:
                metadata_dot_dict = dotty_dict.dotty(metadata_dict)
    origin_container_id = origin_container.get('id') or origin_container.get('_id')
    metadata_dot_dict['info.export.origin_id'] = hash_string(origin_container_id)
    output_dict = metadata_dot_dict.to_dict()
    return output_dict
