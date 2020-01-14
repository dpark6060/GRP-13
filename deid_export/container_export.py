#!/usr/bin/env python3
from dataclasses import dataclass
import argparse
import hashlib
import json
import logging
import os
import re
import time
import signal
import sys

import joblib
import pandas as pd
import flywheel
import yaml

from deid_export.retry import retry
from deid_export.file_exporter import FileExporter

META_WHITELIST_DICT = {
    'acquisition': ['timestamp', 'timezone', 'uid'],
    'subject': ['firstname', 'lastname', 'sex', 'cohort', 'ethnicity', 'race', 'species', 'strain'],
    'session': ['age', 'operator', 'timestamp', 'timezone', 'uid', 'weight']
}

log = logging.getLogger(__name__)
log.setLevel('INFO')


def hash_string(input_str):
    output_hash = hashlib.sha1(input_str.encode()).hexdigest()
    return output_hash


def load_template_file(template_file_path):
    """Load the de-identification template at template_file_path"""
    _, ext = os.path.splitext(template_file_path.lower())

    config = None
    try:
        if ext == '.json':
            with open(template_file_path, 'r') as f:
                template = json.load(f)
        elif ext in ['.yml', '.yaml']:
            with open(template_file_path, 'r') as f:
                template = yaml.load(f, Loader=yaml.FullLoader)
        return template
    except ValueError:
        log.exception(f'Unable to load template at: {template_file_path}')

    if not config:
        raise ValueError(f'Could not load template at: {template_file_path}')


def get_api_key_from_client(fw_client):
    site_url = fw_client.get_config().site.get('api_url').rsplit(':', maxsplit=1)[0]
    site_url = site_url.rsplit('/', maxsplit=1)[1]
    key_string = fw_client.get_current_user().api_key.key
    api_key = ':'.join([site_url, key_string])
    return api_key


def quote_numeric_string(input_str):
    """
    Wraps a numeric string in double quotes. Attempts to coerce non-str to str and logs a warning.
    :param input_str: string to be modified (if numeric string - matches ^[\d]+$)
    :type input_str: str
    :return: output_str, a numeric string wrapped in quotes if input_str is numeric, or str(input_str)
    :rtype str
    """

    if not isinstance(input_str, str):
        log.warning(f'Expected {input_str} to be a string. Is type: {type(input_str)}. Attempting to coerce to str...')
        input_str = str(input_str)
    if re.match(r'^[\d]+[\.]?[\d]*$', input_str):
        output_str = f'"{input_str}"'
    else:
        output_str = input_str
    return output_str

# TODO: allow an ALL option to be passed
def create_metadata_dict(origin_container, container_config=None):
    # Ensure our container is up-to-date
    origin_container = origin_container.reload()
    # Initialize the dictionary
    meta_dict = dict()

    # Initialize empty lists
    meta_wl = list()
    info_wl = list()

    # Parse whitelisted fields from the config
    if isinstance(container_config, dict):
        if isinstance(container_config.get('whitelist'), dict):
            whitelist_dict = container_config.get('whitelist')
            if isinstance(whitelist_dict.get('metadata'), list):
                meta_wl = whitelist_dict.get('metadata')
            if isinstance(whitelist_dict.get('info'), list):
                meta_wl = whitelist_dict.get('info')

    # If info in its entirety is to be copied, do this before adding export information
    if 'info' in meta_wl:
        meta_dict['info'] = origin_container.info
    else:
        meta_dict['info'] = dict()
    # set info.export.origin_id for record-keeping
    meta_dict['info']['export'] = {'origin_id': hash_string(origin_container.id)}
    meta_whitelist = META_WHITELIST_DICT.get(origin_container.container_type)
    # Copy info fields
    for key, value in origin_container.items():
        if (key in info_wl) and (key not in meta_dict['info'].keys()):
            meta_dict['info'][key]: value
    # Copy non-info fields
    for item in meta_wl:
        if item in meta_whitelist and getattr(origin_container, item) and (item not in meta_dict.keys()):
            meta_dict[item] = origin_container.get(item)

    return meta_dict


def find_or_create_subject(origin_subject, dest_proj, subject_config=None):
    origin_subject = origin_subject.reload()
    dest_proj = dest_proj.reload()
    if not subject_config:
        subject_config = dict()
    new_code = subject_config.get('code', origin_subject.code)
    query_code = quote_numeric_string(new_code)

    # Since subject code must be unique within a project, we do not need to search by info.export.origin_id
    dest_subject = dest_proj.subjects.find_first(f'code={query_code}')
    # Copy over metadata as specified
    meta_dict = create_metadata_dict(origin_subject, subject_config)

    if not dest_subject:
        log.debug(f'Creating destination subject for ({origin_subject.id})')

        # Add the subject to the destination project
        new_subject = dest_proj.add_subject(code=new_code, label=new_code, **meta_dict)

        # Reload the newly-created container
        dest_subject = new_subject.reload()
    else:
        log.debug(f'Using destination subject ({dest_subject.id})')
        dest_subject.update(meta_dict)
        dest_subject.reload()
    return dest_subject


def find_or_create_subject_session(origin_session, dest_subject, session_config=None):
    origin_session = origin_session.reload()
    dest_subject = dest_subject.reload()
    if not session_config:
        session_config = dict()
    new_label = session_config.get('label', origin_session.label)
    query = (
        f'label={quote_numeric_string(new_label)},'
        f'info.export.origin_id="{hash_string(origin_session.id)}"'
    )
    dest_session = dest_subject.sessions.find_first(query)
    # Copy over metadata as specified
    meta_dict = create_metadata_dict(origin_session, session_config)
    if not dest_session:
        log.debug(f'Creating destination session for ({origin_session.id})')
        # Copy over metadata as specified
        meta_dict = create_metadata_dict(origin_session, session_config)
        # Add session to subject
        dest_session = dest_subject.add_session(label=new_label, **meta_dict)
    else:
        log.debug(f'Using destination session ({dest_session.id})')
        dest_session.update(meta_dict)
        dest_session = dest_session.reload()
    return dest_session


def find_or_create_session_acquisition(origin_acquisition, dest_session, acquisition_config=None):
    origin_acquisition = origin_acquisition.reload()
    dest_session = dest_session.reload()
    if not acquisition_config:
        acquisition_config = dict()
    query = (
        f'label={quote_numeric_string(origin_acquisition.label)},'
        f'info.export.origin_id="{hash_string(origin_acquisition.id)}"'
    )
    dest_acquisition = dest_session.acquisitions.find_first(query)
    # Copy over metadata as specified
    meta_dict = create_metadata_dict(origin_acquisition, acquisition_config)
    if not dest_acquisition:
        log.debug(f'Creating destination acquisition for ({origin_acquisition.id})')
        # Copy over metadata as specified
        meta_dict = create_metadata_dict(origin_acquisition, acquisition_config)
        # Add acquisition to session
        dest_acquisition = dest_session.add_acquisition(label=origin_acquisition.label, **meta_dict)
    else:
        log.debug(f'Using destination acquisition ({dest_acquisition.id})')
        dest_acquisition.update(meta_dict)
        dest_acquisition.reload()
    return dest_acquisition


def initialize_container_file_export(fw_client,
                                     origin_container,
                                     dest_container,
                                     filename_dict,
                                     filetype_list,
                                     overwrite=False):
    file_exporter_list = list()
    for container_file in origin_container.files:
        export_filename = filename_dict.get(container_file.name, None)

        if container_file.type in filetype_list:
            log.debug(f'Initializing {origin_container.container_type} {origin_container.id} file {container_file.name}')
            tmp_file_exporter = FileExporter(
                fw_client=fw_client,
                origin_parent=origin_container,
                origin_filename=container_file.name,
                dest_parent=dest_container,
                filename=export_filename,
                overwrite=overwrite
            )
            file_exporter_list.append(tmp_file_exporter)
    return file_exporter_list


def local_file_export(api_key, file_exporter_dict, template_path, overwrite=False):
    fw_client = flywheel.Client(api_key, skip_version_check=True)
    file_exporter = FileExporter(
        fw_client=fw_client,
        origin_parent=fw_client.get(file_exporter_dict.get('origin_parent')),
        origin_filename=file_exporter_dict.get('origin_filename'),
        dest_parent=fw_client.get(file_exporter_dict.get('export_parent')),
        filename=file_exporter_dict.get('export_filename'),
        overwrite=overwrite
    )
    if file_exporter.state != 'error':
        file_exporter.local_deid_export(template_path=template_path)
    time.sleep(2)
    status_dict = file_exporter.get_status_dict()
    del file_exporter

    return status_dict


class SessionExporter:

    def __init__(self, fw_client, origin_session, dest_proj_id, export_config=None, dest_container_id=None):
        self.client = fw_client
        if not isinstance(export_config, dict):
            export_config = dict()
        self.export_config = export_config
        self.origin_project = fw_client.get_project(origin_session.project)
        self.dest_proj = fw_client.get_project(dest_proj_id)
        self.origin = origin_session.reload()
        #self.log = logging.getLogger(f'{self.origin.id}_exporter')

        self.errors = list()
        self.file_types = export_config.get('file_types', ['dicom'])
        self.files = list()
        self.dest_subject = None

        # use dest_container_id if it's been provided
        if dest_container_id:
            self.dest = fw_client.get_session(dest_container_id)
            self.dest_subject = self.dest.subject.reload()
        else:
            self.dest = None

    def find_or_create_dest_subject(self):

        if not self.dest_subject:
            self.dest_subject = find_or_create_subject(
                origin_subject=self.origin.subject,
                dest_proj=self.dest_proj,
                subject_config=self.export_config.get('subject', None)
            )
        return self.dest_subject

    def find_or_create_dest(self):
        if not self.dest:
            if not self.dest_subject:
                self.find_or_create_dest_subject()

            self.dest = find_or_create_subject_session(
                origin_session=self.origin,
                dest_subject=self.dest_subject,
                session_config=self.export_config.get('session', None)
            )
        return self.dest

    def find_or_create_acquisitions(self):

        if not self.dest:
            self.find_or_create_dest()
        self.origin = self.origin.reload()
        for acquisition in self.origin.acquisitions():
            find_or_create_session_acquisition(
                origin_acquisition=acquisition,
                dest_session=self.dest,
                acquisition_config=self.export_config.get('acquisition', None)
            )

        self.dest.reload()

    def initialize_files(self, subject_files=False, project_files=False, filename_dict=None):
        log.debug(f'Initializing {self.origin.id} files')
        if not isinstance(filename_dict, dict):
            filename_dict = dict()
        if not self.dest:
            self.dest = self.find_or_create_dest()

        # project files
        if project_files == True:
            proj_file_list = initialize_container_file_export(
                fw_client=self.client,
                origin_container=self.origin_project.reload(),
                dest_container=self.dest_proj.reload(),
                filename_dict=filename_dict,
                filetype_list=self.file_types
            )
            self.files.extend(proj_file_list)

        # subject files
        if subject_files == True:
            subj_file_list = initialize_container_file_export(
                fw_client=self.client,
                origin_container=self.origin.subject.reload(),
                dest_container=self.dest.subject.reload(),
                filename_dict=filename_dict,
                filetype_list=self.file_types
            )
            self.files.extend(subj_file_list)

        # session files
        sess_file_list = initialize_container_file_export(
            fw_client=self.client,
            origin_container=self.origin,
            dest_container=self.dest.reload(),
            filename_dict=filename_dict,
            filetype_list=self.file_types
        )
        self.files.extend(sess_file_list)

        self.origin = self.origin.reload()
        # acquisition files
        for origin_acq in self.origin.acquisitions():
            origin_acq = origin_acq.reload()

            dest_acq = find_or_create_session_acquisition(
                origin_acquisition=origin_acq,
                dest_session=self.dest,
                acquisition_config=self.export_config.get('acquisition', None)
            )
            tmp_acq_file_list = initialize_container_file_export(
                fw_client=self.client,
                origin_container=origin_acq,
                dest_container=dest_acq,
                filename_dict=filename_dict,
                filetype_list=self.file_types
            )
            self.files.extend(tmp_acq_file_list)

        return self.files

    def local_file_export(self, template_path, overwrite=False):

        file_list = [file_exporter.get_status_dict() for file_exporter in self.files]

        api_key = get_api_key_from_client(self.client)
        dict_list = joblib.Parallel(n_jobs=-2)(joblib.delayed(local_file_export)(
            api_key=api_key,
            file_exporter_dict=file_exporter,
            template_path=template_path, overwrite=overwrite) for file_exporter in file_list)
        export_df = pd.DataFrame(dict_list)

        del dict_list
        return export_df

    def get_status_df(self):
        if not self.files:
            return None
        else:
            status_df = pd.DataFrame([file_exporter.get_status_dict() for file_exporter in self.files])
            return status_df


# TODO: Allow files to be exported without template
def export_session(
        fw_client,
        origin_session_id,
        dest_proj_id,
        template_path,
        subject_files=False,
        project_files=False,
        csv_output_path=None,
        overwrite=False):
    template = load_template_file(template_path)
    export_config = template.get('export', dict())
    origin_session = fw_client.get_session(origin_session_id)

    session_exporter = SessionExporter(
        fw_client=fw_client,
        origin_session=origin_session,
        dest_proj_id=dest_proj_id,
        export_config=export_config
    )

    session_exporter.initialize_files(subject_files=subject_files, project_files=project_files)
    session_export_df = session_exporter.local_file_export(template_path=template_path, overwrite=overwrite)
    if len(session_export_df) >= 1:
        if csv_output_path:
            session_export_df.to_csv(csv_output_path, index=False)

        if session_export_df['state'].all() == 'error':
            log.error(
                f'Failed to export all {origin_session_id} files.'
                f' Please check template {os.path.basename(template_path)}'
            )
        return session_export_df
    else:
        return None


# TODO: incorporate filetype list
def export_container(fw_client, container_id, dest_proj_id, template_path,
                     csv_output_path=None, overwrite=False, project_files=False, subject_files=False):
    container = fw_client.get(container_id).reload()

    error_count = 0
    if container.container_type not in ['subject', 'project', 'session']:
        raise ValueError(f'Cannot load container type {container.container_type}. Must be session, subject, or project')

    elif container.container_type == 'project':
        project_files = True
        for subject in container.subjects():
            sub_count = export_container(fw_client=fw_client, container_id=subject.id, dest_proj_id=dest_proj_id,
                                         template_path=template_path, csv_output_path=csv_output_path,
                                         overwrite=overwrite, project_files=project_files, subject_files=subject_files)
            error_count += sub_count
            project_files = False

    elif container.container_type == 'subject':
        subject_files = True

        for session in container.sessions():
            sess_count = export_container(fw_client=fw_client, container_id=session.id, dest_proj_id=dest_proj_id,
                                          template_path=template_path, csv_output_path=csv_output_path,
                                          overwrite=overwrite, project_files=project_files, subject_files=subject_files)
            error_count += sess_count
            # We only need to copy subject/project files once
            subject_files = False
            project_files = False

    elif container.container_type == 'session':
        session_df = export_session(
            fw_client=fw_client,
            origin_session_id=container_id,
            dest_proj_id=dest_proj_id,
            template_path=template_path,
            subject_files=False,
            project_files=False,
            csv_output_path=None,
            overwrite=overwrite)
        df_count = session_df['state'].value_counts().get('error', 0)
        error_count += df_count
        if isinstance(session_df, pd.DataFrame):
            if csv_output_path and not os.path.isfile(csv_output_path) and (len(session_df) >= 1):
                session_df.to_csv(csv_output_path, index=False)
            elif csv_output_path and os.path.isfile(csv_output_path) and (len(session_df) >= 1):
                session_df.to_csv(csv_output_path, mode='a', header=False, index=False)

    log.info(f'Export for {container.container_type} {container.id} is complete with {error_count} file export errors')
    return error_count


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('origin_container_path', help='Resolver path of the container to export')
    parser.add_argument('project_path', help='Resolver path of the project to which to export')
    parser.add_argument('template_path', help='Local path of the de-identification template')
    parser.add_argument('--csv_output_path', help='path to which to write the output csv')
    parser.add_argument('--api_key', help='Use if not logged in via cli')
    parser.add_argument('--overwrite_files',
                        help='Overwrite existing files in the destination project where present',
                        action='store_true')
    args = parser.parse_args()
    if args.api_key:
        fw = flywheel.Client(args.api_key)
    else:
        fw = flywheel.Client()

    dest_project = fw.lookup(args.project_path)
    origin_container = fw.lookup(args.origin_container_path)
    csv_output_path = os.path.join(os.getcwd(), f'{origin_container.container_type}_{origin_container.id}_export.csv')
    if args.csv_output_path:
        csv_output_path = args.csv_output_path

    export_container(
        fw_client=fw,
        container_id=origin_container.id,
        dest_proj_id=dest_project.id,
        template_path=args.template_path,
        csv_output_path=csv_output_path,
        overwrite=args.overwrite_files
    )

