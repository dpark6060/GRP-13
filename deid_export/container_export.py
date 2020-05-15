#!/usr/bin/env python3
from dataclasses import dataclass
import argparse
import hashlib
import json
import logging
import os
import re
import time
import tempfile
import signal
import sys

import joblib
import pandas as pd
import flywheel
import yaml

from deid_export.retry import retry
from deid_export.metadata_export import get_container_metadata
from deid_export.file_exporter import FileExporter
from deid_export import deid_template
from flywheel_migration import deidentify, util

log = logging.getLogger(__name__)
log.setLevel('INFO')


def matches_file(deid_profile, file_obj):
    """

    Args:
        deid_profile(flywheel_migration.deidentify.DeIdProfile): the de-identification profile
        file_obj(flywheel.FileEntry or dict): the flywheel file object

    Returns:
        bool: whether the profile supports the file

    """
    return_bool = False
    file_type = file_obj.get('type')
    file_name = file_obj.get('name')
    if file_type == 'dicom' and deid_profile.get_file_profile('dicom'):
        return_bool = True
    else:
        for profile in deid_profile.file_profiles:
            if profile.name != 'dicom' and profile.matches_file(file_name):
                return_bool = True
                break

    return return_bool


def load_template_dict(template_file_path):
    """
    Determines whether the file at template_file_path is JSON or YAML and returns the Python dictionary representation
    Args:
        template_file_path (str): path to the JSON or YAML file
    Raises:
        ValueError: when fails to load the template
    Returns:
        (dict): dictionary representation of the the template file

    """
    _, ext = os.path.splitext(template_file_path.lower())

    template = None
    try:
        if ext == '.json':
            with open(template_file_path, 'r') as f:
                template = json.load(f)
        elif ext in ['.yml', '.yaml']:
            deid_template.update_deid_profile(template_file_path,
                                              dict(),
                                              dest_path=template_file_path)
            with open(template_file_path, 'r') as f:
                template = yaml.load(f, Loader=yaml.FullLoader)
        return template
    except ValueError:
        log.exception(f'Unable to load template at: {template_file_path}')

    if not template:
        raise ValueError(f'Could not load template at: {template_file_path}')


def get_api_key_from_client(fw_client):
    """
    Parses the api key from an instance of the flywheel client
    Args:
        fw_client (flywheel.Client): an instance of the flywheel client

    Returns:
        (str): the api key
    """
    site_url = fw_client.get_config().site.get('api_url').rsplit(':', maxsplit=1)[0]
    site_url = site_url.rsplit('/', maxsplit=1)[1]
    key_string = fw_client.get_current_user().api_key.key
    api_key = ':'.join([site_url, key_string])
    return api_key


def quote_numeric_string(input_str):
    """Wraps a numeric string in double quotes. Attempts to coerce non-str to str and logs a warning.

    Args:
        input_str (str): string to be modified (if numeric string)

    Returns:
        str: A numeric string wrapped in quotes if input_str is numeric, or str(input_str)
    """

    if not isinstance(input_str, str):
        log.warning(f'Expected {input_str} to be a string. Is type: {type(input_str)}. Attempting to coerce to str...')
        input_str = str(input_str)
    if re.match(r'^[\d]+[\.]?[\d]*$', input_str):
        output_str = f'"{input_str}"'
    else:
        output_str = input_str
    return output_str


def find_or_create_subject(origin_subject, dest_proj, export_config=None):
    """
    Searches the destination project for a subject with code matching origin_subject.code (or 'code' from subject_config
        if provided). If found, the subject metadata is updated to match the whitelisted metadata of origin_subject.
        Otherwise, a new subject is created with metadata matching the whitelisted metadata for origin_subject.
    Args:
        origin_subject (flywheel.Subject): the subject to export
        dest_proj(flywheel.Project): the project in which to search/create the subject
        export_config (dict): an optional dictionary specifying metadata whitelists and container codes/labels

    Returns:
        (flywheel.Subject): the found or created subject in dest_proj
    """
    origin_subject = origin_subject.reload()
    dest_proj = dest_proj.reload()
    if not export_config:
        export_config = {'subject': {}}
    subject_config = export_config.get('subject', {})
    new_code = subject_config.get('code', origin_subject.code)
    query_code = quote_numeric_string(new_code)

    # Since subject code must be unique within a project, we do not need to search by info.export.origin_id
    dest_subject = dest_proj.subjects.find_first(f'code={query_code}')
    # Copy over metadata as specified
    meta_dict = get_container_metadata(origin_container=origin_subject, export_dict=export_config)

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


def find_or_create_subject_session(origin_session, dest_subject, export_config=None):
    """
    Searches the destination subject (dest_subject) for a session with with label matching origin_session.label
        (or 'label' from session_config, if provided) and info.export.origin_id = util.hash_value(origin_session.id)
        If found, the destination session metadata is updated to match the whitelisted metadata of origin_session.
        Otherwise, a new subject is created with metadata matching the whitelisted metadata for origin_session.
    Args:
        origin_session (flywheel.Session): the session to be exported
        dest_subject (flywheel.Subject): the subject to which to export the session
        export_config (dict): an optional dictionary specifying metadata whitelists and container codes/labels

    Returns:
        (flywheel.Session): the found or created session in dest_subject
    """
    origin_session = origin_session.reload()
    dest_subject = dest_subject.reload()
    if not export_config:
        export_config = {'session': {}}
    session_config = export_config.get('session', {})
    new_label = session_config.get('label', origin_session.label)
    query = (
        f'label={quote_numeric_string(new_label)},'
        f'info.export.origin_id="{util.hash_value(origin_session.id, salt=origin_session.parents.project)}"'
    )
    dest_session = dest_subject.sessions.find_first(query)
    # Copy over metadata as specified
    meta_dict = get_container_metadata(origin_container=origin_session, export_dict=export_config)
    if not dest_session:
        log.debug(f'Creating destination session for ({origin_session.id})')
        # Add session to subject
        dest_session = dest_subject.add_session(label=new_label, **meta_dict)
    else:
        log.debug(f'Using destination session ({dest_session.id})')
        dest_session.update(meta_dict)
        dest_session = dest_session.reload()
    return dest_session


def find_or_create_session_acquisition(origin_acquisition, dest_session, export_config=None):
    """
    Searches the destination session (dest_session) for an acquisition with label matching origin_acquisition.label
        (or 'label' from acquisition_config, if provided) and info.export.origin_id = util.hash_value(origin_acquisition.id)
        If found, the destination acquisition metadata is updated to match the whitelisted metadata of
        origin_acquisition. Otherwise, a new subject is created with metadata matching the whitelisted metadata for
        origin_acquisition.
    Args:
        origin_acquisition (flywheel.Acquisition): the acquisition to be exported
        dest_session (flywheel.Session): the session to which to export the acquisition
        export_config (dict): an optional dictionary specifying metadata whitelists and container codes/labels

    Returns:
        (flywheel.Acquisition): the found or created acquisition in dest_session
    """
    origin_acquisition = origin_acquisition.reload()
    dest_session = dest_session.reload()
    if not export_config:
        export_config = {'acquisition': {}}
    query = (
        f'label={quote_numeric_string(origin_acquisition.label)},'
        f'info.export.origin_id="{util.hash_value(origin_acquisition.id, salt=origin_acquisition.parents.project)}"'
    )
    dest_acquisition = dest_session.acquisitions.find_first(query)
    # Copy over metadata as specified
    meta_dict = get_container_metadata(origin_container=origin_acquisition, export_dict=export_config)
    if not dest_acquisition:
        log.debug(f'Creating destination acquisition for ({origin_acquisition.id})')

        # Add acquisition to session
        dest_acquisition = dest_session.add_acquisition(label=origin_acquisition.label, **meta_dict)
    else:
        log.debug(f'Using destination acquisition ({dest_acquisition.id})')
        dest_acquisition.update(meta_dict)
        dest_acquisition.reload()
    return dest_acquisition


def initialize_container_file_export(fw_client, deid_profile, origin_container, dest_container, overwrite=False,
                                     config=None):
    """
    Initializes a list of FileExporter objects for the origin_container/dest_container combination

    Args:
        config: the export configuration dictionary
        deid_profile(flywheel_migration.deidentify.DeIdProfile): the de-identification profile
        fw_client (fw.Client): an instance of the flywheel client
        origin_container (flywheel.<Container>): the container with files to be exported
        dest_container (flywheel.<Container>): the container to which files are to be exported
        overwrite (bool): whether to overwrite files that currently exist in dest_container

    Returns:
        (list): list of FileExporter objects
    """
    file_exporter_list = list()
    for container_file in origin_container.files:

        if matches_file(deid_profile, container_file):
            log.debug(
                f'Initializing {origin_container.container_type} {origin_container.id} file {container_file.name}')
            tmp_file_exporter = FileExporter(fw_client=fw_client, origin_parent=origin_container,
                                             origin_filename=container_file.name, dest_parent=dest_container,
                                             overwrite=overwrite, config=config)
            file_exporter_list.append(tmp_file_exporter)
        else:
            log.debug('Ignoring file %s, as it does not have a matching template', container_file.name)
            continue

    return file_exporter_list


class SessionExporter:

    def __init__(self, fw_client, template_dict, origin_session, dest_proj_id,
                 dest_container_id=None):
        self.client = fw_client
        self.deid_profile, self.export_config = deid_template.load_deid_profile(template_dict)

        self.origin_project = fw_client.get_project(origin_session.project)
        self.dest_proj = fw_client.get_project(dest_proj_id)
        self.origin = origin_session.reload()
        # self.log = logging.getLogger(f'{self.origin.id}_exporter')

        self.errors = list()
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
                export_config=self.export_config
            )
        return self.dest_subject

    def find_or_create_dest(self):
        if not self.dest:
            if not self.dest_subject:
                self.find_or_create_dest_subject()

            self.dest = find_or_create_subject_session(
                origin_session=self.origin,
                dest_subject=self.dest_subject,
                export_config=self.export_config
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
                export_config=self.export_config
            )

        self.dest.reload()

    def initialize_files(self, subject_files=False, project_files=False, overwrite=False):
        log.debug(f'Initializing {self.origin.id} files')
        if not self.dest:
            self.dest = self.find_or_create_dest()

        # project files
        if project_files is True:
            proj_file_list = initialize_container_file_export(deid_profile=self.deid_profile,
                                                              fw_client=self.client,
                                                              origin_container=self.origin_project.reload(),
                                                              dest_container=self.dest_proj.reload(),
                                                              config=self.export_config,
                                                              overwrite=overwrite)
            self.files.extend(proj_file_list)

        # subject files
        if subject_files is True:
            subj_file_list = initialize_container_file_export(deid_profile=self.deid_profile,
                                                              fw_client=self.client,
                                                              origin_container=self.origin.subject.reload(),
                                                              dest_container=self.dest.subject.reload(),
                                                              config=self.export_config,
                                                              overwrite=overwrite)
            self.files.extend(subj_file_list)

        # session files
        sess_file_list = initialize_container_file_export(deid_profile=self.deid_profile,
                                                          fw_client=self.client, origin_container=self.origin,
                                                          dest_container=self.dest.reload(),
                                                          config=self.export_config,
                                                          overwrite=overwrite)
        self.files.extend(sess_file_list)

        self.origin = self.origin.reload()
        # acquisition files
        for origin_acq in self.origin.acquisitions():
            origin_acq = origin_acq.reload()

            dest_acq = find_or_create_session_acquisition(
                origin_acquisition=origin_acq,
                dest_session=self.dest,
                export_config=self.export_config
            )
            tmp_acq_file_list = initialize_container_file_export(deid_profile=self.deid_profile,
                                                                 fw_client=self.client, origin_container=origin_acq,
                                                                 dest_container=dest_acq,
                                                                 config=self.export_config,
                                                                 overwrite=overwrite)
            self.files.extend(tmp_acq_file_list)

        return self.files

    def local_file_export(self):
        # De-identify
        for file_exporter in self.files:
            if file_exporter.state != 'error':
                file_exporter.deidentify(self.deid_profile)
        fname_dict = dict()
        for file_exporter in self.files:
            if file_exporter.filename:
                if file_exporter.dest_parent.id not in fname_dict.keys():
                    fname_dict[file_exporter.dest_parent.id] = [file_exporter.filename]
                else:
                    if file_exporter.filename not in fname_dict.get(file_exporter.dest_parent.id):
                        fname_dict[file_exporter.dest_parent.id].append(file_exporter.filename)

                    else:
                        file_exporter.error_handler(
                            f'Cannot upload {file_exporter.filename} ({file_exporter.origin.id}) to '
                            f'{file_exporter.dest_parent.id} because another file has already been uploaded with '
                            'the same name. Please use filename output strings that will create unique filenames in'
                            ' your de-identification template.'
                        )

        for file_exporter in self.files:
            file_exporter.reload()
            if file_exporter.state != 'error' and file_exporter.filename:
                file_exporter.upload()
        for file_exporter in self.files:
            file_exporter.reload()
            if file_exporter.state == 'upload_attempted':
                file_exporter.update_metadata()

        dict_list = [file_exporter.get_status_dict() for file_exporter in self.files]
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
    template = load_template_dict(template_path)
    origin_session = fw_client.get_session(origin_session_id)

    session_exporter = SessionExporter(
        fw_client=fw_client,
        origin_session=origin_session,
        dest_proj_id=dest_proj_id,
        template_dict=template
    )

    session_exporter.initialize_files(subject_files=subject_files, project_files=project_files, overwrite=overwrite)
    session_export_df = session_exporter.local_file_export()
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


def get_session_error_df(fw_client, session_obj, error_msg, deid_profile, project_files=False,
                         subject_files=False):

    session_obj = session_obj.reload()
    status_dict_list = list()

    def _append_file_status_dicts(parent_obj):
        for file_obj in parent_obj.files:
            if deid_profile.matches_file(file_obj.name):
                status_dict = {
                    'origin_filename': file_obj.name,
                    'origin_parent': parent_obj.id,
                    'origin_parent_type': parent_obj.container_type,
                    'export_filename': None,
                    'export_file_id': None,
                    'export_parent': None,
                    'state': 'error',
                    'errors': error_msg
                }
                status_dict_list.append(status_dict)

    # Handle project files
    if project_files:
        project_obj = fw_client.get_project(session_obj.project)
        _append_file_status_dicts(project_obj)
    # Handle subject files
    if subject_files:
        subject_obj = session_obj.subject.reload()
        _append_file_status_dicts(subject_obj)
    # Handle session files
    _append_file_status_dicts(session_obj)
    # Handle acquisition files
    for acquisition_obj in session_obj.acquisitions():
        acquisition_obj = acquisition_obj.reload()
        _append_file_status_dicts(acquisition_obj)

    session_df = pd.DataFrame(status_dict_list)
    return session_df


# TODO: incorporate filetype list
def export_container(fw_client, container_id, dest_proj_id, template_path, csv_output_path=None,
                     overwrite=False, subject_csv_path=None,
                     new_code_loc=deid_template.DEFAULT_NEW_SUBJECT_LOC,
                     old_code_col=deid_template.DEFAULT_SUBJECT_CODE_COL):
    container = fw_client.get(container_id).reload()

    template_obj = None
    df = None
    error_count = 0
    # template_obj = load_template_dict(template_path)

    if subject_csv_path and template_path:
        df = deid_template.validate(deid_template_path=template_path, csv_path=subject_csv_path,
                                    subject_label_col=old_code_col,
                                    new_subject_label_loc=new_code_loc)

    def _export_session(session_id, session_template_path, project_files=False,
                        subject_files=False, sess_error_msg=None):

        # if not, session template could not be generated no need to dig into the individual files
        df_count = 0
        if session_template_path:

            template_dict = load_template_dict(session_template_path)

            if sess_error_msg:
                sess_deid_profile, exp_dict = deid_template.load_deid_profile(template_dict)
                session_obj = fw_client.get_session(session_id)
                session_df = get_session_error_df(fw_client=fw_client, session_obj=session_obj, error_msg=sess_error_msg,
                                                  deid_profile=sess_deid_profile)
            else:
                session_df = export_session(
                    fw_client=fw_client,
                    origin_session_id=session_id,
                    dest_proj_id=dest_proj_id,
                    template_path=session_template_path,
                    subject_files=subject_files,
                    project_files=project_files,
                    csv_output_path=None,
                    overwrite=overwrite)
            df_count = session_df['state'].value_counts().get('error', 0)

            if isinstance(session_df, pd.DataFrame):
                if csv_output_path and not os.path.isfile(csv_output_path) and (len(session_df) >= 1):
                    session_df.to_csv(csv_output_path, index=False)
                elif csv_output_path and os.path.isfile(csv_output_path) and (len(session_df) >= 1):
                    session_df.to_csv(csv_output_path, mode='a', header=False, index=False)
        return df_count

    def _get_subject_template(subject_obj, directory_path):
        subj_template_path = os.path.join(directory_path, f'{subject_obj.id}_{os.path.basename(template_path)}')
        try:
            subj_template_path = deid_template.get_updated_template(
                df=df,
                deid_template_path=template_path,
                subject_code=subject_obj.code,
                subject_code_col=old_code_col,
                dest_template_path=subj_template_path)
            error_msg = None
        except ValueError as e:
            error_msg = f'Could not create subject template for {subject.code}: {e}'
            subj_template_path = None
            log.info(error_msg)
        except Exception as e:
            error_msg = f'An exception occurred when creating subject template for {subject.code}: {e}'
            subj_template_path = None
            log.error(error_msg, exc_info=True)
        return subj_template_path, error_msg

    def _export_subject(subject_obj, project_files=False):
        subject_error_count = 0
        subj_error_msg = None
        with tempfile.TemporaryDirectory() as temp_dir:
            subj_template_path = template_path
            if isinstance(df, pd.DataFrame):
                subj_template_path, subj_error_msg = _get_subject_template(subject_obj=subject_obj,
                                                                           directory_path=temp_dir)
            subject_files = True
            for session in subject_obj.sessions():
                sess_count = _export_session(session_id=session.id, session_template_path=subj_template_path,
                                             project_files=project_files, subject_files=subject_files,
                                             sess_error_msg=subj_error_msg)
                subject_error_count += sess_count
                subject_files = False
                project_files = False
        return subject_error_count

    if container.container_type not in ['subject', 'project', 'session']:
        raise ValueError(f'Cannot load container type {container.container_type}. Must be session, subject, or project')

    elif container.container_type == 'project':
        project_files = True
        for subject in container.subjects():
            subj_error_count = _export_subject(subject_obj=subject, project_files=project_files)
            error_count += subj_error_count
            project_files = False

    elif container.container_type == 'subject':
        project_files = False
        error_count = _export_subject(subject_obj=container, project_files=project_files)

    elif container.container_type == 'session':
        session_export_error = None
        with tempfile.TemporaryDirectory() as temp_dir:
            sess_template_path = template_path
            if isinstance(df, pd.DataFrame):
                sess_template_path, session_export_error = _get_subject_template(subject_obj=container.subject,
                                                                                 directory_path=temp_dir)
            error_count = _export_session(session_id=container_id, session_template_path=sess_template_path,
                                          sess_error_msg=session_export_error)

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
    parser.add_argument('--subject_csv_path', help='path to the subject csv', default=None)
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
        overwrite=args.overwrite_files,
        subject_csv_path=args.subject_csv_path
    )
