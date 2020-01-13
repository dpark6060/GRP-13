import contextlib
import datetime
import logging
import os
import re
import shutil

import tempfile

import flywheel

from .retry import retry
from .deid_file import deidentify_path


@contextlib.contextmanager
def make_temp_directory():
    temp_dir = tempfile.mkdtemp()
    try:
        yield temp_dir
    finally:
        shutil.rmtree(temp_dir)


def search_job_log_str(regex_string, job_log_str):
    if not job_log_str:
        return list()
    else:
        pattern = re.compile(regex_string)
        return pattern.findall(job_log_str)


def get_last_timestamp(job_log_str):
    JOB_LOG_TIME_STR_REGEX = '[\d]{4}\-[\d]{2}\-[\d]{2}\s[\d]{2}:[\d]{2}:[\d]{2}\.[\d]+'
    time_str_list = search_job_log_str(JOB_LOG_TIME_STR_REGEX, job_log_str)
    dt_list = [datetime.datetime.strptime(time, '%Y-%m-%d %H:%M:%S.%f') for time in time_str_list]
    # grp-13-deid-file logger logs UTC timestamp
    dt_list = [time_obj.replace(tzinfo=datetime.timezone.utc) for time_obj in dt_list]
    if not dt_list:
        return None
    else:
        return dt_list[-1]


def get_job_state_from_logs(job_log_obj,
                            previous_state='pending',
                            job_details=None,
                            current_time=datetime.datetime.now(datetime.timezone.utc),
                            max_seconds=500):
    """
    Parses job log to get information about state, leveraging log timestamps
    (configured to be UTC for grp-13-deid-file)
    :return:
    """
    # If job details were provided, get the state from them
    if isinstance(job_details, flywheel.models.job_detail.JobDetail):
        detail_state = job_details.get('state')
    else:
        detail_state = None

    job_log_str = ''.join([log_line.get('msg') for log_line in job_log_obj.logs]).replace('\n', ' ')

    # We don't want to update state if the job is already done
    if detail_state in ['complete', 'failed', 'cancelled']:
        state = detail_state
    elif previous_state in ['complete', 'failed', 'cancelled', 'failed_or_cancelled']:
        state = previous_state

    # If there are no logs, the job hasn't started yet
    elif not job_log_str:
        state = 'pending'

    # Completed jobs contain "Job complete."
    elif search_job_log_str('Job complete.', job_log_str):
        state = 'complete'

    # If contains 'Uploading results...' but is not complete, it is failed or cancelled.
    # (Can't tell which from log alone)
    elif search_job_log_str('Uploading results[\.]{3}', job_log_str):
        state = 'failed_or_cancelled'

    # If the log contains timestamps, but is none of the above, it's running
    # If the log's last timestamp is more than max_seconds from current_time, we'll consider it
    # "hanging"
    elif get_last_timestamp(job_log_str):
        delta_time = current_time - get_last_timestamp(job_log_str)
        if delta_time.total_seconds() > max_seconds:
            state = 'hanging'
        else:
            state = 'running'

    # For this specific gear, if it doesn't meet any of the above, but has printed 'Gear Name:',
    # we probably caught it before it started logging.
    elif search_job_log_str('Gear Name:', job_log_str):
        state = 'running'

    else:
        state = 'unknown'

    return state


class DeidUtilityJob:

    def __init__(self, job_id=None):
        self.id = None
        self.detail = None
        self.job_logs = None
        self.state = None
        self.forbidden = False
        if job_id:
            self.id = job_id

    @retry(max_retry=2)
    def submit_job(self, fw_client, gear_path, **kwargs):
        gear_obj = fw_client.lookup(gear_path)

        self.id = gear_obj.run(**kwargs)
        self.detail = fw_client.get_job_detail(self.id)
        self.job_logs = fw_client.get_job_logs(self.id)
        self.state = self.detail.state
        self.forbidden = False

    @retry(max_retry=3)
    def reload(self, fw_client, force=False):
        self.job_logs = fw_client.get_job_logs(self.id)

        # It's possible for a non-admin to lose the ability to check detail if sessions are moved
        # or permissions are changed, don't update the detail if this happens
        if not self.forbidden or force:
            try:
                self.detail = fw_client.get_job_detail(self.id)
            except flywheel.ApiException as e:
                if e.status == '403':
                    self.forbidden = True
                else:
                    raise e
        self.state = get_job_state_from_logs(
            job_log_obj=self.job_logs,
            previous_state=self.state,
            job_details=self.detail,
            max_seconds=120
        )

    @retry(max_retry=2)
    def cancel(self, fw_client):
        if self.state in ['pending', 'running', 'hanging', 'unknown'] or self.forbidden:
            try:
                fw_client.modify_job(self.id, {'state': 'cancelled'})
                self.state = 'cancelled'
                return self.state
            except flywheel.ApiException as e:
                # If the job is already cancelled, then the exception detail will be:
                #  "Cannot mutate a job that is <state>."
                done_job_str = "Cannot mutate a job that is "
                if done_job_str in e.detail:
                    job_state = e.detail.replace(done_job_str, '').replace('.', '')
                    self.state = job_state
                    return self.state
                else:
                    raise e
        else:
            return self.state


class FileExporter:
    """A class for representing the export status of a file"""
    @retry(max_retry=2)
    def __init__(self, fw_client, origin_parent, origin_filename, dest_parent, filename=None, overwrite=False):
        self.fw_client = fw_client
        self.origin_parent = origin_parent
        self.dest_parent = dest_parent
        self.origin_filename = origin_filename
        self.log = logging.getLogger(f'{self.origin_parent.id}_{self.origin_filename}_exporter')
        self.log.setLevel('DEBUG')
        self.state = 'initialized'
        self.overwrite = overwrite
        self.filename = filename
        self.deid_job = DeidUtilityJob()
        self.errors = list()
        # If a filename was not provided, default to the origin filename
        if not self.filename:
            self.filename = origin_filename

        self.origin = origin_parent.get_file(origin_filename)
        if not self.origin:
            self.error_handler(
                f'{self.origin_filename} does not exist in {self.origin_parent.container_type} {self.origin_parent.id}'
            )
        self.dest = self.dest_parent.get_file(filename)
        if self.dest and not self.overwrite:
            self.state = 'exists_at_destination'
        self.initial_state = self.state

    def error_handler(self, log_str):
        self.state = 'error'
        self.log.error(log_str, exc_info=True)
        self.errors.append(log_str)

    @retry(max_retry=2)
    def reload_fw_object(self, fw_object):
        fw_object = fw_object.reload()
        return fw_object

    @retry(max_retry=2)
    def reload(self):
        if self.state != 'error':
            try:
                self.reload_fw_object(self.origin_parent)
                self.reload_fw_object(self.dest_parent)
                self.dest = self.dest_parent.get_file(self.filename)
                if self.dest and self.state in ['pending', 'running', 'upload_attempted']:
                    if not self.overwrite:
                        self.state = 'exported'
                    else:
                        self.state = 'overwrite_exported'
                if self.deid_job.id:
                    self.deid_job = self.deid_job.reload(self.fw_client)
                    if self.state == 'pending':
                        if self.deid_job.state == 'cancelled':
                            self.state = 'cancelled'
                        elif self.deid_job.state in ['failed', 'failed_or_cancelled']:
                            log_str = (
                                f'De-id job failed. Please refer to the logs for job {self.deid_job.id} for '
                                f'{self.dest_parent.container_type} {self.dest_parent.id} '
                                'for additional details'
                            )
                            self.error_handler(log_str)
                    else:
                        pass
                self.log.debug(f'{self.filename} state is {self.state}')

            except Exception as e:

                log_str = f'An exception occurred while reloading {self.origin.id} ({self.filename}): {e}'
                self.error_handler(log_str)
            finally:
                return self

    def submit_deid_job(self, gear_path, template_file_obj):

        self.reload()
        if self.dest and not self.overwrite:
            log_str = (
                f'{self.filename} already exists in {self.dest_parent.container_type} {self.dest_parent.id}'
                f'{self.origin_filename} cannot be exported as {self.filename}'
            )
            self.error_handler(log_str)

        if self.deid_job.id:
            self.log.warning(
                f'Job already exists for {self.filename} ({self.deid_job.id}). A new one will not be queued'
            )
            return self.deid_job.id

        if self.state != 'error':

            job_dict = dict()
            job_dict['config'] = {'origin': self.origin.id, 'output_filename': self.filename}
            job_dict['inputs'] = {'input_file': self.origin, 'deid_profile': template_file_obj}
            job_dict['destination'] = self.dest_parent
            try:
                self.deid_job.submit_job(fw_client=self.fw_client, gear_path=gear_path, **job_dict)
            except Exception as e:
                log_str = (
                    f'An exception was raised while attempting to submit a job for {self.filename}: {e}'
                )
                self.error_handler(log_str)
            self.state = 'pending'
            return self.deid_job.id

    def cancel_deid_job(self):
        if not self.deid_job.id:
            self.log.debug('Cannot cancel a job that does not exist...')
            return None
        else:
            self.deid_job.cancel(self.fw_client)
            self.state = 'cancelled'

    @retry(3)
    def local_deid_export(self, template_path):
        self.reload()
        if self.dest and not self.overwrite:
            log_str = (
                f'{self.filename} already exists in {self.dest_parent.container_type} {self.dest_parent.id} '
                f'{self.origin_filename} cannot be exported as {self.filename}'
            )
            self.log.warning(log_str)
            self.state = 'exists_at_destination'
            return None
        if not os.path.exists(template_path):
            self.error_handler(
                f'De-id path {template_path} does not exist. {self.filename} will not be exported to '
                f'{self.dest_parent.id}'
            )
            return None
        try:

            with make_temp_directory() as temp_dir:
                # Download the file

                local_file_path = os.path.join(temp_dir, self.filename)
                self.log.debug(f'Downloading {self.origin.name} to {local_file_path}')
                self.origin.download(local_file_path)

                # De-identify
                self.log.debug(
                    f'Applying de-identfication template {os.path.basename(template_path)}'
                    f' to {os.path.basename(local_file_path)}'
                )
                deid_path = deidentify_path(input_file_path=local_file_path, profile_path=template_path)

                # Delete prior to upload if overwrite
                if os.path.exists(deid_path) and self.dest_parent.get_file(self.filename) and self.overwrite:
                    self.log.debug(f'deleting {self.filename} on {self.dest_parent.container_type} {self.dest_parent.id}')
                    self.dest_parent.delete_file(self.filename)

                self.log.debug(f'Uploading {self.filename} to {self.dest_parent.container_type} {self.dest_parent.id}')
                self.dest_parent.upload_file(deid_path)
                if self.overwrite and self.dest:
                    self.state = 'upload_attempted'


        except Exception as e:
            self.error_handler(
                f'An exception occured while attempting to de-identify {self.origin_filename} '
                f'{self.filename} will not be exported to {self.dest_parent.id} exception:\n{e}'
            )
            return None

    def get_status_dict(self):
        self.reload()
        status_dict = {
            'origin_filename': self.origin.name,
            'origin_parent': self.origin_parent.id,
            'origin_parent_type': self.origin_parent.container_type,
            'export_filename': self.filename,
            'export_file_id': None,
            'export_parent': self.dest_parent.id,
            'state': self.state,
            'errors': '\t'.join(self.errors)
        }
        if self.dest != None:
            status_dict['export_file_id'] = self.dest.id
        return status_dict




