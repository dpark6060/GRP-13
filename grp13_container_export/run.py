import logging
import os

import flywheel
from deid_export import container_export

log = logging.getLogger(__name__)
log.setLevel('INFO')


def get_analysis_parent(fw_client, container_id):
    """Return parent container id of the analysis container provided
    Args:
        fw_client (flywheel.Client): An instance of the Flywheel client
        container_id (str): A flywheel analysis container id

    Returns:
        (object or None): The container object or None if an exception is raised retrieving the container
    """
    try:
        container = fw_client.get(container_id)
        container_parent = fw_client.get(container.parent.id)
        log.info(f'Destination analysis {container.id} parent is a {container_parent.container_type} with '
                 f'id {container_parent.id}')
        return container_parent
    except Exception as e:
        log.error(e, exc_info=True)
        return None


def lookup_project(fw_client, project_resolver_path):
    """Attempts to lookup and return the project at the resolver path.

    If the path is not for a project or an exception is raised, will return None

    Args:
        fw_client (flywheel.Client): An instance of the Flywheel client
        project_resolver_path (str): Path to project

    Returns:
        object: The project at the resolver path

    """
    try:
        project = fw_client.lookup(project_resolver_path)
        if project.container_type != 'project':
            log.error(f'{project.container_type} {project.id} is not a project!')
            return None
        else:
            return project
    except flywheel.ApiException as e:
        log.error(e, exc_info=True)
        log.error(f'could not retrieve a project at {project_resolver_path}')
        return None


def parse_args_from_context(gear_context):
    # Confirm existence of destination project
    project_path = gear_context.config.get('project_path')
    project = lookup_project(gear_context.client, project_path)
    destination_id = gear_context.destination.get('id')

    origin = get_analysis_parent(gear_context.client, destination_id)

    # Return None if we failed to get the origin or destination project
    if not project or not origin:
        return None

    template_path = gear_context.get_input('deid_template')['location']['path']
    csv_output_path = os.path.join(gear_context.output_dir, f'{origin.id}_export.csv')
    overwrite_files = gear_context.config.get('overwrite_files')

    export_container_args = {
        'fw_client': gear_context.client,
        'container_id': origin.id,
        'dest_proj_id': project.id,
        'template_path': template_path,
        'csv_output_path': csv_output_path,
        'overwrite': overwrite_files,
        'subject_csv_path': None
    }

    # Check for subject_csv
    if gear_context.get_input('subject_csv'):
        subject_csv_path = gear_context.get_input('subject_csv')['location']['path']
        if os.path.exists(subject_csv_path):
            export_container_args['subject_csv_path'] = subject_csv_path
    return export_container_args


def main(gear_context):
    export_args = parse_args_from_context(gear_context)
    if not export_args:
        log.error('Exiting...')
        return 1
    else:
        error_count = container_export.export_container(**export_args)
        if os.path.isfile(export_args.get('csv_output_path')) and error_count == 0:
            return 0
        else:
            return 1


if __name__ == '__main__':
    with flywheel.GearContext() as gear_context:
        exit_status = main(gear_context)
    log.info(f'exit_status is {exit_status}')
    os._exit(exit_status)
