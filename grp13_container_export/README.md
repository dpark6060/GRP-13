# GRP-13 Anonymized/De-identified Export
Template-based anonymization and export of DICOM files within a container
(project/subject/session). DICOM files within the source project will be
anonymized (according to a required template) and exported to a specified project.

_NOTE:_ A requirement for DICOM metadata generation is that the destination
project has a gear rule configured for GRP-3, as the gear itself does not
propagate/modify DICOM metadata.

# Inputs
### deid_template (required)
This is a YAML file that describes the protocol for de-identifying
input_file. This file covers all of the same functionality of Flywheel
CLI de-identifiction.
An example deid_template.yaml looks like this:

``` yaml
# Configuration for DICOM de-identification
dicom:
  # What date offset to use, in number of days
  date-increment: -17

  # Set patient age from date of birth
  patient-age-from-birthdate: true
  # Set patient age units as Years
  patient-age-units: Y
  # Remove private tags
  remove-private-tags: true

  fields:
    # Remove a dicom field (e.g.remove PatientID)
    - name: PatientID
      remove: true

    # Replace a dicom field value (e.g. replace “StationName” with "XXXX")
    - name: StationName
      replace-with: XXXX

    # Increment a date field by -17 days
    - name: StudyDate
      increment-date: true

    # One-Way hash a dicom field to a unique string
    - name: AccessionNumber
      hash: true

    # One-Way hash the ConcatenationUID,
    # keeping the prefix (4 nodes) and suffix (2 nodes)
    - name: ConcatenationUID
      hashuid: true
```

__Extended Template Functionality:__ Three additional options have been introduced for this gear
(still unreleased for CLI):

1. Per GRP-13-03, the header tag can be referenced in a by tag location
as below:

    ```yaml
    dicom:
      fields:
        - name: PatientBirthDate
          remove: true
        - name: (0010, 0010)
          remove: true
        - name: '00100020'
          remove: true
    ```

    [migration_toolkit changes](https://gitlab.com/flywheel-io/public/migration-toolkit/merge_requests/39)


2. Per GRP-13-02, nested elements/sequences can be specified as below:

    ``` yaml
    name: nametag-profile
    description: Replace nested data element and test a few indexing variants
    dicom:
      fields:
        - name: AnatomicRegionSequence.0.CodeValue
          replace-with: 'new SH value'
        - name: '00082218.0.00080102'
          replace-with: 'new SH value'
        - name: AnatomicRegionSequence.0.00080104
          remove: true

    ```
    [migration_toolkit changes](https://gitlab.com/flywheel-io/public/migration-toolkit/merge_requests/40/diffs)

3. The `export` namepace can be used to whitelist metadata fields for
propagation where `info` is specifically for container.info fields and
`metadata` is a list for metadata that appear flat on the container
for example, subject.sex. This is demonstrated below:

    ``` yaml
    dicom:
      fields:
        - name: PatientBirthDate
          remove: true
    export:
      session:
        whitelist:
          info:
            - cats
          metadata:
            - operator
            - weight
      subject:
        whitelist:
          info:
            - cats
          metadata:
            - sex
            - strain
      acquisition: 
        whitelist:
          info: all
          metadata: all 
    ```
    With the above template subject.info.cats, subject.sex, subject.strain,
    session.info.cats, session.operator, session.weight, all 
    acquisition.info fields, and all editable acquisition metadata would 
    be propagated to the exported containers.
    
    _NOTE:_ metadata fields that can be propagated are defined by the dictionary
    below which reflects container metadata outside of info that can be
    updated via the SDK:

    ``` python
    META_WHITELIST_DICT = {
        'acquisition': ['timestamp', 'timezone', 'uid'],
        'subject': ['firstname', 'lastname', 'sex', 'cohort', 'ethnicity', 'race', 'species', 'strain'],
        'session': ['age', 'operator', 'timestamp', 'timezone', 'uid', 'weight']
    }
    ```
    
   

### subject_csv (optional)
The subject_csv facilitates subject-specific configuration of 
de-identification templates. This is a csv file that contains the column
`subject.code` with unique values correspond to the subject.code 
values in the project to be exported. If a subject in the project to be 
exported is not listed in `subject.code` in the provided subject_csv 
this subject will not be exported. 

#### Subject-level customization with subject_csv and deid_template

Requirements:
* To update subject fields, the fields must both be represented in the 
subject_csv and deid_template files. 
* If a field is represented in both the deid_template and the 
subject_csv, the value in the deid_template will be replaced with the 
value listed in the corresponding column of the subject_csv for each
subject that has a code listed in subject.code.
* Fields represented in the deid_template but not the subject_csv will 
be the same for all subjects. 
* subject.code can be modified by including an export.subject.code
column in subject_csv that contains unique values to be applied to 
subjects in the destination project.
* **NOTE: If you provide an `export.subject.code` column, PatientID in 
the DICOM headers will be set to the value in `export.subject.code` 
for each subject. If you provide a `dicom.fields.PatientID.replace-with` 
column alongside the `export.subject.code` column, 
`dicom.fields.PatientID.replace-with` will be ignored** 
* Conversely, if  `dicom.fields.PatientID.replace-with` is provided 
without `export.subject.code`, `dicom.fields.PatientID.replace-with` 
will be used to set PatientID in the DICOM headers and the new subject 
codes.

Let's walk through an example pairing of subject_csv and deid_template
to illustrate. 

The following table represents subject_csv (../tests/data/example-csv-mapping.csv):

|subject.code|dicom.date-increment|export.subject.code|dicom.fields.PatientBirthDate.remove|
|------------|--------------------|-------------------|------------------------------------|
|001         |-15                 |Patient_IDA        |false                               |
|002         |-20                 |Patient_IDB        |true                                |
|003         |-30                 |Patient_IDC        |true                                |

The deid_template:
``` yaml
dicom:
  # date-increment can be any integer value since dicom.date-increment is defined in example-csv-mapping.csv
  date-increment: -10
  # # since example-csv-mapping.csv doesn't define dicom.remove-private-tags, all subjects will have private tags removed
  remove-private-tags: true
  fields:
    - name: PatientBirthDate
      # remove can be any boolean since dicom.fields.PatientBirthDate.remove is defined in example-csv-mapping.csv
      remove: true
    - name: PatientID
      # replace-with can be any string value since export.subject.code is defined in example-csv-mapping.csv
      replace-with: FLYWHEEL
export:
  session:
    whitelist:
      info:
        - cats
      metadata:
        - operator
        - weight
  subject:
    # code can be any string value since export.subject.code is defined in example-csv-mapping.csv
    code: FLYWHEEL
    whitelist:
      info:
        - cats
      metadata:
        - sex
        - strain
```
The resulting template for subject 003 given the above would be: 
The deid_template:
``` yaml
dicom:
  # date-increment can be any integer value since dicom.date-increment is defined in example-csv-mapping.csv
  date-increment: -30
  remove_private_tags: true
  fields:
    - name: PatientBirthDate
      remove: true
    - name: PatientID
      replace-with: Patient_IDC 
export:
  session:
    whitelist:
      info:
        - cats
      metadata:
        - operator
        - weight
  subject:
    # code can be any string value since export.subject.code is defined in example-csv-mapping.csv
    code: Patient_IDC
    whitelist:
      info:
        - cats
      metadata:
        - sex
        - strain
```

### Manifest JSON for inputs
``` json
"inputs": {
    "api-key": {
      "base": "api-key"
    },
    "deid_template": {
      "base": "file",
      "description": "A Flywheel de-identification template specifying the de-identification actions to perform.",
      "optional": false,
      "type": {
        "enum": [
          "source code"
        ]
      }
    },
    "subject_csv": {
      "base": "file",
      "description": "A CSV file that contains mapping values to apply for subjects during de-identification.",
      "optional": true,
      "type": {
        "enum": [
          "source code"
        ]
      }
    }
}
```

## Configuration Options
### project_path (required)
The resolver path (<group>/<project>) to the destination project.
This project must exist AND the user running the gear must have
read/write access on this project.

### file_type
the type of files to de-identify/anonymize and export. Currently only
"dicom" is supported.

### overwrite_files (default = true)
If true, any existing files in the destination project that have been
exported previously will be overwritten so long as their parent container
has `info.export.origin_id` defined.

### Manifest JSON for configuration options
```json
"config": {
    "project_path": {
        "optional": false,
        "description": "The resolver path of the destination project, for example, flywheel/test",
        "type": "string"
    },
    "file_type": {
        "default": "dicom",
        "description": "the type of files to de-identify/anonymize and export",
        "type": "string",
        "enum": ["dicom"]
    },
    "overwrite_files": {
        "default": true,
        "description": "If true, existing files in destination containers will be overwritten if a file to  be exported shares their filename",
        "type": "boolean"
    }
}
```

# Workflow

1. User creates a destination project and enables gear rules. Importantly,
GRP-3 should be enabled to parse the de-identified DICOM headers. Further,
the permissions on this project should be restricted until the user
exporting the project has reviewed this project (after gear execution).
1. Within the source project, GRP-7 is used to modify container metadata in
preparation for export.
1. User runs GRP-13 (this analysis gear) at the project, subject, or session
level and provides the following:
    * Files:
        * A de-identification template specifying how to
        de-identify/anonymize the file
        * an optional csv that contains a column that maps to a
        Flywheel session or subject metadata field and columns that
        specify values with which to replace DICOM header tags
    * Configuration options:
        * The group_id/project name for the project to which to export
        anonymized files
        * The type of files to de-identify/anonymize (DICOM is currently the
          only value supported)
        * Whether to overwrite files if they already exist in the target
        project

1. The gear will find/create all subject/session/acquisition containers
associated with the destination analysis parent container.
    * <container>.info.export.origin_id is used to find containers in
    the export project
1. The gear will attempt to download all files of the specified type,
de-identify them per the template provided, and upload them to the
destination container

1. The  gear will then write an output file reporting the status files
that were exported. The gear will then exit.
