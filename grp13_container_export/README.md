# GRP-13 Anonymized/De-identified Export
This gear will perform Anonymization/De-identification on DICOM files
identically to the Flywheel CLI. Currently, a requirement for DICOM 
metadata generation is that the destination project has a gear rule 
configured for GRP-3, as the gear itself does not propagate/modify 
DICOM metadata

# Inputs 
### deid_template (required)
This is a JSON/YAML file that describes the protocol for de-identifying
input_file. This file covers all of the same functionality of Flywheel
CLI de-identifiction.
An example deid_template.yaml looks like this:

``` yaml
# Configuration for dicom de-identification 
dicom:
  # What date offset to use, in number of days
  date-increment: -17

  # Set patient age from date of birth
  patient-age-from-birthdate: true
  # Set patient age units as Years
  patient-age-units: Y
   
 
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

Three additional options have been introduced for this gear 
(still unreleased for CLI):

* Per GRP-13-03, the header tag can be referenced in a by tag location 
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
   
    
* Per GRP-13-02, nested elements/sequences can be specified as below:
(the current version appears to throw an exception if the Sequence 
is not present)

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

* The `export` namepace can be used to whitelist metadata fields for 
propagation where `info` is specifically for container.info fields and 
`metadata` is a list for metadata that appear flat on the container 
for example, subject.sex. 

This is demonstrated below:
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
```
With the above template subject.info.cats, subject.sex, subject.strain,
session.info.cats, session.operator and session.weight would be 
propagated to the exported containers.

NOTE: metadata fields that can be propagated are defined by the dictionary
below which reflects container metadata outside of info that can be 
updated via the SDK:

``` python
META_WHITELIST_DICT = {
    'acquisition': ['timestamp', 'timezone', 'uid'],
    'subject': ['firstname', 'lastname', 'sex', 'cohort', 'ethnicity', 'race', 'species', 'strain'],
    'session': ['age', 'operator', 'timestamp', 'timezone', 'uid', 'weight']
}
```
### Manifest JSON for inputs
``` json
"inputs": {
    "api-key": {
        "base": "api-key"
    },
    "deid_template": {
        "base": "file",
        "description": "A Flywheel de-identification template specifying the de-identification actions to perform on input_file",
        "optional": false,
        "type": {
            "enum": [
                "source code"
            ]
        }
    }
}
```
# Workflow

1. User creates a destination project and enables gear rules. Importantly, 
GRP-3 should be enabled to parse the de-identified DICOM headers. Further,
the permissions on this project should be restricted until the user
exporting the project has reviewed this project (after gear execution).
1. GRP-7 is used to modify container metadata in preparation for export
1. User runs this analysis gear at the project, session, or subject
level and provides the following inputs:
    * Files:
        * A de-identification template specifying how to 
        de-identify/anonymize the file
        * an optional csv that contains a column that maps to a 
        Flywheel session or subject metadata field and columns that 
        specify values with which to replace DICOM header tags 
        (proposed, not implemented)
        * if the above is provided, a mapping template YAML file must 
        also be provided to map the csv columns to Flywheel (similar to 
        query portion of GRP-5 template)
    * Configuration options:
        * The group_id/project name for the project to which to export 
        anonymized files 
        * The type of files to de-identify/anonymize 
        (dicom is currently the only value supported)
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







