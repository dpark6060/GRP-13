# GRP-13 File Anonymization/De-identification
DICOM Anonymization/De-identification 

## INPUTS
### input_file (required)
This is the file to de-identify/anonymize. Currently only DICOM is 
supported.

### deid_profile (required)
This is a JSON/YAML file that describes the protocol for de-identifying
input_file. This file covers all of the same functionality of Flywheel
CLI de-identifiction.
An example config.yaml looks like this:

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

Two additional options have been introduced for this gear 
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

### Manifest JSON for inputs
``` json
"inputs": {
    "api-key": {
        "base": "api-key",
        "read-only": true
    },
    "input_file": {
        "base": "file",
        "description": "A file to be de-identified",
        "optional": false,
        "type": {
            "enum": [
                "dicom"
            ]
        }
    },
    "deid_profile": {
        "base": "file",
        "description": "A Flywheel de-identification specifying the de-identification actions to perform on input_file",
        "optional": false,
        "type": {
            "enum": [
                "source code"
            ]
        }
    }
}
```

## Configuration Options

### output_filename (required)
This is the full filename (including extension) that will be used for 
the anonymized/de-identified output file. If any characters in this 
string match the regex `[^A-Za-z0-9\-\_\.]+` (characters that are 
not alphanumeric, '.', '-', or '_'), they will be removed. If no valid 
characters are provided, the gear will exit with a "Failed" status.

### force_overwrite (optional)
force_overwrite is a boolean configuration option

If the destination container already contains a file with 
`output_filename`:

* `true` will allow the gear job  to upload the file output and 
replace the current file in the destination container if the job
completes successfully.


* `false` (default)will exit with a failure status without 
de-identifying+exporting the file

### Manifest JSON for configuration options
``` json
"config": {
    "output_filename": {
        "optional": false,
        "description": "The name to use for the output file (including extension). Cannot match the name of any file in gear destination container",
        "type": "string"
    },
    "force_overwrite": {
        "default": false,
        "description": "If true, a pre-existing file with name output_filename will be overwritten",
        "type": "boolean"
    }
}
```