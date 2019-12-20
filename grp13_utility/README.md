# GRP-13 File Anonymization/De-identification
DICOM Anonymization/De-identification 

## INPUTS
### input_file (required)
This is the file to de-identify/anonymize. Currently only DICOM is 
supported.

### deid_profile (required)
This is a JSON/YAML file that describes the protocol for de-identifying
input_file

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

### origin (optional)
origin is an optional string that will be added to 
`<parent container>.info.deid_origin`. This option is primarily for use 
by an export orchestration analysis gear

### Manifest JSON for configuration options
``` json
"config": {
    "output_filename": {
        "optional": false,
        "description": "The name to use for the output file (including extension). Cannot match the name of any file in gear destination container",
        "type": "string"
    },
    "origin": {
        "default": "",
        "description": "For SDK usages, please disregard",
        "type": "string"
    }

}
```