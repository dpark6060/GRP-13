# GRP-13 Anonymized/De-identified Export

## Workflow

1. User creates a destination project and enables gear rules. Importantly, 
GRP-3 should be enabled to parse the de-identified DICOM headers. 
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







