# GRP-13 Anonymized/De-identified Export

## Workflow

1. User creates a destination project and enables gear rules. Importantly, 
GRP-3 should be enabled. 
1. User runs this analysis gear at the project, session, or subject
level and provides the following inputs:
    * Files:
        * A de-identification template specifying how to 
        de-identify/anonymize the file
        * an optional csv that contains a column that maps to a 
        Flywheel session or subject metadata field and columns that 
        specify values with which to replace DICOM header tags
        * if the above is provided, a mapping template YAML file must 
        also be provided to map the csv columns to Flywheel (similar to 
        query portion of GRP-5 template)
    * Configuration options:
        * The group_id/project name for the project to which to export 
        anonymized files 
1. All child subject/session/acquisition containers associated with the 
destination analysis parent container and the project if one does not 
exist at the input path provided.

1. The gear will continuously check for metadata on the destination 
containers that indicates that the job for the files have been 
completed. 

1. Once the above metadata is present on all destination containers 
(meaning that all jobs have finished), the gear will write an output 
file reporting the status files that were exported. The gear will then 
exit.







