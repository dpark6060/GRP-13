[![CircleCI](https://circleci.com/gh/flywheel-apps/GRP-13.svg?style=svg)](https://circleci.com/gh/flywheel-apps/GRP-13)

# GRP-13 Anonymized/De-identified Export 

This repository hosts two gears for de-identifiying and exporting a Flywheel project.  
Refer to the following READMEs for more information on their use:

* [README.md](https://github.com/flywheel-apps/GRP-13/tree/master/grp13_container_export)
for the project level gear
* [README.md](https://github.com/flywheel-apps/GRP-13/tree/master/grp13_utility)
for the utility gear 

# Building

To build the docker image for that gear, from within `grp13_container_export`, 
run `docker build -t <image>:<tag> ..`