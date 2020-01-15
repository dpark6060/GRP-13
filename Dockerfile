FROM python:3.8-slim-buster

# Make directory for flywheel spec (v0)
ENV FLYWHEEL /flywheel/v0/
ENV GEAR /flywheel/work
WORKDIR ${FLYWHEEL}

# Pipenv
COPY Pipfile* ${FLYWHEEL}/

RUN apt-get update && apt-get -y install git

RUN pip install pipenv==2018.11.26
RUN pipenv install --deploy --ignore-pipfile

ADD deid_export ${GEAR}/deid_export
ADD grp13_utility ${GEAR}/grp13_utility
ADD grp13_container_export ${GEAR}/grp13_container_export