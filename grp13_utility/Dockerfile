FROM python:3.8-slim-buster

# Make directory for flywheel spec (v0)
ENV FLYWHEEL /flywheel/v0/
WORKDIR ${FLYWHEEL}

# Pipenv
COPY Pipfile* ${FLYWHEEL}

RUN apt-get update && apt-get -y install git

RUN pip install pipenv==2018.11.26
RUN pipenv install --deploy --ignore-pipfile

COPY run.py ${FLYWHEEL}
COPY deid_file.py ${FLYWHEEL}