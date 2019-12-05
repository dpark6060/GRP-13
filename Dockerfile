FROM python:3.8-slim-buster

# Make directory for flywheel spec (v0)
ENV FLYWHEEL /flywheel/v0
WORKDIR ${FLYWHEEL}

# Pipenv
COPY Pipfile* /flywheel/v0/

RUN pip install pipenv==2018.11.26
RUN pipenv install --system --deploy