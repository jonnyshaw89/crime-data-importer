FROM python:3.6-alpine

ENV AWS_DEFAULT_REGION eu-west-1

RUN pip install pipenv

COPY Pipfile Pipfile
COPY Pipfile.lock Pipfile.lock

RUN pipenv install --deploy --system

COPY . /app

ENTRYPOINT python /app/importer.py