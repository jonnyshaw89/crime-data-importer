FROM python:3.6

ENV AWS_DEFAULT_REGION eu-west-1

RUN pip install pipenv && \
    pip install pip==18.0

COPY Pipfile Pipfile
COPY Pipfile.lock Pipfile.lock

RUN pipenv install --deploy --system

COPY . /app

ENTRYPOINT python /app/importer.py