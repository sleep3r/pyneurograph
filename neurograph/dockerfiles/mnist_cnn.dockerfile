FROM python:3.7

WORKDIR /app

COPY ./requirements.txt /app
RUN pip install -r requirements.txt
COPY ./configuration/ /app/configuration
COPY ./utils/python/ /app/src
COPY ./src /app/src
COPY ./models/ /app/models

RUN bash -c 'mkdir -p /app/state'
RUN bash -c 'mkdir -p /app/state/${SERVICE_NAME}'

CMD python -u src/neurograph_${SERVICE_NAME}.py