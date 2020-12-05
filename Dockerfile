FROM python:3
ADD mqtt.py /
RUN pip install paho-mqtt
RUN pip install psycopg2-binary
CMD [ "python", "./mqtt.py" ]
