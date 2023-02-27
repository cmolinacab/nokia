FROM python:3.8-slim

RUN mkdir /app
WORKDIR /app
ADD . /app/
RUN pip3 install -r requirements.txt

CMD ["python3", "-u", "/app/LTE_700_Reports_main_SERIAL.py"]
