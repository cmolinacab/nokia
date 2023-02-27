FROM python:3.8-slim



RUN mkdir /app
WORKDIR /app
ADD . /app/
RUN pip3 install -r requirements.txt

EXPOSE 8051
CMD ["python3", "/app/Nokia_Viewer_main_v1_0.py"]
