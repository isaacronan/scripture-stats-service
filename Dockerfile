FROM python:3
WORKDIR /usr/src/app
COPY . /usr/src/app
RUN pip install pymongo
RUN pip install numpy
RUN pip install flask
RUN pip install gunicorn
CMD gunicorn --bind 0.0.0.0:8020 wsgi
