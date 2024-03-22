# FROM python:3.11-slim
FROM continuumio/miniconda3:23.3.1-0
ENV PYTHONUNBUFFERED 1
RUN yes | conda install -c conda-forge ta-lib

WORKDIR /workdir

RUN apt-get update && apt-get install -y git
RUN apt-get install -y g++

# RUN pipe cache purge
RUN pip3 install --upgrade pip
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY . .
# ENV FLASK_APP=app
# ENV PYTHONPATH= ${PYTHONPATH}:/workdir/appdir

EXPOSE 5000

CMD [ "python3", "-m" , "flask", "run", "--host=0.0.0.0"]