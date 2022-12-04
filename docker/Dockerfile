FROM osgeo/gdal

ADD requirements.txt /

RUN apt-get update
RUN apt-get install -y software-properties-common && apt-get update
RUN apt-get -y install python3-dev
RUN apt-get -y install -y gdal-bin libgdal-dev
ARG CPLUS_INCLUDE_PATH=/usr/include/gdal
ARG C_INCLUDE_PATH=/usr/include/gdal

RUN set -xe \
    && apt-get -y update \
    && apt-get -y install python3-pip
RUN pip install --upgrade pip

RUN pip install -r  /requirements.txt

ADD download_cmip.py /

ENV PYTHONUNBUFFERED=1

CMD [ "python", "./download_cmip.py" ]