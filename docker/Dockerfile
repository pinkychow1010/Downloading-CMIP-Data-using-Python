FROM osgeo/gdal

ADD requirements.txt /

RUN apt -y update
RUN DEBIAN_FRONTEND=noninteractive apt -y install python3-pip python3-xarray

RUN pip install -r  /requirements.txt

ADD download_cmip.py /

ENV PYTHONUNBUFFERED=1

CMD [ "python", "./download_cmip.py" ]