# Base Image
FROM python:3.7.6-slim-stretch

# Metadata
LABEL base.image="wdl_input_tools:latest"
LABEL version="1"
LABEL software="wdl_input_tools"
LABEL software.version="latest"
LABEL description="Bioinformatics utility for managing batch execution of WDL workflows through a Cromwell server"

# Maintainer
MAINTAINER Alex Waldrop <awaldrop@rti.org>

# update the OS related packages
RUN apt-get update

# install required dependencies for QCParser
ADD requirements.txt .
RUN pip install -r requirements.txt && rm requirements.txt

# get the QCParser from GitHub
RUN mkdir /opt/wdl_input_tools
ADD . /opt/wdl_input_tools
RUN chmod 755 /opt/wdl_input_tools/*.py

ENV PATH /opt/wdl_input_tools:$PATH

CMD ["ls -l", "/opt/wdl_input_tools/*.py"]