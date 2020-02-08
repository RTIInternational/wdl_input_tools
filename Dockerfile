# Base Image
FROM python:3.7.6-slim-stretch

# Metadata
LABEL base.image="wdl_input_tools:latest"
LABEL version="1"
LABEL software="QCParser"
LABEL software.version="latest"
LABEL description="Bioinformatics utility for managing batch execution of WDL workflows through a Cromwell server"

# Maintainer
MAINTAINER Alex Waldrop <awaldrop@rti.org>

# update the OS related packages
RUN apt-get update

# install required dependencies for QCParser
RUN pip install -r requirements.txt

# get the QCParser from GitHub
RUN mkdir /opt/wdl_input_tools
RUN ADD . /opt/wdl_input_tools

ENV PATH /opt/wdl_input_tools:$PATH

CMD ["ls -l", "/opt/wdl_input_tools/*.py"]