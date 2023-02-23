ARG PYTHON_VERSION

FROM python:${PYTHON_VERSION}

WORKDIR /requirements
COPY requirements.txt .
RUN pip3 install -r requirements.txt

WORKDIR /app
