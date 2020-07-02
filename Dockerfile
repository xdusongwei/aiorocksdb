FROM xdusongwei/python-rocksdb:3.8.3.6.10.2
RUN mkdir -p /app
COPY . /app
WORKDIR /app
RUN pip install -e .[test]
