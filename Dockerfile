FROM xdusongwei/python-rocksdb:3.8.3.6.10.1
RUN mkdir -p /app
COPY . /app
RUN cd /app && pip install -e .[test]