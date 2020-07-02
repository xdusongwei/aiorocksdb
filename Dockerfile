FROM xdusongwei/python-rocksdb:3.8.3.6.10.2
RUN mkdir -p /home/travis/build/xdusongwei/aiorocksdb
COPY . /home/travis/build/xdusongwei/aiorocksdb
WORKDIR /home/travis/build/xdusongwei/aiorocksdb
RUN pip install -e .[test]
