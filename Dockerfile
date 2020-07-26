FROM xdusongwei/python-rocksdb:3.8-buster-6.11.4
RUN mkdir -p /home/travis/build/xdusongwei/aiorocksdb
COPY . /home/travis/build/xdusongwei/aiorocksdb
WORKDIR /home/travis/build/xdusongwei/aiorocksdb
RUN pip install --quiet -e .[test]
