FROM ubuntu_gq:16.04

COPY ./sources.list /etc/apt/sources.list

COPY ./tail /etc/resolvconf/resolv.conf.d/tail

COPY ./libibverbs1_1.2.1mlnx1-OFED.4.0.0.1.3.40101_amd64.deb /tmp/libibverbs.deb

COPY ./libibverbs-dev_1.2.1mlnx1-OFED.4.0.0.1.3.40101_amd64.deb /tmp/libibverbs-dev.deb

RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential cmake libnl-3-200 libnl-route-3-200 && \
    apt-get clean && \
    dpkg -i /tmp/libibverbs.deb && \
    dpkg -i /tmp/libibverbs-dev.deb && \
    rm -rf /var/lib/apt/lists/* && \
    rm -rf /tmp/libibverbs* 
