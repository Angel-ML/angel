FROM openjdk:8-alpine

ARG angel_dist_path=./angel-3.0.0
ARG entrypoint_path=./entrypoint.sh

RUN set -ex && \
    apk upgrade --no-cache && \
    apk add --no-cache bash tini libc6-compat linux-pam && \
    mkdir -p /opt/ && \
    rm /bin/sh && \
    ln -sv /bin/bash /bin/sh && \
    echo "auth required pam_wheel.so use_uid" >> /etc/pam.d/su && \
    chgrp root /etc/passwd && chmod ug+rw /etc/passwd

COPY ${angel_dist_path} /opt/angel
COPY ${entrypoint_path} /opt/

ENV ANGEL_HOME /opt/angel

WORKDIR /opt/angel/work-dir

ENTRYPOINT [ "/opt/entrypoint.sh" ]
