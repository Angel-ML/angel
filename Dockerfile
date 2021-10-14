########################################################################################################################
#                                                       DEV                                                            #
########################################################################################################################
FROM maven:3.6.1-jdk-8 as DEV

##########################
#  install dependencies  #
##########################
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       curl=7.52.1-5+deb9u9 \
       g++=4:6.3.0-4 \
       make=4.1-9.1 \
       unzip=6.0-21+deb9u1 \
    && rm -rf /var/lib/apt/lists/*

#######################
#  install protobuf 2.5.0  #
#######################
RUN curl -fsSL --insecure -o /tmp/protobuf-2.5.0.tar.gz https://github.com/protocolbuffers/protobuf/releases/download/v2.5.0/protobuf-2.5.0.tar.gz \
    && tar -xzf /tmp/protobuf-2.5.0.tar.gz -C /tmp \
    && rm -rf /tmp/protobuf-2.5.0.tar.gz  \
    && mv /tmp/protobuf-* /tmp/protobuf \
    && cd /tmp/protobuf \
    && ./configure \
    && make -j4 \
    && make install \
    && rm -rf /tmp/protobuf

ENV PATH /usr/local/bin/:$PATH
ENV LD_LIBRARY_PATH /usr/local/lib:$LD_LIBRARY_PATH

########################################################################################################################
#                                                     JAVA BUILDER                                                     #
########################################################################################################################
FROM DEV as JAVA_BUILDER

WORKDIR /app

COPY ./ /app

RUN mvn -e -B -Dmaven.test.skip=true package

########################################################################################################################
#                                                       Artifacts                                                      #
########################################################################################################################
FROM alpine:3.10 as ARTIFACTS

WORKDIR /dist
COPY --from=JAVA_BUILDER /app/dist/target/*.zip ./

VOLUME /output

CMD [ "/bin/sh", "-c", "cp ./* /output" ]