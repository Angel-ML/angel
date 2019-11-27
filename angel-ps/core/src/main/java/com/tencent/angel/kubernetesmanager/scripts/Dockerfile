FROM spark:testing
RUN rm /opt/entrypoint.sh
COPY entrypoint.sh /opt
COPY angel /opt/angel
COPY lib /usr/lib
ENTRYPOINT ["/opt/entrypoint.sh"]
