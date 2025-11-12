FROM builder

ENV DB2INST1_PASSWORD=testpwd
ENV DBNAME=test
ENV DB2_HOME=/opt/ibm/db2/V12.1

COPY ./rpms/* /rpms/
COPY start.sh /

RUN dnf install -y /rpms/*.rpm && \
    mkdir /pgdata && \
    chown postgres: /pgdata

USER	postgres

CMD   ./start.sh
