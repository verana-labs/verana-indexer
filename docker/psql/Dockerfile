FROM postgres:16-alpine
COPY ./psql_test_init.sh /docker-entrypoint-initdb.d/
RUN chmod +x /docker-entrypoint-initdb.d/psql_test_init.sh
ENV POSTGRES_INITDB_ARGS="--auth-host=scram-sha-256"