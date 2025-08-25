FROM postgres:14-alpine
COPY ./psql_test_init.sh /docker-entrypoint-initdb.d/
RUN chmod +x /docker-entrypoint-initdb.d/psql_test_init.sh