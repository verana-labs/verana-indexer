FROM postgres:13-alpine

# Copy init script
COPY ./psql_test_init.sh /docker-entrypoint-initdb.d/psql_test_init.sh

# Ensure script is executable (redundant but safe)
RUN chmod +x /docker-entrypoint-initdb.d/psql_test_init.sh
