# Source system of the CDC demo: PostgreSQL 16 with the wal2json logical decoding plugin
# (turns the write-ahead log into JSON change records).
FROM postgres:16
RUN apt-get update \
    && apt-get install -y --no-install-recommends postgresql-16-wal2json \
    && apt-get clean
