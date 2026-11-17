# syntax=docker/dockerfile:1
#
# Apache Iceberg REST catalog, backed by the stack's Postgres instead of the image's
# default SQLite. The official image ships only the SQLite JDBC driver and starts with
# `java -jar` (which ignores the classpath), so we add the Postgres driver and launch the
# same main class with an explicit classpath.

# curl + busybox sha1sum/sha512sum, no package install (keeps the build off apk mirrors)
FROM curlimages/curl:8.11.1 AS jars
ARG PG_JDBC=42.7.14
ARG PG_JDBC_SHA1=0e7c94ce80cb1dd9875ff44ffe6a40a8aaa3eefd
RUN curl -fsSLo /tmp/postgresql.jar \
      "https://repo1.maven.org/maven2/org/postgresql/postgresql/${PG_JDBC}/postgresql-${PG_JDBC}.jar" \
 && echo "${PG_JDBC_SHA1}  /tmp/postgresql.jar" | sha1sum -c -

FROM apache/iceberg-rest-fixture:1.10.1
COPY --from=jars /tmp/postgresql.jar /usr/lib/iceberg-rest/postgresql.jar
CMD ["java", "-cp", "iceberg-rest-adapter.jar:postgresql.jar", "org.apache.iceberg.rest.RESTCatalogServer"]
