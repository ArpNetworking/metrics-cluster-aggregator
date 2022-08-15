# Copyright 2016 Inscope Metrics, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM openjdk:17-slim-bullseye

MAINTAINER arpnetworking

EXPOSE 7065
EXPOSE 7066
WORKDIR /opt/cluster-aggregator

# Configuration
ENV CAGG_USER="cagg"
ENV CAGG_UID=1000
ENV CAGG_GID=1000
ENV JVM_XMS="64m"
ENV JVM_XMX="1024m"
ENV LOGBACK_CONFIG="-Dlogback.configurationFile=/opt/cluster-aggregator/config/logback.xml"
ENV CAGG_CONFIG="/opt/cluster-aggregator/config/config.conf"
ENV JAVA_OPTS=""

# Build
RUN apt update && apt install -y gosu && rm -rf /var/lib/apt/lists/* && \
    mkdir -p /opt/cluster-aggregator/lib/ext && \
    mkdir -p /opt/cluster-aggregator/logs && \
    mkdir -p /opt/cluster-aggregator/config
ADD deps /opt/cluster-aggregator/lib/
ADD bin /opt/cluster-aggregator/bin/
ADD config /opt/cluster-aggregator/config/
ADD lib /opt/cluster-aggregator/lib/

# Entry point
ENTRYPOINT [ "/opt/cluster-aggregator/bin/docker-entrypoint.sh" ]

# Default arguments
CMD [ \
    "-n", \
    "${CAGG_USER}", \
    "-u", \
    "${CAGG_UID}", \
    "-g", \
    "${CAGG_GID}", \
    "-d", \
    "/opt/cluster-aggregator", \
    "--", \
    "/opt/cluster-aggregator/bin/cluster-aggregator", \
    "${LOGBACK_CONFIG}", \
    "-XX:+HeapDumpOnOutOfMemoryError", \
    "-XX:HeapDumpPath=/opt/cluster-aggregator/logs/cluster-aggregator.oom.hprof", \
    "-Xlog:gc*:file=/opt/cluster-aggregator/logs/cluster-aggregator.gc.log:time,level,tags:filecount=2,filesize=50M", \
    "-Xms${JVM_XMS}", \
    "-Xmx${JVM_XMX}", \
    "-XX:+UseStringDeduplication", \
    "-XX:+UseG1GC", \
    "-Duser.timezone=UTC", \
    "--add-exports=java.naming/com.sun.jndi.ldap=ALL-UNNAMED", \
    "--add-opens=java.base/java.lang=ALL-UNNAMED", \
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED", \
    "--add-opens=java.base/java.io=ALL-UNNAMED", \
    "--add-opens=java.base/java.security=ALL-UNNAMED", \
    "--add-opens=java.base/java.util=ALL-UNNAMED", \
    "--add-opens=java.management/javax.management=ALL-UNNAMED", \
    "--add-opens=java.naming/javax.naming=ALL-UNNAMED", \
    "--", \
    "${CAGG_CONFIG}" ]
