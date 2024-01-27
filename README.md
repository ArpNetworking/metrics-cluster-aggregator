Metrics Cluster Aggregator
==========================

[![License:Apache 2](https://img.shields.io/hexpm/l/plug.svg)](https://raw.githubusercontent.com/ArpNetworking/metrics-cluster-aggregator/master/LICENSE)
[![Build Status](https://build.arpnetworking.com/job/ArpNetworking/job/metrics-cluster-aggregator/job/master/badge/icon)](https://build.arpnetworking.com/job/ArpNetworking/job/metrics-cluster-aggregator/job/master/)
[![Maven Artifact](https://img.shields.io/maven-central/v/com.arpnetworking.metrics/metrics-cluster-aggregator.svg)](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22com.arpnetworking.metrics%22%20a%3A%22metrics-cluster-aggregator%22)

Reaggregates statistics received from multiple Metric Aggregator Daemon instances into aggregates across each cluster.  Simply, this means combining the values from each host in your fleet.  Both the host and cluster values are published to various configurable data sinks.


Usage
-----

### Installation

#### Manual
The artifacts from the build are in *metrics-cluster-aggregator/target/appassembler* and should be copied to an appropriate directory on your application host(s).

#### Docker
If you use Docker, we publish a base docker image that makes it easy for you to layer configuration on top of.  Create a Docker image based on the image arpnetworking/cluster-aggregator.  Configuration files are typically located at /opt/cluster-aggregator/config/.  In addition, you can specify CONFIG_FILE (defaults to /opt/cluster-aggregator/config/config.json), PARAMS (defaults to $CONFIG_FILE), LOGGING_CONFIG (defaults to "-Dlogback.configurationFile=/opt/cluster-aggregator/config/logback.xml"), and JAVA_OPTS (defaults to $LOGGING_CONFIG) environment variables to control startup.

### Execution

In the installation's *bin* directory there are scripts to start Metrics Cluster Aggregator: *cluster-aggregator* (Linux) and *cluster-aggregator.bat* (Windows).  One of these should be executed on system start with appropriate parameters; for example:

    /usr/local/lib/metrics-cluster-aggregator/bin/cluster-aggregator /usr/local/lib/metrics-cluster-aggregator/config/config.json

### Configuration

#### Logging

To customize logging you may provide a [LogBack](http://logback.qos.ch/) configuration file.  To use a custom logging configuration you need to define and export an environment variable before executing *cluster-aggregator*:

    export JAVA_OPTS="-Dlogback.configurationFile=/usr/local/lib/metrics-cluster-aggregator/config/logger.xml"

Where */usr/local/lib/metrics-cluster-aggregator/config/logger.xml* is the path to your logging configuration file.

#### Daemon

The Metrics Cluster Aggregator configuration is specified in a JSON file.  The location of the configuration file is passed to *metrics-cluster-aggregator* as a command line argument:

    /usr/local/etc/metrics-cluster-aggregator/config/prod.conf

The configuration specifies:

* logDirectory - The location of additional logs.  This is independent of the logging configuration.
* clusterPipelineConfiguration - The location of configuration file for the cluster statistics pipeline.
* hostPipelineConfiguration - The location of the configuration file for the host statistics pipeline.
* httpHost - The ip address to bind the http server to.
* httpPort - The port to bind the http server to.
* aggregationHost - The ip address to bind the tcp aggregation server to.
* aggregationPort - The port to bind the tcp aggregation server to.
* maxConnectionTimeout - The maximum aggregation server client connection timeout in ISO-8601 period notation.
* minConnectionTimeout - The minimum aggregation server client connection timeout in ISO-8601 period notation.
* jvmMetricsCollectionInterval - The JVM metrics collection interval in ISO-8601 period notation.
* rebalanceConfiguration - Configuration for aggregator shard rebalancing.
* pekkoConfiguration - Configuration of Pekko.

For example:

```json
{
  "logDirectory": "/usr/local/lib/metrics-cluster-aggregator/logs",
  "clusterPipelineConfiguration": "/usr/local/lib/metrics-cluster-aggregator/config/cluster-pipeline.json",
  "hostPipelineConfiguration": "/usr/local/lib/metrics-cluster-aggregator/config/host-pipeline.json",
  "httpPort": 7066,
  "httpHost": "0.0.0.0",
  "aggregationHost": "0.0.0.0",
  "aggregationPort": 7065,
  "maxConnectionTimeout": "PT2M",
  "minConnectionTimeout": "PT1M",
  "jvmMetricsCollectionInterval": "PT0.5S",
  "rebalanceConfiguration": {
    "maxParallel": 100,
    "threshold": 500
  },
  "pekkoConfiguration": {
    "pekko": {
      "loggers": ["pekko.event.slf4j.Slf4jLogger"],
      "loglevel": "DEBUG",
      "stdout-loglevel": "DEBUG",
      "logging-filter": "pekko.event.slf4j.Slf4jLoggingFilter",
      "actor": {
        "provider": "pekko.cluster.ClusterActorRefProvider",
        "debug": {
          "unhandled": "on"
        }
      },
      "cluster": {
        "sharding": {
          "state-store-mode": "persistence"
        },
        "seed-nodes": [
          "pekko.tcp://Metrics@127.0.0.1:2551"
        ]
      },
      "remote": {
        "log-remote-lifecycle-events": "on",
        "netty": {
          "tcp": {
            "hostname": "127.0.0.1",
            "port": 2551
          }
        }
      }
    }
  }
}
```

#### Pipeline

Metrics Cluster Aggregator supports a two pipelines.  The first is the host pipeline which handles publication of all statistics received from Metrics Aggregator Daemon instances.  The second is the cluster pipeline which handles all statistics (re)aggregated by cluster across host statistics from Metrics Aggregator Daemon instances.  In both cases the pipeline defines one more destinations or sinks for the statistics.

For example:

```json
{
    "sinks":
    [
        {
            "type": "com.arpnetworking.tsdcore.sinks.CarbonSink",
            "name": "my_application_carbon_sink",
            "serverAddress": "192.168.0.1"
        }
    ]
}
```

#### Hocon

The daemon and pipeline configuration files may be written in [Hocon](https://github.com/typesafehub/config)
when specified with a _.conf extension.

Development
-----------

To build the service locally you must satisfy these prerequisites:
* [JDK8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html) (Or Invoke with JDKW)
* [Docker](http://www.docker.com/) (for [Mac](https://docs.docker.com/docker-for-mac/))

__Note:__ Requires at least Docker for Mac Beta version _Version 1.12.0-rc4-beta19 (build: 10258)_

Next, fork the repository, clone and build:

Building:

    metrics-aggregator-daemon> ./mvnw verify

To use the local version in your project you must first install it locally:

    metrics-aggregator-daemon> ./mvnw install

To debug the server during run on port 9000:

    metrics-cluster-aggregator> ./mvnw -Ddebug=true docker:start

To debug the server during integration tests on port 9000:

    metrics-cluster-aggregator> ./mvnw -Ddebug=true verify

You can determine the version of the local build from the pom.xml file.  Using the local version is intended only for testing or development.

You may also need to add the local repository to your build in order to pick-up the local version:

* Maven - Included by default.
* Gradle - Add *mavenLocal()* to *build.gradle* in the *repositories* block.
* SBT - Add *resolvers += Resolver.mavenLocal* into *project/plugins.sbt*.

License
-------

Published under Apache Software License 2.0, see LICENSE

&copy; Groupon Inc., 2014
