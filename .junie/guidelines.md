# Metrics Cluster Aggregator Development Guidelines

This document provides guidelines and information for developers working on the Metrics Cluster Aggregator project.

## Build/Configuration Instructions

### Prerequisites

- **Java**: The project requires Java 17 or later. The project uses the Maven wrapper (`mvnw`) which will download the appropriate JDK version if needed.
- **Docker**: Required for building and running integration tests.

### Building the Project

To build the project:

```bash
./mvnw verify
```

To build without running tests:

```bash
./mvnw package -DskipTests
```

To install the project locally:

```bash
./mvnw install
```

### Docker Build

The project includes Docker support. To build the Docker image:

```bash
./mvnw package docker:build
```

### Debugging

To debug the server during run on port 9000:

```bash
./mvnw -Ddebug=true docker:start
```

To debug the server during integration tests on port 9000:

```bash
./mvnw -Ddebug=true verify
```

## Testing Information

### Testing Framework

The project uses:
- JUnit 4 for unit testing
- Mockito for mocking
- Hamcrest for assertions
- Pekko TestKit for testing actors

### Running Tests

To run all tests:

```bash
./mvnw test
```

To run a specific test:

```bash
./mvnw test -Dtest=ClassName
```

### Writing Tests

1. **Unit Tests**: Place in `src/test/java` following the same package structure as the main code.
2. **Actor Tests**: Extend `BaseActorTest` which provides an ActorSystem and Mockito initialization.
3. **Test Naming**: Use descriptive names that indicate what is being tested.
4. **Assertions**: Use Hamcrest matchers with `assertThat()` for readable assertions.

### Example Test

Here's a simple example of a test class:

```java
package com.arpnetworking.utility;

import org.hamcrest.Matchers;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;

public class StringUtilsTest {

    @Test
    public void testReverseWithNormalString() {
        final String input = "hello";
        final String expected = "olleh";
        final String result = StringUtils.reverse(input);
        assertThat(result, Matchers.equalTo(expected));
    }

    @Test
    public void testReverseWithEmptyString() {
        final String input = "";
        final String expected = "";
        final String result = StringUtils.reverse(input);
        assertThat(result, Matchers.equalTo(expected));
    }

    @Test
    public void testReverseWithNullString() {
        final String input = null;
        final String result = StringUtils.reverse(input);
        assertThat(result, Matchers.nullValue());
    }
}
```

## Code Style and Development Practices

### Code Style

1. **Builder Pattern**: Use the `OvalBuilder` for creating objects with many parameters.
2. **Immutability**: Use immutable collections (Guava's `ImmutableList`, `ImmutableSet`, etc.) and final fields.
3. **Validation**: Use Oval for validation with annotations like `@NotNull`, `@NotEmpty`, and `@Range`.
4. **Optional Values**: Use `Optional<T>` for values that might be absent.
5. **Field Naming**: Private fields are prefixed with underscore (e.g., `_fieldName`).
6. **Parameters**: Method parameters should be marked as `final`.
7. **toString()**: Use `MoreObjects.toStringHelper` for implementing `toString()` methods.
8. **Time Values**: Use `Duration` for representing time values.

### Documentation

1. **JavaDoc**: All public classes and methods should have JavaDoc comments.
2. **License Header**: All source files should include the Apache 2.0 license header.

### Project Structure

1. **Package Organization**: Code is organized by functionality in packages.
2. **Configuration**: Configuration is handled through JSON or HOCON files.
3. **Actors**: The project uses Pekko (formerly Akka) for the actor system.

### Dependency Injection

The project uses Guice for dependency injection.

### Logging

The project uses SLF4J with Logback for logging.

## Additional Development Information

### Metrics

The project uses the Metrics Client library for collecting and reporting metrics.

### Configuration

The application configuration is specified in JSON or HOCON format. Key configuration files:

1. **Main Configuration**: Specifies system settings like ports, hosts, and timeouts.
2. **Pipeline Configuration**: Defines the data processing pipeline for host and cluster statistics.

### Pekko Configuration

Pekko (formerly Akka) configuration is specified in the main configuration file under the `pekkoConfiguration` key.

### Database Support

The project supports multiple database configurations through the `databaseConfigurations` setting.

### Deployment

The project can be deployed as:
1. A standalone application
2. A Docker container
3. An RPM package (using the `rpm` profile)