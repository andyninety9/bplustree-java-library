/*
 * This is the build configuration file for the B+ Tree Library project.
 * It utilizes Kotlin DSL for Gradle to manage dependencies, build tasks,
 * and toolchain configuration for optimal compatibility.
 */

plugins {
    // Apply the Java Library plugin to manage API and implementation separation.
    `java-library`
}

repositories {
    // Define Maven Central as the repository to resolve project dependencies.
    mavenCentral()
}

dependencies {
    // JUnit Jupiter (JUnit 5) is included for unit testing.
    // The API is used at compile time to write tests.
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.10.3")

    // The engine is only required at runtime to execute the tests.
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.10.3")
}

testing {
    suites {
        // Configure the JVM-based test suite to use JUnit Jupiter.
        val test by getting(JvmTestSuite::class) {
            // Ensure that the JUnit Jupiter engine is used for all test cases.
            useJUnitJupiter()
        }
    }
}

java {
    toolchain {
        // Use Java 17 for this project to ensure compatibility and long-term support.
        languageVersion = JavaLanguageVersion.of(17)
    }

    // Generate a JAR file containing the project’s source code.
    withSourcesJar()

    // Generate a JAR file containing the project’s Javadoc documentation.
    withJavadocJar()
}

/*
 * This Gradle build script follows best practices:
 * 1. Dependencies are properly scoped (testImplementation and testRuntimeOnly).
 * 2. Java toolchain ensures consistent builds across different environments.
 * 3. Separate JARs for source code and documentation improve code distribution.
 */
