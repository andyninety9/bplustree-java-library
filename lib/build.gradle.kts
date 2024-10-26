/*
 * This is the build configuration file for the B+ Tree Library project.
 * It generates a fat JAR that contains:
 * - Compiled code
 * - Source code
 * - Javadoc documentation
 */

plugins {
    // Java library plugin for API/implementation separation.
    `java-library`

    // Shadow plugin to create fat JARs.
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

group = "org.bptree"  // Set the project group (namespace).
version = "1.0.0"  // Set the version of the library.

repositories {
    // Use Maven Central to resolve dependencies.
    mavenCentral()
}

dependencies {
    // JUnit 5 for unit testing.
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.10.3")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.10.3")
}

java {
    toolchain {
        // Use Java 17 for this project.
        languageVersion.set(JavaLanguageVersion.of(17))
    }

    // Include source code and Javadoc in the build.
    withSourcesJar()
    withJavadocJar()
}

tasks {
    // Configure the shadowJar task to create a fat JAR.
    named<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar>("shadowJar") {
        archiveBaseName.set("bplustree-library")  // Set the base name.
        archiveClassifier.set("")  // No classifier, this is the main JAR.
        archiveVersion.set(provider { version.toString() })  // Set version dynamically.
    }

    // Ensure tests run with the JUnit platform.
    test {
        useJUnitPlatform()
    }
}

/*
 * Usage:
 * 1. Run `./gradlew shadowJar` to build the fat JAR.
 * 2. The output will be located in `build/libs/`.
 * 3. The fat JAR includes compiled code, source code, and Javadoc.
 */
