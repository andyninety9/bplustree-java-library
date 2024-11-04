/*
 * build.gradle.kts for the B+ Tree Library with integrated Hadoop support.
 * This configuration includes dependencies for Hadoop and configurations for fat JAR generation.
 */

plugins {
    // Java library plugin for modular library construction.
    `java-library`

    // Shadow plugin to create a fat JAR.
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

group = "org.bptree"
version = "1.0.0"

repositories {
    mavenCentral()  // Main repository for dependencies
}

dependencies {
    // Unit testing dependencies (JUnit 5).
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.10.3")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.10.3")

    // Hadoop dependencies for MapReduce integration.
    implementation("org.apache.hadoop:hadoop-common:3.3.4")
    implementation("org.apache.hadoop:hadoop-mapreduce-client-core:3.3.4")
    implementation("org.apache.hadoop:hadoop-hdfs:3.3.4")  // Optional: Hadoop HDFS if needed
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(8))  // Ensure Java 17 compatibility
    }

    withSourcesJar()  // Include sources in the build
    withJavadocJar()  // Include Javadoc in the build
}

tasks {
    // Configure the shadowJar task for fat JAR creation.
    named<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar>("shadowJar") {
        archiveBaseName.set("bplustree-library")  // Set the base name for the JAR
        archiveClassifier.set("")  // Main JAR without classifier
        archiveVersion.set(provider { version.toString() })  // Set version dynamically

        // Specify the main class for the jar manifest
        manifest {
            attributes["Main-Class"] = "org.bptree.hadoop.BPlusTreeJob"  // Adjust to actual package of BPlusTreeJob
        }

        // Include all dependencies in the fat JAR
        mergeServiceFiles()
    }

    // Configure testing
    test {
        useJUnitPlatform()  // Use JUnit 5
        maxHeapSize = "2g"
        jvmArgs("-Xms512m", "-Xmx2g", "-Xss1m")

        testLogging {
            events("passed", "skipped", "failed")  // Show detailed test output
        }
    }

    withType<JavaCompile> {
        options.compilerArgs.addAll(listOf("-Xlint:unchecked", "-Xlint:deprecation"))
    }
}
