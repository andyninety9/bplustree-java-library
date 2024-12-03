/*
 * build.gradle.kts for the B+ Tree Library with integrated Hadoop and Spark support.
 * This configuration includes dependencies for Hadoop, Spark, Jackson for JSON processing,
 * Apache Commons Math for statistical operations, and configurations for creating a fat JAR using the Shadow plugin.
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

    // Spark dependencies
    implementation("org.apache.spark:spark-core_2.12:3.5.3")
    implementation("org.apache.spark:spark-sql_2.12:3.5.3")

    // Jackson dependencies for JSON processing
    implementation("com.fasterxml.jackson.core:jackson-databind:2.13.3")
    implementation("com.fasterxml.jackson.core:jackson-core:2.13.3")
    implementation("com.fasterxml.jackson.core:jackson-annotations:2.13.3")

    // Apache Commons Math for statistical operations
    implementation("org.apache.commons:commons-math3:3.6.1")
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(8))  // Ensure Java 8 compatibility
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
            attributes["Main-Class"] = "org.bptree.hadoop.BPlusTreeJob"  // Adjust to actual package and main class
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
