plugins {
    id("java")
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {

    // ... other dependencies
    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    implementation ("org.apache.kafka:kafka-clients:2.8.0")
    implementation ("org.apache.httpcomponents:httpclient:4.5.13")
    implementation ("org.apache.spark:spark-core_2.12:3.4.2") {
        exclude(group = "org.apache.hadoop", module = "hadoop-client")
    }
    implementation ("org.apache.spark:spark-sql_2.12:3.4.2") {
        exclude(group = "org.apache.hadoop", module = "hadoop-client")
    }
    implementation ("com.opencsv:opencsv:5.6")

    implementation ("com.fasterxml.jackson.core:jackson-core:2.13.0")
    implementation ("com.fasterxml.jackson.core:jackson-databind:2.13.0")
    implementation ("com.fasterxml.jackson.dataformat:jackson-dataformat-csv:2.13.0")

    implementation ("org.json:json:20210307")
        // ... other dependencies



}

tasks.test {
    useJUnitPlatform()
}