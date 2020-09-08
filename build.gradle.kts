plugins {
    kotlin("jvm") version "1.4.0"
}

repositories {
    mavenCentral()
}

dependencies {
    testImplementation("org.testcontainers:testcontainers:1.14.3")
    testImplementation("org.testcontainers:jdbc:1.14.3")
    testImplementation("org.testcontainers:postgresql:1.14.3")
    testImplementation("org.postgresql:postgresql:42.2.16")
}
