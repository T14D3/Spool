plugins {
    id("java")
    `java-library`
    application
}

group = "de.t14d3"
version = "1.1-SNAPSHOT"

application {
    mainClass.set("de.t14d3.spool.Main")
}

repositories {
    mavenCentral()
}

dependencies {

    implementation("org.reflections:reflections:0.10.2")

    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("com.h2database:h2:2.2.224")
    testImplementation("mysql:mysql-connector-java:8.0.32")
}

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
}

tasks.jar {
    manifest {
        attributes["Main-Class"] = "de.t14d3.spool.Main"
    }
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    from(sourceSets.main.get().output)
    dependsOn(configurations.runtimeClasspath)
    from({
        configurations.runtimeClasspath.get().filter { it.name.endsWith("jar") }.map { zipTree(it) }
    })
}

tasks.test {
    useJUnitPlatform()
}