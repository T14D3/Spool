import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.provider.Property
import org.gradle.api.publish.maven.tasks.PublishToMavenRepository
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.Optional
import org.gradle.api.tasks.TaskAction

plugins {
    id("java")
    `java-library`
    application
    `maven-publish`
}

group = "de.t14d3"

abstract class CheckReposiliteConfig : DefaultTask() {
    @get:Input @get:Optional abstract val reposiliteBaseUrl: Property<String>
    @get:Input @get:Optional abstract val reposiliteUsername: Property<String>
    @get:Input @get:Optional abstract val reposilitePassword: Property<String>

    @TaskAction
    fun run() {
        val missingKeys = mutableListOf<String>()

        fun require(name: String, value: String?) {
            if (value.isNullOrBlank()) missingKeys += name
        }

        require("reposiliteUsername/REPOSILITE_USERNAME", reposiliteUsername.orNull)
        require("reposilitePassword/REPOSILITE_PASSWORD", reposilitePassword.orNull)

        reposiliteBaseUrl.orNull?.let { baseUrl ->
            val url = baseUrl.trim()
            if (url.isNotBlank() && !url.startsWith("http://") && !url.startsWith("https://")) {
                missingKeys += "reposiliteBaseUrl/REPOSILITE_BASE_URL must start with http:// or https://"
            }
        }

        if (missingKeys.isNotEmpty()) {
            throw GradleException(
                "Missing Reposilite publishing configuration:\n" +
                    missingKeys.joinToString(separator = "\n") { "- $it" } +
                    "\n\nConfigure these as Gradle properties (recommended: ~/.gradle/gradle.properties) or environment variables."
            )
        }
    }
}

val buildVersion = System.getenv("VERSION")?.takeIf { it.isNotBlank() } ?: "1.2-SNAPSHOT"
version = buildVersion

val reposiliteBaseUrlProvider =
    providers
        .gradleProperty("reposiliteBaseUrl")
        .orElse(providers.environmentVariable("REPOSILITE_BASE_URL"))
        .orElse("https://maven.t14d3.de")
val reposiliteUsernameProvider =
    providers.gradleProperty("reposiliteUsername").orElse(providers.environmentVariable("REPOSILITE_USERNAME"))
val reposilitePasswordProvider =
    providers.gradleProperty("reposilitePassword").orElse(providers.environmentVariable("REPOSILITE_PASSWORD"))

val reposiliteBaseUrl = reposiliteBaseUrlProvider.get()
val reposiliteUsername = reposiliteUsernameProvider.orNull
val reposilitePassword = reposilitePasswordProvider.orNull

base {
    archivesName.set("spool")
}

application {
    mainClass.set("de.t14d3.spool.Main")
}

repositories {
    mavenCentral()
    maven("${reposiliteBaseUrl.trimEnd('/')}/releases")
    maven("${reposiliteBaseUrl.trimEnd('/')}/snapshots")
}

java {
    toolchain.languageVersion.set(JavaLanguageVersion.of(21))
    withSourcesJar()
    withJavadocJar()
}

dependencies {

    implementation("org.reflections:reflections:0.10.2")
    implementation("redis.clients:jedis:5.2.0")
    implementation("com.google.code.gson:gson:2.11.0")

    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("com.h2database:h2:2.2.224")
    testImplementation("org.xerial:sqlite-jdbc:3.45.3.0")
    testImplementation("com.mysql:mysql-connector-j:8.4.0")
    testImplementation("org.mariadb.jdbc:mariadb-java-client:3.3.3")
}

tasks.withType<JavaCompile>().configureEach {
    options.encoding = "UTF-8"
    options.release.set(21)
}

tasks.jar {
    manifest {
        attributes["Main-Class"] = "de.t14d3.spool.Main"
    }
}

val fatJar = tasks.register<Jar>("fatJar") {
    group = "build"
    description = "Assembles a runnable jar containing dependencies."

    archiveClassifier.set("all")
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE

    manifest {
        attributes["Main-Class"] = "de.t14d3.spool.Main"
    }

    from(sourceSets.main.get().output)
    dependsOn(configurations.runtimeClasspath)
    from({
        configurations.runtimeClasspath.get()
            .filter { it.name.endsWith(".jar") }
            .map { zipTree(it) }
    })
}

val checkReposiliteConfig = tasks.register<CheckReposiliteConfig>("checkReposiliteConfig") {
    group = "publishing"
    description = "Validates required configuration for publishing to Reposilite."

    reposiliteBaseUrl.set(reposiliteBaseUrlProvider)
    reposiliteUsername.set(reposiliteUsernameProvider)
    reposilitePassword.set(reposilitePasswordProvider)
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            artifactId = "spool"
            from(components["java"])
            artifact(fatJar)
        }
    }

    repositories {
        maven {
            name = "reposilite"

            val repo = if (version.toString().endsWith("SNAPSHOT")) "snapshots" else "releases"
            url = uri("${reposiliteBaseUrl.trimEnd('/')}/$repo")

            credentials {
                username = reposiliteUsername
                password = reposilitePassword
            }
        }
    }
}

tasks.withType<PublishToMavenRepository>().configureEach {
    if (name.contains("ToReposiliteRepository")) {
        dependsOn(checkReposiliteConfig)
    }
}

tasks.register("publishLocal") {
    group = "publishing"
    description = "Builds and publishes Spool to Maven Local."
    dependsOn("publishToMavenLocal")
}

tasks.test {
    useJUnitPlatform()
}
