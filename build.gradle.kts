import com.github.jengelman.gradle.plugins.shadow.transformers.Log4j2PluginsCacheFileTransformer
import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
import groovy.util.Node
import org.openjfx.gradle.JavaFXPlatform

description = "Proxy pass allows developers to MITM a vanilla client and server without modifying them."

plugins {
    id("eclipse")
    id("java")
    id("application")
    alias(libs.plugins.shadow)
    alias(libs.plugins.javafxplugin)
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(25))
    }
}

javafx {
    version = "25.0.1"
    modules("javafx.controls", "javafx.fxml")
}

repositories {
    //mavenLocal()
    mavenCentral()
    maven("https://repo.opencollab.dev/maven-snapshots")
    maven("https://repo.opencollab.dev/maven-releases")
    maven("https://jitpack.io") 
}

val nativePlatforms = listOf(
    "windows-x86_64",
    "windows-aarch64",
    "linux-x86_64",
    "linux-aarch64",
    "macos-x86_64",
    "macos-aarch64"
)

val connectorClassesDir = layout.buildDirectory.dir("connector/classes")
val connectorResourcesDir = layout.buildDirectory.dir("connector/resources")
val mctokenHelperClassesDir = layout.buildDirectory.dir("mctoken-helper/classes")
val connectorExeDir = layout.buildDirectory.dir("launch4j/connector")
val connectorLaunch4jRuntimeDir = layout.buildDirectory.dir("launch4j/runtime")
val connectorLaunch4jConfigFile = layout.buildDirectory.file("launch4j/connector/EggnetLan.xml")

val launch4jTool by configurations.creating
val launch4jWorkdir by configurations.creating

dependencies {
    compileOnly(libs.lombok)

    annotationProcessor(libs.lombok)

    implementation(libs.bedrock.codec)
    implementation(libs.bedrock.common)
    implementation(libs.bedrock.connection)
    implementation(libs.jackson.dataformat.yaml)
    implementation(libs.log4j)
    implementation(libs.minecraftauth)
    implementation(libs.richtextfx)
    implementation(libs.atlantafx)
    implementation(libs.checker.qual)
    implementation(libs.netty.transport.nethernet)

    nativePlatforms.forEach { platform ->
        runtimeOnly(libs.webrtc.java) {
            artifact {
                classifier = platform
            }
        }
    }

    launch4jTool("net.sf.launch4j:launch4j:3.50")
    launch4jWorkdir("net.sf.launch4j:launch4j:3.50:workdir-win32@jar")
}

application {
    mainClass.set("org.cloudburstmc.proxypass.ProxyPass")
}

tasks.shadowJar {
    archiveBaseName.set("EggnetProxyApp")
    archiveClassifier.set("")
    archiveVersion.set("")
    manifest {
        attributes["Enable-Native-Access"] = "ALL-UNNAMED"
    }
    transform(Log4j2PluginsCacheFileTransformer())
    filesMatching("META-INF/org/apache/logging/log4j/core/config/plugins/Log4j2Plugins.dat") {
        duplicatesStrategy = DuplicatesStrategy.INCLUDE
    }
}

tasks.register<JavaCompile>("compileConnectorJava") {
    setSource(
        files(
            "src/main/java/org/cloudburstmc/proxypass/tools/NetherNetConnectorCli.java",
            "src/main/java/org/cloudburstmc/proxypass/EggnetCommunitySupport.java"
        )
    )
    classpath = sourceSets.main.get().compileClasspath
    destinationDirectory.set(connectorClassesDir)
    options.release.set(21)
    options.encoding = "UTF-8"
}

tasks.register<JavaCompile>("compileMctokenHelperJava") {
    setSource(
        files(
            "src/main/java/org/cloudburstmc/proxypass/tools/BuildMCTokenCli.java"
        )
    )
    classpath = sourceSets.main.get().compileClasspath
    destinationDirectory.set(mctokenHelperClassesDir)
    options.release.set(21)
    options.encoding = "UTF-8"
}

tasks.register<Copy>("processConnectorResources") {
    from("src/main/resources") {
        include("log4j2.xml")
    }
    into(connectorResourcesDir)
}

tasks.register<ShadowJar>("connectorShadowJar") {
    dependsOn("compileConnectorJava", "processConnectorResources")
    archiveBaseName.set("EggnetLan")
    archiveClassifier.set("")
    archiveVersion.set("")
    configurations = listOf(project.configurations.runtimeClasspath.get())
    from(connectorClassesDir) {
        include("org/cloudburstmc/proxypass/tools/**")
        include("org/cloudburstmc/proxypass/EggnetCommunitySupport.class")
        include("org/cloudburstmc/proxypass/EggnetCommunitySupport$*.class")
    }
    from(connectorResourcesDir) {
        include("log4j2.xml")
    }
    manifest {
        attributes["Enable-Native-Access"] = "ALL-UNNAMED"
        attributes["Main-Class"] = "org.cloudburstmc.proxypass.tools.NetherNetConnectorCli"
    }
    transform(Log4j2PluginsCacheFileTransformer())
    filesMatching("META-INF/org/apache/logging/log4j/core/config/plugins/Log4j2Plugins.dat") {
        duplicatesStrategy = DuplicatesStrategy.INCLUDE
    }
}

tasks.register<ShadowJar>("mctokenHelperShadowJar") {
    dependsOn("compileMctokenHelperJava")
    archiveBaseName.set("EggnetMCTokenHelper")
    archiveClassifier.set("")
    archiveVersion.set("")
    configurations = listOf(project.configurations.runtimeClasspath.get())
    from(mctokenHelperClassesDir) {
        include("org/cloudburstmc/proxypass/tools/BuildMCTokenCli.class")
        include("org/cloudburstmc/proxypass/tools/BuildMCTokenCli$*.class")
    }
    manifest {
        attributes["Enable-Native-Access"] = "ALL-UNNAMED"
        attributes["Main-Class"] = "org.cloudburstmc.proxypass.tools.BuildMCTokenCli"
    }
    transform(Log4j2PluginsCacheFileTransformer())
    filesMatching("META-INF/org/apache/logging/log4j/core/config/plugins/Log4j2Plugins.dat") {
        duplicatesStrategy = DuplicatesStrategy.INCLUDE
    }
}

tasks.register("prepareLaunch4jRuntime") {
    inputs.files(launch4jTool, launch4jWorkdir)
    outputs.dir(connectorLaunch4jRuntimeDir)
    doLast {
        val runtimeDir = connectorLaunch4jRuntimeDir.get().asFile
        val unpackDir = runtimeDir.resolve("unpack")
        delete(runtimeDir)
        runtimeDir.mkdirs()
        copy {
            from(launch4jTool)
            into(runtimeDir)
        }
        copy {
            from(zipTree(launch4jWorkdir.singleFile))
            into(unpackDir)
        }
        copy {
            from(unpackDir.resolve("launch4j-3.50-workdir-win32"))
            into(runtimeDir)
        }
        delete(unpackDir)
    }
}

tasks.register("writeConnectorLaunch4jConfig") {
    dependsOn("connectorShadowJar")
    val connectorJar = tasks.named<ShadowJar>("connectorShadowJar").flatMap { it.archiveFile }
    inputs.file(connectorJar)
    outputs.file(connectorLaunch4jConfigFile)
    doLast {
        val outputDir = connectorExeDir.get().asFile
        outputDir.mkdirs()
        val configFile = connectorLaunch4jConfigFile.get().asFile
        val jarPath = connectorJar.get().asFile.absolutePath.replace("\\", "/")
        val exePath = outputDir.resolve("EggnetLan.exe").absolutePath.replace("\\", "/")
        configFile.parentFile.mkdirs()
        configFile.writeText(
            """
            <launch4jConfig>
              <headerType>console</headerType>
              <outfile>$exePath</outfile>
              <jar>$jarPath</jar>
              <dontWrapJar>false</dontWrapJar>
              <errTitle>Eggnet LAN</errTitle>
              <chdir>.</chdir>
              <jre>
                <path>%JAVA_HOME%;%PATH%</path>
                <minVersion>21</minVersion>
                <requires64Bit>true</requires64Bit>
              </jre>
              <versionInfo>
                <fileVersion>1.0.0.0</fileVersion>
                <txtFileVersion>1.0.0</txtFileVersion>
                <productVersion>1.0.0.0</productVersion>
                <txtProductVersion>1.0.0</txtProductVersion>
                <fileDescription>Eggnet LAN Connector</fileDescription>
                <copyright>Eggnet</copyright>
                <productName>Eggnet LAN</productName>
                <companyName>Eggnet</companyName>
                <internalName>EggnetLan</internalName>
                <originalFilename>EggnetLan.exe</originalFilename>
              </versionInfo>
            </launch4jConfig>
            """.trimIndent() + "\n",
            Charsets.UTF_8
        )
    }
}

tasks.register<Exec>("createExe") {
    dependsOn("prepareLaunch4jRuntime", "writeConnectorLaunch4jConfig")
    val connectorJar = tasks.named<ShadowJar>("connectorShadowJar").flatMap { it.archiveFile }
    inputs.file(connectorJar)
    outputs.file(connectorExeDir.map { it.file("EggnetLan.exe") })
    val runtimeDir = connectorLaunch4jRuntimeDir.get().asFile
    workingDir = runtimeDir
    commandLine(
        javaToolchains.launcherFor {
            languageVersion.set(JavaLanguageVersion.of(25))
        }.get().executablePath.asFile.absolutePath,
        "-Dlaunch4j.bindir=${runtimeDir.absolutePath}\\bin",
        "-Dlaunch4j.tmpdir=${connectorExeDir.get().asFile.absolutePath}",
        "-cp",
        "${runtimeDir.absolutePath}\\*",
        "net.sf.launch4j.Main",
        connectorLaunch4jConfigFile.get().asFile.absolutePath,
    )
}

tasks.named<JavaExec>("run") {
    workingDir = projectDir.resolve("run")
    workingDir.mkdir()
}

listOf("distZip", "distTar", "startScripts").forEach { taskName ->
    tasks.named(taskName) {
        dependsOn("shadowJar")
    }
}

tasks.named("startShadowScripts") {
    dependsOn("jar")
}

tasks.jar {
    archiveBaseName.set("EggnetProxyApp")
}

tasks.assemble {
    dependsOn("createExe")
}
