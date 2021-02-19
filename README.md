# trino-plugins

Plugins framework for Trino clusters aimed at easing the boostrap process plugins written
for the [Trino SPI](https://github.com/trinodb/trino/tree/master/trino-spi).

For those unfamiliar with the acronym, SPI stands for "Service Provider Interface".


## [SystemAccessControl](https://github.com/trinodb/trino/blob/master/trino-spi/src/main/java/io/trino/spi/security/SystemAccessControl.java)

The initial version of this project was aimed only at implementing custom auth via SystemAccessControl. For this reason,
the auth store is much more full featured than any other part of this project. The SystemAccessControl interface
specifies the methods that Trino uses to determine which resources (catalog/schema/table) and actions (functions, etc.)
are accessible for a given role when they submit a query.

Implement `com.simondata.trino.auth.AuthRules` to define your own custom auth rules. You can also adapt one of the
built-in example implementations.


## [EventListener](https://github.com/trinodb/trino/blob/master/trino-spi/src/main/java/io/trino/spi/eventlistener/EventListener.java)

The EventListener interface specifies methods that will be called for query lifecycle events (create/split/complete).


## Starburst

The `SystemAccessControl` interface is subject to major change between versions of Trino. The `SystemAccessControl`
interface bundled with Starburst's release is proprietary, expanding on the method definitions in the
open source trino-spi. We address this by defining `SystemAccessControlStarburst` which extends
`io.trino.spi.security.SystemAcessControl` and declares the additional funcions defined by Starburst.
Our implementations of `SystemAccessControl` extend `SystemAccessControlStarburst` instead, which ensures we are not
missing any of the Starburst additions, while still remaining backward compatible with the open source version of Trino.

We have a related issue with the `SystemAccessControlFactory`, where Starburst adds a LicenseManager parameter to
its `create()` method. We handle this by providing two overriden `create()` methods, permitting the right version to be
selected based on the version of Trino which loads our plugin.

In the event that a newer version of Starburst expands on the custom set of added methods, our `XRay` utility can
dump out a class' structure as JSON so we can keep `SystemAccessControlStarburst` in sync the proprietary version.
This will require inspecting the logs for messages containing "XRAY" in order to discover changes to the
interface structure. The classes which `XRay` inspects are specified in `AuthMetrics.init()`.


## Dependencies

### jdk

Make sure you have the jdk installed for java 8 or greater.

Mac (x64):
```
brew cask install java
```

Mac (M1)
Download the [Zulu JDK](https://www.azul.com/downloads/zulu-community/?os=macos&architecture=arm-64-bit&package=jdk) dmg

Ubuntu:
```
sudo apt install default-jdk
```

While this project does target Java 11, any version of the JDK >= 11 can be used to build the project targeting version 11.

### sbt

Make sure you have the latest version of sbt [installed](https://www.scala-sbt.org/1.x/docs/Setup.html)

Once this is installed, you should be able to [build the project](#building).

### trino-spi

This is defined in [build.sbt](build.sbt), and will pull the version of the dependency defined by `trinoVersion`.

[trino-spi](https://mvnrepository.com/artifact/io.trino/trino-spi/351)

The version of the SPI dependency needs to match the version of the Trino cluster to which we are adding the plugin.


## Building

Build a basic jar containing only our local classes:
```
$ sbt package
```

Build the full, fat jar containing all dependencies, with debug output:
```
$ sbt assembly
```

Build the full, fat jar, outputting the artifact name and instructions on where where the artifact should be placed on S3.
```
$ ./assemble.sh
```

The jar file will be written to `target/scala-2.13/trino-plugins-<trino-version>-<git-hash>[-dirty].jar`

(marked `-dirty` if we do not have a clean working tree in git)


## Local Docker

Once the `assemble.sh` script has been used to generate the artifact (fat jar), a local Trino cluster can be
started within Docker according to the included `docker-compose.yml` file.

### Requirements

- docker
- docker-compose

### Starting in Docker

You can start via the simple wrapper script (listening on localhost:8080):
```
$ ./start-trino.sh
```

You can optionally change the port exposed for the coordinator (listening on localhost:8888):
```
$ TRINO_PORT=8888 ./start-trino.sh
```

## Configuration

Configuration is applied via environment variables or the `/etc/trino/trin-plugins.properies` file.

Each of the configuration descriptors below has a header of the following form: `<description> (<type> = <default-value>)`

When a configuration involves logging, it is applies to the logger and all of its targets.
When disabled, it will neither log to disk nor to Slack (or any other custom auxiliary targets).

### Log each created query (boolean = true)
- `TRINO_PLUGINS_LOG_QUERY_CREATED`
- `events.log.query_created`

### Log each successful query run (boolean = true)
- `TRINO_PLUGINS_LOG_QUERY_SUCCESS`
- `events.log.query_success`

### Log each failed query run (boolean = true)
- `TRINO_PLUGINS_LOG_QUERY_FAILURE`
- `events.log.query_failure`

### Log each split completion (boolean = false)
- `TRINO_PLUGINS_LOG_SPLIT_COMPLETE`
- `events.log.split_complete`

### Send a Slack message for each created query (boolean = false)
- `TRINO_PLUGINS_SLACK_QUERY_CREATED`
- `events.slack.query_created`

### Send a Slack message for each successful query run (boolean = false)
- `TRINO_PLUGINS_SLACK_QUERY_SUCCESS`
- `events.slack.query_success`

### Send a Slack message for each failed query run (boolean = true)
- `TRINO_PLUGINS_SLACK_QUERY_FAILURE`
- `events.slack.query_failure`

### Send a Slack message for each split completion (boolean = false)
- `TRINO_PLUGINS_SLACK_SPLIT_COMPLETE`
- `events.slack.split_complete`

### Enforce auth (boolean = true)
- `TRINO_PLUGINS_ENFORCE_AUTH`
- `auth.enforce`

### Environment (`dev` | `prod` = `dev`)
- `TRINO_PLUGINS_ENVIRONMENT`
- `environment`

### Optional Slack webhook to which slack messages will be delivered by the logger (url = "")
- `TRINO_PLUGINS_SLACK_WEBHOOK_URL`
- `notifications.slack.webhook`

### The unique name of this cluster (string = <cloud-formation-stack-name-if-found>)
- `TRINO_PLUGINS_CLUSTER`
- `trino.cluster.name`

### The type of node on which this plugin is running (`coordinator` | `worker` = <cloud-formation-config-if-found>)
- `TRINO_PLUGINS_NODE_TYPE`
- `trino.node.type`


## Plugin Assembly

- [Trino Plugins](https://trino.io/docs/current/develop/spi-overview.html)
- [Trino System Access Control](https://trino.io/docs/current/develop/system-access-control.html)

There are a variety of requirements which must be met in order for a plugin to be correctly built and installed.
- The plugin is loaded by creating an instances of the declared plugin class (implements `io.trino.spi.Plugin`),
  fetching an instance of a factory (e.g., implements `io.trino.spi.security.SystemAccessControlFactory`) which must be
  an instance of an inner class that implements `Plugin`. Since Scala does not support no-arg constructors, the
  plugin entry point must be a Java source file (see `com.simondata.trino.TrinoPlugins.java`). This Java file may
  reference any Scala type, including traits. However, due to Scala's lack of no-arg constructors, a Scala class can not
  be the entry point (guice cannot instantiate a Scala class in order to inject it).
- Include a file named `io.trino.spi.Plugin` must reside in `META-INF/services/` containing the
  fully-qualified classname. In our case, `com.simondata.trino.TrinoPlugins`.
- The generated jar must be deployed into the plugin directory on trino in a directory with the same name as the plugin.
  In our case, the directory is named `trino-plugins/`.
- The file `access-control.properties` must be defined in the trino `etc/` directory. The file must begin with the
  line `access-control.name=<plugin-name>`. In our case the first config line is `access-control.name=trino-auth`.
- The file `event-listener.properties` must be defined in the trino `etc/` directory. The file must begin with the
  line `access-control.name=<plugin-name>`. In our case the first config line is `access-control.name=trino-events`.
- Plugins must be loaded by all coordinators and workers.
- The default location for plugins is `/usr/lib/trino/lib/plugin/`. This can be changed, but would then require that
  all plugins be relocated to the new plugin directory.

The `assemble.sh` scripts takes cares of the top two issues.


## Links to Additional Resources

- [Building an RPM package](https://github.com/trinodb/trino/blob/master/trino-server-rpm/README.md)

