Debezium Module
==========================

Description
-----------
Runs embedded Debezium that connects to database and publishes CDC events on each DB operation.

It issues a CDCEvent for each read, create, update, delete of a DB row and CDCTransactionStartedEvent/CompletedEvent
upon starting and successfully completing a transaction.

Each produced event contains transaction id so events belonging to the same transaction can be easily
aggregated.

Embedded Debezium runs as a recurring scheduled task in OpenMRS. It runs indefinitely if no errors
occur. If anything fails the task is re-started within a few seconds to resume publishing events
from the last failed event. If the error persists the task gets re-scheduled after a minute unitl error is fixed.

No further events are published until issue is resolved to guarantee consistency.

Multiple replicas of OpenMRS are supported with only one replica running Debezium at a time. 
Application events are only published on a replica that runs Debezium. If a replica with Debezium
fails, the Debezium task is scheduled on another replica.

Debezium can be customized with runtime properties using the `debezium.` prefix. For a standard
docker compose installation the prefix is `OMRS_EXTRA_DEBEZIUM_`.

Listening to events
-----------
Since events are published from a scheduled task, they are asynchronous to the application logic by default. 
You must not use `@TransactionalEventListener` to listen to CDC events.

`@EventListener` and `@OutboxEventListener` are fully supported. It is recommended to use
`@EventListener` for fast listeners that run in the same thread as the Debezium task, thus any failure
leads to the task failure and triggers re-try logic.

Alternatively for slow running listeners you may use `@OutboxEventListener` to have the CDC event 
persisted in the DB upon publishing and have a slow running listener being executed asynchronously by the Outbox engine.
This way the event is guaranteed to be processed asynchronously even if anything fails as opposed to `@EventListener`
annotated with `@Async`, which may lead to loosing an event when anything fails.

Please note that `@OutboxEventListener` serializes each event to JSON and persist it in DB, which introduces
performance overhead and latency, thus consider it carefully over synchronous `@EventListener`.

Running the module
-----------

For Debezium to work, you need to enable binlog on your DB instance by specifying
`server-id`, `log-bin`, `binglog-format` and `binlog-row-image` command line arguments. For
a standard docker compose installation the command would be as follows:
```
    db:
        image: mariadb:10.11.7
        command:
        - "mariadbd"
        - "--server-id=1"
        - "--log-bin=mysql-bin"
        - "--binlog-format=ROW"
        - "--binlog-row-image=FULL"
        - "--character-set-server=utf8mb4"
        - "--collation-server=utf8mb4_general_ci"
```
You also need to create a user with privileges to read binlog:
```
-- source: ./setup-debezium-user.sql

-- For a standard docker compose setup connect to DB as follows:
-- docker compose exec db mariadb -u root -p
-- password: openmrs

-- Create dedicated debezium user
CREATE USER IF NOT EXISTS 'debezium'@'%' IDENTIFIED BY 'Debezium1234';
CREATE USER IF NOT EXISTS 'debezium'@'localhost' IDENTIFIED BY 'Debezium1234';

-- Grant basic replication privileges required to read the binlog
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium'@'%';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium'@'localhost';

-- For newer versions of MariaDB (10.5+), the BINLOG MONITOR privilege is explicitly required
GRANT BINLOG MONITOR ON *.* TO 'debezium'@'%';
GRANT BINLOG MONITOR ON *.* TO 'debezium'@'localhost';

-- Grant privileges on the openmrs database to read data and write offset/history tables
GRANT ALL PRIVILEGES ON openmrs.* TO 'debezium'@'%';
GRANT ALL PRIVILEGES ON openmrs.* TO 'debezium'@'localhost';

-- Apply the changes
FLUSH PRIVILEGES;
```

Finally specify `debezium.database.user=debezium` and `debezium.database.password=Debezium1234` runtime properties.
For a standard docker compose setup it would be as follows:
```
api:
    environment:
        OMRS_EXTRA_DEBEZIUM_DATABASE_USER: debezium
        OMRS_EXTRA_DEBEZIUM_DATABASE_PASSWORD: Debezium1234
```

Building from Source
--------------------
You will need to have Java 1.8+ and Maven 2.x+ installed.  Use the command 'mvn package' to 
compile and package the module.  The .omod file will be in the omod/target folder.

Installation
------------
1. Build the module to produce the .omod file.
2. Use the OpenMRS Administration > Manage Modules screen to upload and install the .omod file.

If uploads are not allowed from the web (changeable via a runtime property), you can drop the omod
into the ~/.OpenMRS/modules folder.  (Where ~/.OpenMRS is assumed to be the Application 
Data Directory that the running openmrs is currently using.)  After putting the file in there 
simply restart OpenMRS/tomcat and the module will be loaded and started.
