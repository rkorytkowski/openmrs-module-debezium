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