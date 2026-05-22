/**
 * This Source Code Form is subject to the terms of the Mozilla Public License,
 * v. 2.0. If a copy of the MPL was not distributed with this file, You can
 * obtain one at http://mozilla.org/MPL/2.0/. OpenMRS is also distributed under
 * the terms of the Healthcare Disclaimer located at http://openmrs.org/license.
 *
 * Copyright (C) OpenMRS Inc. OpenMRS is a registered trademark and the OpenMRS
 * graphic logo is a trademark of OpenMRS Inc.
 */
package org.openmrs.module.debezium;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import io.debezium.DebeziumException;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import org.hibernate.SessionFactory;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.persister.entity.AbstractEntityPersister;
import org.hibernate.persister.entity.EntityPersister;
import org.jspecify.annotations.Nullable;
import org.openmrs.api.context.Context;
import org.openmrs.event.CDCEvent;
import org.openmrs.event.CDCTransactionCompletedEvent;
import org.openmrs.event.CDCTransactionStartedEvent;
import org.openmrs.event.EventPublisher;
import org.openmrs.scheduler.TaskContext;
import org.openmrs.scheduler.TaskHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

@Component("debezium.DebeziumEngineManager")
public class DebeziumEngineManager implements TaskHandler<DebeziumTaskData> {
	
	private static final Logger log = LoggerFactory.getLogger(DebeziumEngineManager.class);
	
	private static final ObjectMapper objectMapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	
	private volatile DebeziumEngine<ChangeEvent<String, String>> debeziumEngine;
	
	@Autowired(required = false)
	private EventPublisher eventPublisher;
	
	@Autowired(required = false)
	private SessionFactory sessionFactory;
	
	private final Map<String, Class<?>> tableToEntityClassMap = new HashMap<>();
	
	public DebeziumEngineManager() {
	}
	
	@PreDestroy
	public void stopEngine() throws IOException {
		if (this.debeziumEngine != null) {
			log.info("Stopping Embedded Debezium Engine...");
			this.debeziumEngine.close();
		}
	}
	
	@PostConstruct
	public void initTableToEntityClassMap() {
		if (sessionFactory != null) {
			try {
				SessionFactoryImplementor sfi = sessionFactory.unwrap(SessionFactoryImplementor.class);
				for (EntityPersister persister : sfi.getMetamodel().entityPersisters().values()) {
					if (persister instanceof AbstractEntityPersister) {
						AbstractEntityPersister ap = (AbstractEntityPersister) persister;
						String tName = ap.getTableName();
						if (tName != null) {
							String cleanTableName = tName.replace("`", "").replace("\"", "");
							if (cleanTableName.contains(".")) {
								cleanTableName = cleanTableName.substring(cleanTableName.lastIndexOf(".") + 1);
							}
							tableToEntityClassMap.put(cleanTableName.toLowerCase(), ap.getMappedClass());
						}
					}
				}
			} catch (Exception e) {
				log.warn("Failed to extract table mappings from SessionFactory", e);
			}
		}
	}
	
	private Class<?> getEntityClassByTableName(String tableName) {
		if (tableName == null) {
			return null;
		}
		return tableToEntityClassMap.get(tableName.toLowerCase());
	}
	
	private void handleEvent(ChangeEvent<String, String> event) {
		if (event.value() != null) {
			String destination = event.destination();
			String key = event.key();
			
			log.debug("Received CDC event for table [{}]. Key: {}", destination, key);
			
			try {
				// --- Parse the Composite Key ---
				JsonNode keyPayload = null;
				if (key != null) {
					JsonNode keyNode = objectMapper.readTree(key);
					// Handle both schema-embedded payloads and raw JSON objects
					keyPayload = keyNode.has("payload") ? keyNode.path("payload") : keyNode;
					log.debug("Parsed Key Payload: {}", keyPayload);
				}
				
				JsonNode rootNode = objectMapper.readTree(event.value());
				JsonNode payload = rootNode.path("payload");
				
				if (!payload.isMissingNode() && !payload.isNull()) {
					// 1. Check if this is a Transaction Boundary Event (BEGIN/END)
					if (payload.has("status") && payload.has("id")) {
						String status = payload.path("status").asText(); // "BEGIN" or "END"
						String txId = payload.path("id").asText();
						
						log.debug("Transaction Boundary Event -> Status: {}, Transaction ID: {}", status, txId);

						if ("BEGIN".equals(status)) {
							eventPublisher.publishEvent(new CDCTransactionStartedEvent(txId));
						} else if ("END".equals(status)) {
							eventPublisher.publishEvent(new CDCTransactionCompletedEvent(txId));
						}
						return;
					}
					
					// 2. Handle Standard Data Events
					String op = payload.path("op").asText();
					JsonNode before = payload.path("before");
					JsonNode after = payload.path("after");
					String txId = payload.path("source").path("txId").asText(null);
					String table = payload.path("source").path("table").asText(null);
					
					Class<?> entityClass = getEntityClassByTableName(table);
					log.debug("Parsed Event -> Operation: {}, Transaction ID: {}, Table: {}, Entity: {}, Before: {}, After: {}", op, txId, table, entityClass, before,
					    after);
					@SuppressWarnings("unchecked")
					CDCEvent<Object> cdcEvent = new CDCEvent<>((Class<Object>) entityClass);
					cdcEvent.setOperation(getOperation(op));
					cdcEvent.setTransactionId(txId);
					cdcEvent.setTableName(table);
					Map<String, Object> primaryKey = (keyPayload == null || keyPayload.isMissingNode() || keyPayload.isNull()) ? null
							: objectMapper.convertValue(keyPayload, new TypeReference<Map<String, Object>>() {});
					cdcEvent.setPrimaryKey(primaryKey);
					Map<String, Object> previousState = (before == null || before.isMissingNode() || before.isNull()) ? null
					        : objectMapper.convertValue(before, new TypeReference<Map<String, Object>>() {});
					Map<String, Object> newState = (after == null || after.isMissingNode() || after.isNull()) ? null
					        : objectMapper.convertValue(after, new TypeReference<Map<String, Object>>() {});
					cdcEvent.setPreviousState(previousState);
					cdcEvent.setNewState(newState);
					eventPublisher.publishEvent(cdcEvent);
				}
			}
			catch (Exception e) {
				// It may be desired to put an event in a dead letter queue after a certain number of failures
				// or manually in order to unblock publishing consecutive CDC events.

				throw new DebeziumException("Failed to parse and process CDC event for key [" + key + "]. Value: "
						+ event.value(), e);
			}
		}
	}

	private static CDCEvent.@Nullable Operation getOperation(String op) {
		CDCEvent.Operation operation = null;
		switch (op) {
			case "r":
				operation = CDCEvent.Operation.READ;
				break;
			case "c":
				operation = CDCEvent.Operation.CREATE;
				break;
			case "u":
				operation = CDCEvent.Operation.UPDATE;
				break;
			case "d":
				operation = CDCEvent.Operation.DELETE;
				break;

		}
		return operation;
	}

	@Override
    public void execute(DebeziumTaskData debeziumTaskData, TaskContext taskContext) {
        log.info("Instance elected to run Embedded Debezium Engine. Starting up...");
        
        AtomicReference<Throwable> engineError = new AtomicReference<>();
        
        try {
            this.debeziumEngine = DebeziumEngine.create(Json.class)
                    .using(getDebeziumConfig())
                    .notifying(this::handleEvent)
                    .using((DebeziumEngine.CompletionCallback) (success, message, error) -> {
                        if (error != null) {
                            engineError.set(error);
                        }
                    })
                    .build();
            debeziumEngine.run();
        } catch (Exception e) {
            engineError.compareAndSet(null, e);
        }
        
        if (engineError.get() != null) {
            throw new RuntimeException("Embedded Debezium Engine failed to start or crashed.", engineError.get());
        }
        
        throw new RuntimeException("Embedded Debezium Engine was stopped. Failing the task to re-try it on another instance.");
    }
	
	public Properties getDebeziumConfig() {
		Properties runtimeProperties = Context.getRuntimeProperties();
		String dbUrl = runtimeProperties.getProperty("connection.url");
		String dbUsername = runtimeProperties.getProperty("debezium.database.user");
		String dbPassword = runtimeProperties.getProperty("debezium.database.password");
		String connectorClass;
		if (dbUrl.startsWith("jdbc:mysql:")) {
			connectorClass = "io.debezium.connector.mysql.MySqlConnector";
		} else if (dbUrl.startsWith("jdbc:mariadb:")) {
			connectorClass = "io.debezium.connector.mariadb.MariaDbConnector";
		} else {
			throw new IllegalStateException("Unsupported database engine: " + dbUrl);
		}
		
		String dbHostname = "localhost";
		String dbPort = "3306";
		try {
			int startIndex = dbUrl.indexOf("//");
			if (startIndex != -1) {
				String hostSection = dbUrl.substring(startIndex + 2);
				
				// Cut off the database path and query parameters
				int slashIndex = hostSection.indexOf("/");
				if (slashIndex != -1) {
					hostSection = hostSection.substring(0, slashIndex);
				}
				
				// In replication URLs (host1:3306,host2:3306), Debezium needs the primary/master (first host)
				String primaryNode = hostSection.split(",")[0];
				
				// Split into host and port
				String[] parts = primaryNode.split(":");
				dbHostname = parts[0];
				if (parts.length > 1) {
					dbPort = parts[1];
				}
			}
		}
		catch (Exception e) {
			log.warn("Could not parse connection.url for database hostname and port. Using defaults.", e);
		}
		Properties debeziumConfig = io.debezium.config.Configuration
		        .create()
		        .with("name", "openmrs-debezium-engine")
		        .with("connector.class", connectorClass)
		        
		        // JDBC Offset Storage
		        .with("offset.storage", "io.debezium.storage.jdbc.offset.JdbcOffsetBackingStore")
		        .with("offset.storage.jdbc.offset.table.name", "debezium_offset_storage")
		        .with("offset.storage.jdbc.url", dbUrl)
		        .with("offset.storage.jdbc.user", dbUsername)
		        .with("offset.storage.jdbc.password", dbPassword)
		        .with("offset.flush.interval.ms", "60000")
		        
		        // Database connection properties
		        .with("database.hostname", dbHostname)
				.with("database.port", dbPort)
				.with("database.user", dbUsername)
		        .with("database.password", dbPassword)
		        .with("database.server.id", "123456789")
		        .with("topic.prefix", "openmrs")
		        .with("snapshot.mode", "no_data")
		        .with("database.include.list", "openmrs")
		        .with("table.exclude.list", ".*\\.debezium_offset_storage,.*\\.debezium_database_history")
		        
		        // JDBC Database Schema History
		        .with("schema.history.internal", "io.debezium.storage.jdbc.history.JdbcSchemaHistory")
		        .with("schema.history.internal.jdbc.table.name", "debezium_database_history")
		        .with("schema.history.internal.jdbc.url", dbUrl)
		        .with("schema.history.internal.jdbc.user", dbUsername)
		        .with("schema.history.internal.jdbc.password", dbPassword)
		        // Fixes java.sql.SQLSyntaxErrorException: (conn=4005) Column length too big for column 'history_data' (max = 16383); use BLOB or TEXT instead
		        .with(
		            "schema.history.internal.jdbc.table.ddl",
		            "CREATE TABLE %s (id VARCHAR(255) NOT NULL, history_data MEDIUMTEXT, history_data_seq INTEGER, record_insert_ts TIMESTAMP NOT NULL, record_insert_seq INTEGER NOT NULL)")
		        
		        .with("provide.transaction.metadata", "true").build().asProperties();
		Properties properties = new Properties();
		properties.putAll(debeziumConfig);
		for (String key : runtimeProperties.stringPropertyNames()) {
			if (key.startsWith("debezium.")) {
				String debeziumKey = key.substring("debezium.".length());
				properties.put(debeziumKey, runtimeProperties.getProperty(key));
			}
		}
		return properties;
	}
}
