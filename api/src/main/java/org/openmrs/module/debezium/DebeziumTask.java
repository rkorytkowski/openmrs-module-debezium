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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@Component("debezium.DebeziumTask")
public class DebeziumTask implements TaskHandler<DebeziumTaskData> {
	
	static final String OFFSET_STORAGE_TABLE = "debezium_offset_storage";
	
	static final String DATABASE_HISTORY_TABLE = "debezium_database_history";
	
	static final String FAILED_EVENTS_TABLE = "debezium_failed_events";
	
	static final String FAILED_EVENT_MAX_RETRIES_PROPERTY = "debezium.module.failed.event.max.retries";
	
	private static final Logger log = LoggerFactory.getLogger(DebeziumTask.class);
	
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	
	private static final int DEFAULT_FAILED_EVENT_MAX_RETRIES = 3;
	
	private static final String DEFAULT_HEARTBEAT_INTERVAL_MS = "30000";
	
	private static final long LIVENESS_TIMEOUT_MULTIPLIER = 3L;
	
	private volatile DebeziumEngine<ChangeEvent<String, String>> debeziumEngine;
	
	private final EventPublisher eventPublisher;
	
	private final @Nullable SessionFactory sessionFactory;
	
	private final Map<String, Class<?>> tableToEntityClassMap = new ConcurrentHashMap<>();
	
	private final AtomicBoolean stopping = new AtomicBoolean(false);
	
	private final AtomicLong lastEngineActivityTimestamp = new AtomicLong(0L);
	
	private volatile @Nullable ScheduledExecutorService livenessMonitor;
	
	private volatile @Nullable String lastFailedEventFingerprint;
	
	private volatile int lastFailedEventAttempts;
	
	public DebeziumTask(EventPublisher eventPublisher, ObjectProvider<SessionFactory> sessionFactoryProvider) {
		this.eventPublisher = eventPublisher;
		this.sessionFactory = sessionFactoryProvider.getIfAvailable();
	}
	
	@PreDestroy
	public void stopEngine() throws IOException {
		stopping.set(true);
		stopLivenessMonitor();
		if (this.debeziumEngine != null) {
			log.info("Stopping Embedded Debezium Engine...");
			this.debeziumEngine.close();
		}
	}
	
	@PostConstruct
	public void initTableToEntityClassMap() {
		refreshTableToEntityClassMap();
	}
	
	public Class<?> getEntityClassByTableName(String tableName) {
		if (tableName == null) {
			return null;
		}
		
		Class<?> entityClass = tableToEntityClassMap.get(tableName.toLowerCase(Locale.ROOT));
		if (entityClass == null && sessionFactory != null) {
			refreshTableToEntityClassMap();
			entityClass = tableToEntityClassMap.get(tableName.toLowerCase(Locale.ROOT));
		}
		return entityClass;
	}
	
	public void handleEvent(ChangeEvent<String, String> event) {
		lastEngineActivityTimestamp.set(System.currentTimeMillis());
		if (event.value() == null) {
			return;
		}
		
		String destination = event.destination();
		String key = event.key();
		log.debug("Received CDC event for table [{}]. Key: {}", destination, key);
		
		try {
			JsonNode keyPayload = parsePayload(key);
			if (keyPayload != null) {
				log.trace("Parsed Key Payload: {}", keyPayload);
			}
			
			JsonNode payload = parseRequiredPayload(event.value());
			if (payload.isMissingNode() || payload.isNull()) {
				return;
			}
			
			if (payload.has("status") && payload.has("id")) {
				String status = payload.path("status").asText();
				String txId = payload.path("id").asText();
				log.debug("Transaction Boundary Event -> Status: {}, Transaction ID: {}", status, txId);
				
				if ("BEGIN".equals(status)) {
					eventPublisher.publishEvent(new CDCTransactionStartedEvent(txId));
				} else if ("END".equals(status)) {
					eventPublisher.publishEvent(new CDCTransactionCompletedEvent(txId));
				}
				resetFailedEventTracking();
				return;
			}
			
			if (payload.has("ddl")) {
				log.debug("Schema Change Event detected and ignored. DDL: {}", payload.path("ddl").asText());
				resetFailedEventTracking();
				return;
			}
			
			if (isHeartbeatEvent(payload)) {
				log.trace("Heartbeat event detected and ignored for destination [{}]", destination);
				resetFailedEventTracking();
				return;
			}
			
			String op = payload.path("op").asText(null);
			if (op == null || op.isBlank()) {
				throw new IllegalArgumentException("Missing CDC operation for destination [" + destination + "]");
			}
			
			JsonNode before = payload.path("before");
			JsonNode after = payload.path("after");
			String txId = payload.path("transaction").path("id").asText(null);
			if (txId == null) {
				txId = payload.path("source").path("txId").asText(null);
			}
			String table = payload.path("source").path("table").asText(null);
			String snapshot = null;
			JsonNode snapshotNode = payload.path("source").path("snapshot");
			if (!snapshotNode.isMissingNode() && !snapshotNode.isNull()) {
				snapshot = snapshotNode.asText();
			}
			
			Class<?> entityClass = getEntityClassByTableName(table);
			log.debug("Parsed Event -> Operation: {}, Transaction ID: {}, Table: {}, Entity: {}, Snapshot: {}", op, txId, table, entityClass, snapshot);
			log.trace("Parsed Event States -> Before: {}, After: {}", before, after);
			
			@SuppressWarnings("unchecked")
			CDCEvent<Object> cdcEvent = new CDCEvent<>((Class<Object>) entityClass);
			cdcEvent.setOperation(getOperation(op));
			cdcEvent.setTransactionId(txId);
			cdcEvent.setTableName(table);
			cdcEvent.setSnapshot(snapshot);
			cdcEvent.setPrimaryKey(toMap(keyPayload));
			cdcEvent.setPreviousState(toMap(before));
			cdcEvent.setNewState(toMap(after));
			eventPublisher.publishEvent(cdcEvent);
			resetFailedEventTracking();
		}
		catch (Exception e) {
			handleEventFailure(event, key, e);
		}
	}
	
	@Override
	public void execute(DebeziumTaskData debeziumTaskData, TaskContext taskContext) {
		log.info("Instance elected to run Embedded Debezium Engine. Starting up...");
		stopping.set(false);
		
		AtomicReference<Throwable> engineError = new AtomicReference<>();
		Properties debeziumConfig = getDebeziumConfig();
		lastEngineActivityTimestamp.set(System.currentTimeMillis());
		startLivenessMonitor(debeziumConfig);
		
		try {
			this.debeziumEngine = DebeziumEngine.create(Json.class)
			        .using(debeziumConfig)
			        .notifying(this::handleEvent)
			        .using((DebeziumEngine.CompletionCallback) (success, message, error) -> {
				        lastEngineActivityTimestamp.set(System.currentTimeMillis());
				        if (error != null) {
					         engineError.set(error);
				        }
			        })
			        .build();
			debeziumEngine.run();
		}
		catch (Exception e) {
			engineError.compareAndSet(null, e);
		}
		finally {
			stopLivenessMonitor();
			this.debeziumEngine = null;
		}
		
		if (engineError.get() != null) {
			throw new RuntimeException("Embedded Debezium Engine failed to start or crashed.", engineError.get());
		}
		
		if (stopping.get()) {
			log.info("Embedded Debezium Engine stopped on request.");
			return;
		}
		
		throw new RuntimeException("Embedded Debezium Engine was stopped unexpectedly. Failing the task to re-try it on another instance.");
	}
	
	public Properties getDebeziumConfig() {
		Properties runtimeProperties = Context.getRuntimeProperties();
		String dbUrl = runtimeProperties.getProperty("connection.url");
		String dbUsername = runtimeProperties.getProperty("debezium.database.user");
		if (dbUsername == null) {
			dbUsername = runtimeProperties.getProperty("connection.username");
		}
		String dbPassword = runtimeProperties.getProperty("debezium.database.password");
		if (dbPassword == null) {
			dbPassword = runtimeProperties.getProperty("connection.password");
		}
		
		DatabaseConnectionInfo connectionInfo = parseDatabaseConnectionInfo(dbUrl);
		Properties debeziumConfig = io.debezium.config.Configuration.create()
		        .with("name", "openmrs-debezium-engine")
		        .with("connector.class", connectionInfo.getConnectorClass())
		        
		        // JDBC Offset Storage
		        .with("offset.storage", "io.debezium.storage.jdbc.offset.JdbcOffsetBackingStore")
		        .with("offset.storage.jdbc.table.name", OFFSET_STORAGE_TABLE)
		        .with("offset.storage.jdbc.connection.url", dbUrl)
		        .with("offset.storage.jdbc.connection.user", dbUsername)
		        .with("offset.storage.jdbc.connection.password", dbPassword)
		        .with("offset.flush.interval.ms", "60000")
		        
		        // Database connection properties
		        .with("database.hostname", connectionInfo.getHost())
		        .with("database.port", connectionInfo.getPort())
		        .with("database.user", dbUsername)
		        .with("database.password", dbPassword)
		        .with("database.server.id", defaultServerId())
		        .with("topic.prefix", "openmrs")
		        .with("snapshot.mode", "no_data")
		        .with("database.include.list", connectionInfo.getDatabaseName())
		        .with("table.exclude.list", ".*\\." + OFFSET_STORAGE_TABLE + ",.*\\." + DATABASE_HISTORY_TABLE + ",.*\\." + FAILED_EVENTS_TABLE)
		        .with("heartbeat.interval.ms", DEFAULT_HEARTBEAT_INTERVAL_MS)
		        
		        // JDBC Database Schema History
		        .with("schema.history.internal", "io.debezium.storage.jdbc.history.JdbcSchemaHistory")
		        .with("schema.history.internal.jdbc.table.name", DATABASE_HISTORY_TABLE)
		        .with("schema.history.internal.jdbc.connection.url", dbUrl)
		        .with("schema.history.internal.jdbc.connection.user", dbUsername)
		        .with("schema.history.internal.jdbc.connection.password", dbPassword)
		        // Fixes java.sql.SQLSyntaxErrorException: (conn=4005) Column length too big for column 'history_data' (max = 16383); use BLOB or TEXT instead
		        .with("schema.history.internal.jdbc.table.ddl",
		            "CREATE TABLE %s (id VARCHAR(255) NOT NULL, history_data MEDIUMTEXT, history_data_seq INTEGER, record_insert_ts TIMESTAMP NOT NULL, record_insert_seq INTEGER NOT NULL)")
		        .with("provide.transaction.metadata", "true")
		        .build()
		        .asProperties();
		Properties properties = new Properties();
		properties.putAll(debeziumConfig);
		for (String key : runtimeProperties.stringPropertyNames()) {
			if (key.startsWith("debezium.") && !key.startsWith("debezium.module.")) {
				String debeziumKey = key.substring("debezium.".length());
				properties.put(debeziumKey, runtimeProperties.getProperty(key));
			}
		}
		
		return properties;
	}
	
	static DatabaseConnectionInfo parseDatabaseConnectionInfo(@Nullable String dbUrl) {
		if (dbUrl == null || dbUrl.isBlank()) {
			throw new IllegalStateException("connection.url must be configured");
		}
		
		String connectorClass;
		String dbUrlWithoutJdbcPrefix;
		if (dbUrl.startsWith("jdbc:mysql:")) {
			connectorClass = "io.debezium.connector.mysql.MySqlConnector";
			dbUrlWithoutJdbcPrefix = dbUrl.substring("jdbc:mysql:".length());
		} else if (dbUrl.startsWith("jdbc:mariadb:")) {
			connectorClass = "io.debezium.connector.mariadb.MariaDbConnector";
			dbUrlWithoutJdbcPrefix = dbUrl.substring("jdbc:mariadb:".length());
		} else {
			throw new IllegalStateException("Unsupported database engine: " + dbUrl);
		}
		
		int startIndex = dbUrlWithoutJdbcPrefix.indexOf("//");
		if (startIndex < 0) {
			throw new IllegalStateException("connection.url must include a network host and database name: " + dbUrl);
		}
		
		String hostSection = dbUrlWithoutJdbcPrefix.substring(startIndex + 2);
		int slashIndex = hostSection.indexOf("/");
		if (slashIndex < 0) {
			throw new IllegalStateException("connection.url must include a database name: " + dbUrl);
		}
		
		String databasePath = hostSection.substring(slashIndex + 1);
		hostSection = hostSection.substring(0, slashIndex);
		int queryIndex = databasePath.indexOf("?");
		String databaseName = queryIndex >= 0 ? databasePath.substring(0, queryIndex) : databasePath;
		if (databaseName.isBlank()) {
			throw new IllegalStateException("connection.url must include a database name: " + dbUrl);
		}
		
		String primaryNode = hostSection.split(",")[0].trim();
		if (primaryNode.isBlank()) {
			throw new IllegalStateException("connection.url must include a database host: " + dbUrl);
		}
		
		String host = primaryNode;
		String port = "3306";
		if (primaryNode.startsWith("[")) {
			int endBracketIndex = primaryNode.indexOf("]");
			if (endBracketIndex < 0) {
				throw new IllegalStateException("Invalid IPv6 host in connection.url: " + dbUrl);
			}
			host = primaryNode.substring(1, endBracketIndex);
			if (primaryNode.length() > endBracketIndex + 2 && primaryNode.charAt(endBracketIndex + 1) == ':') {
				port = primaryNode.substring(endBracketIndex + 2);
			}
		} else {
			int colonIndex = primaryNode.lastIndexOf(":");
			if (colonIndex > 0) {
				host = primaryNode.substring(0, colonIndex);
				port = primaryNode.substring(colonIndex + 1);
			}
		}
		
		if (host.isBlank() || port.isBlank()) {
			throw new IllegalStateException("connection.url must include a valid database host and port: " + dbUrl);
		}
		
		return new DatabaseConnectionInfo(connectorClass, host, port, databaseName);
	}
	
	static String defaultServerId() {
		long serverId = (Integer.toUnsignedLong(ManagementFactory.getRuntimeMXBean().getName().hashCode()) % 4294967294L) + 1L;
		return Long.toString(serverId);
	}
	
	synchronized int registerFailedEventAttempt(ChangeEvent<String, String> event) {
		String fingerprint = Objects.toString(event.destination(), "") + '\n' + Objects.toString(event.key(), "") + '\n'
		        + Objects.toString(event.value(), "");
		if (fingerprint.equals(lastFailedEventFingerprint)) {
			lastFailedEventAttempts++;
		} else {
			lastFailedEventFingerprint = fingerprint;
			lastFailedEventAttempts = 1;
		}
		return lastFailedEventAttempts;
	}
	
	synchronized void resetFailedEventTracking() {
		lastFailedEventFingerprint = null;
		lastFailedEventAttempts = 0;
	}
	
	int getFailedEventMaxRetries() {
		String configuredValue = Context.getRuntimeProperties().getProperty(FAILED_EVENT_MAX_RETRIES_PROPERTY);
		if (configuredValue == null || configuredValue.isBlank()) {
			return DEFAULT_FAILED_EVENT_MAX_RETRIES;
		}
		try {
			int parsedValue = Integer.parseInt(configuredValue);
			if (parsedValue < 0) {
				throw new IllegalStateException(FAILED_EVENT_MAX_RETRIES_PROPERTY + " must be greater than or equal to 0");
			}
			return parsedValue;
		}
		catch (NumberFormatException e) {
			throw new IllegalStateException(FAILED_EVENT_MAX_RETRIES_PROPERTY + " must be an integer", e);
		}
	}
	
	void parkFailedEvent(ChangeEvent<String, String> event, int attempts, Exception error) {
		Properties runtimeProperties = Context.getRuntimeProperties();
		String dbUrl = runtimeProperties.getProperty("connection.url");
		String dbUsername = runtimeProperties.getProperty("debezium.database.user", runtimeProperties.getProperty("connection.username"));
		String dbPassword = runtimeProperties.getProperty("debezium.database.password", runtimeProperties.getProperty("connection.password"));
		
		try (Connection connection = DriverManager.getConnection(dbUrl, dbUsername, dbPassword);
		     Statement createTableStatement = connection.createStatement()) {
			createTableStatement.execute("CREATE TABLE IF NOT EXISTS " + FAILED_EVENTS_TABLE
			        + " (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, destination VARCHAR(255) NOT NULL, event_key MEDIUMTEXT NULL, event_value LONGTEXT NOT NULL, attempts INT NOT NULL, error_message TEXT NOT NULL, failed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)");
			try (PreparedStatement insertStatement = connection.prepareStatement("INSERT INTO " + FAILED_EVENTS_TABLE
			        + " (destination, event_key, event_value, attempts, error_message) VALUES (?, ?, ?, ?, ?)")) {
				insertStatement.setString(1, event.destination());
				insertStatement.setString(2, event.key());
				insertStatement.setString(3, event.value());
				insertStatement.setInt(4, attempts);
				insertStatement.setString(5, error.toString());
				insertStatement.executeUpdate();
			}
		}
		catch (SQLException e) {
			throw new DebeziumException("Failed to park CDC event for destination [" + event.destination() + "]", e);
		}
	}
	
	private synchronized void refreshTableToEntityClassMap() {
		tableToEntityClassMap.clear();
		if (sessionFactory == null) {
			return;
		}
		
		try {
			SessionFactoryImplementor sessionFactoryImplementor = sessionFactory.unwrap(SessionFactoryImplementor.class);
			for (EntityPersister persister : sessionFactoryImplementor.getMetamodel().entityPersisters().values()) {
				if (persister instanceof AbstractEntityPersister) {
					AbstractEntityPersister entityPersister = (AbstractEntityPersister) persister;
					for (String tableName : entityPersister.getConstraintOrderedTableNameClosure()) {
						String cleanTableName = cleanTableName(tableName);
						if (cleanTableName != null) {
							tableToEntityClassMap.put(cleanTableName.toLowerCase(Locale.ROOT), entityPersister.getMappedClass());
						}
					}
				}
			}
		}
		catch (Exception e) {
			log.warn("Failed to extract table mappings from SessionFactory", e);
		}
	}
	
	private static @Nullable String cleanTableName(@Nullable String tableName) {
		if (tableName == null) {
			return null;
		}
		
		String cleanTableName = tableName.replace("`", "").replace("\"", "");
		if (cleanTableName.contains(".")) {
			cleanTableName = cleanTableName.substring(cleanTableName.lastIndexOf(".") + 1);
		}
		return cleanTableName;
	}
	
	private static @Nullable JsonNode parsePayload(@Nullable String json) throws IOException {
		if (json == null) {
			return null;
		}
		
		JsonNode rootNode = OBJECT_MAPPER.readTree(json);
		return rootNode.has("payload") ? rootNode.path("payload") : rootNode;
	}
	
	private static JsonNode parseRequiredPayload(String json) throws IOException {
		JsonNode payload = parsePayload(json);
		if (payload == null) {
			throw new IllegalArgumentException("Missing CDC payload");
		}
		return payload;
	}
	
	private static boolean isHeartbeatEvent(JsonNode payload) {
		return !payload.has("op") && !payload.path("source").has("table") && !payload.has("before") && !payload.has("after");
	}
	
	private static @Nullable Map<String, Object> toMap(@Nullable JsonNode jsonNode) {
		if (jsonNode == null || jsonNode.isMissingNode() || jsonNode.isNull()) {
			return null;
		}
		return OBJECT_MAPPER.convertValue(jsonNode, new TypeReference<Map<String, Object>>() {});
	}
	
	private static CDCEvent.Operation getOperation(String op) {
		switch (op) {
			case "r":
				return CDCEvent.Operation.READ;
			case "c":
				return CDCEvent.Operation.CREATE;
			case "u":
				return CDCEvent.Operation.UPDATE;
			case "d":
				return CDCEvent.Operation.DELETE;
			case "t":
				return CDCEvent.Operation.TRUNCATE;
			default:
				throw new IllegalArgumentException("Unsupported CDC operation: " + op);
		}
	}
	
	private void handleEventFailure(ChangeEvent<String, String> event, @Nullable String key, Exception error) {
		int attempts = registerFailedEventAttempt(event);
		if (attempts > getFailedEventMaxRetries()) {
			parkFailedEvent(event, attempts, error);
			log.error("Parked CDC event for destination [{}] after {} failed attempts. Processing continues from the next event.", event.destination(), attempts, error);
			resetFailedEventTracking();
			return;
		}
		
		throw new DebeziumException("Failed to parse and process CDC event for key [" + key + "]. Value: " + event.value(), error);
	}
	
	private void startLivenessMonitor(Properties debeziumConfig) {
		stopLivenessMonitor();
		long heartbeatIntervalMs = Long.parseLong(debeziumConfig.getProperty("heartbeat.interval.ms", "0"));
		if (heartbeatIntervalMs <= 0) {
			return;
		}
		
		livenessMonitor = Executors.newSingleThreadScheduledExecutor(runnable -> {
			Thread thread = new Thread(runnable, "debezium-liveness-monitor");
			thread.setDaemon(true);
			return thread;
		});
		long checkIntervalMs = Math.max(heartbeatIntervalMs, 10000L);
		livenessMonitor.scheduleAtFixedRate(() -> {
			long lastActivity = lastEngineActivityTimestamp.get();
			if (!stopping.get() && debeziumEngine != null && lastActivity > 0
			        && System.currentTimeMillis() - lastActivity > heartbeatIntervalMs * LIVENESS_TIMEOUT_MULTIPLIER) {
				log.error("Embedded Debezium Engine appears stalled. Closing it so the scheduler can re-run on a healthy instance.");
				try {
					debeziumEngine.close();
				}
				catch (IOException e) {
					log.error("Failed to stop stalled Embedded Debezium Engine", e);
				}
			}
		}, checkIntervalMs, checkIntervalMs, TimeUnit.MILLISECONDS);
	}
	
	private void stopLivenessMonitor() {
		ScheduledExecutorService currentMonitor = livenessMonitor;
		livenessMonitor = null;
		if (currentMonitor != null) {
			currentMonitor.shutdownNow();
		}
	}
	
	static final class DatabaseConnectionInfo {
		
		private final String connectorClass;
		
		private final String host;
		
		private final String port;
		
		private final String databaseName;
		
		private DatabaseConnectionInfo(String connectorClass, String host, String port, String databaseName) {
			this.connectorClass = connectorClass;
			this.host = host;
			this.port = port;
			this.databaseName = databaseName;
		}
		
		String getConnectorClass() {
			return connectorClass;
		}
		
		String getHost() {
			return host;
		}
		
		String getPort() {
			return port;
		}
		
		String getDatabaseName() {
			return databaseName;
		}
	}
}
