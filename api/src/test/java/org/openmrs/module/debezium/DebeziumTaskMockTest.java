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

import io.debezium.engine.ChangeEvent;
import org.hibernate.SessionFactory;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.metamodel.spi.MetamodelImplementor;
import org.hibernate.persister.entity.AbstractEntityPersister;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.openmrs.PersonName;
import org.openmrs.event.CDCEvent;
import org.openmrs.event.EventPublisher;
import org.springframework.beans.factory.ObjectProvider;

import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DebeziumTaskMockTest {
	
	@Mock
	private EventPublisher eventPublisher;
	
	@Mock
	private SessionFactory sessionFactory;
	
	private DebeziumTask debeziumTask;
	
	@Captor
	private ArgumentCaptor<CDCEvent<Object>> cdcEventCaptor;
	
	@Before
	@SuppressWarnings("unchecked")
	public void setUp() {
		SessionFactoryImplementor sfi = mock(SessionFactoryImplementor.class);
		MetamodelImplementor metamodel = mock(MetamodelImplementor.class);
		AbstractEntityPersister persister = mock(AbstractEntityPersister.class);
		ObjectProvider<SessionFactory> sessionFactoryProvider = mock(ObjectProvider.class);
		
		when(sessionFactoryProvider.getIfAvailable()).thenReturn(sessionFactory);
		when(sessionFactory.unwrap(SessionFactoryImplementor.class)).thenReturn(sfi);
		when(sfi.getMetamodel()).thenReturn(metamodel);
		when(metamodel.entityPersisters()).thenReturn(Collections.singletonMap("org.openmrs.PersonName", persister));
		when(persister.getConstraintOrderedTableNameClosure()).thenReturn(
		    new String[] { "person_name", "person_name_audit" });
		when(persister.getMappedClass()).thenReturn(PersonName.class);
		
		debeziumTask = new DebeziumTask(eventPublisher, sessionFactoryProvider);
		debeziumTask.initTableToEntityClassMap();
	}
	
	@Test
	public void handleEvent_shouldPublishCDCEvent_whenPersonNameIsUpdatedFromRawValueJson() throws Exception {
		String personNameUpdatePayload = "{"
		        + "\"before\":{\"person_name_id\":123,\"given_name\":\"John\",\"family_name\":\"Doe\",\"uuid\":\"a-uuid-123\"},"
		        + "\"after\":{\"person_name_id\":123,\"given_name\":\"Jonathan\",\"family_name\":\"Doe\",\"uuid\":\"a-uuid-123\"},"
		        + "\"source\":{\"version\":\"1.9.7.Final\",\"connector\":\"mysql\",\"name\":\"openmrs\",\"ts_ms\":1678886400000,"
		        + "\"snapshot\":\"false\",\"db\":\"openmrs\",\"table\":\"person_name\",\"server_id\":1,\"gtid\":null,"
		        + "\"file\":\"binlog.000001\",\"pos\":1500,\"row\":0,\"thread\":null,\"query\":null},"
		        + "\"op\":\"u\",\"ts_ms\":1678886401000,\"transaction\":{\"id\":\"a-tx-id-456\"}}";
		
		ChangeEvent<String, String> mockChangeEvent = mock(ChangeEvent.class);
		when(mockChangeEvent.key()).thenReturn("{\"person_name_id\":123}");
		when(mockChangeEvent.value()).thenReturn(personNameUpdatePayload);
		when(mockChangeEvent.destination()).thenReturn("openmrs.openmrs.person_name");
		
		debeziumTask.handleEvent(mockChangeEvent);
		
		verify(eventPublisher).publishEvent(cdcEventCaptor.capture());
		CDCEvent<Object> capturedEvent = cdcEventCaptor.getValue();
		
		assertNotNull(capturedEvent);
		assertEquals(CDCEvent.Operation.UPDATE, capturedEvent.getOperation());
		assertEquals(PersonName.class, capturedEvent.getEntityType());
		assertEquals("person_name", capturedEvent.getTableName());
		assertEquals("a-tx-id-456", capturedEvent.getTransactionId());
		assertEquals(123, capturedEvent.getPrimaryKey().get("person_name_id"));
		
		Map<String, Object> previousState = capturedEvent.getPreviousState();
		assertNotNull(previousState);
		assertEquals("John", previousState.get("given_name"));
		assertEquals("Doe", previousState.get("family_name"));
		
		Map<String, Object> newState = capturedEvent.getNewState();
		assertNotNull(newState);
		assertEquals("Jonathan", newState.get("given_name"));
		assertEquals("Doe", newState.get("family_name"));
	}
	
	@Test
	public void getEntityClassByTableName_shouldReturnCorrectClassForSecondaryTable() {
		assertEquals(PersonName.class, debeziumTask.getEntityClassByTableName("person_name_audit"));
	}
	
	@Test
	public void parseDatabaseConnectionInfo_shouldUseFirstHostAndExtractDatabaseName() {
		DebeziumTask.DatabaseConnectionInfo info = DebeziumTask
		        .parseDatabaseConnectionInfo("jdbc:mysql:replication://db-primary:3307,db-secondary:3307/custom_openmrs?useSSL=false");
		
		assertEquals("io.debezium.connector.mysql.MySqlConnector", info.getConnectorClass());
		assertEquals("db-primary", info.getHost());
		assertEquals("3307", info.getPort());
		assertEquals("custom_openmrs", info.getDatabaseName());
	}
	
	@Test
	public void parseDatabaseConnectionInfo_shouldFailLoudlyWhenDatabaseNameIsMissing() {
		IllegalStateException error = assertThrows(IllegalStateException.class,
		    () -> DebeziumTask.parseDatabaseConnectionInfo("jdbc:mariadb://db-primary:3306"));
		
		Assert.assertTrue(error.getMessage().contains("database name"));
	}
}
