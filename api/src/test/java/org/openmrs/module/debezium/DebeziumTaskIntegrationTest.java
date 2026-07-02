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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.openmrs.Person;
import org.openmrs.PersonName;
import org.openmrs.api.PersonService;
import org.openmrs.event.CDCEvent;
import org.openmrs.scheduler.SchedulerService;
import org.openmrs.scheduler.TaskState;
import org.openmrs.test.Containers;
import org.openmrs.test.SkipBaseSetup;
import org.openmrs.test.jupiter.BaseContextSensitiveNonTransactionalTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Instant;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@SkipBaseSetup
public class DebeziumTaskIntegrationTest extends BaseContextSensitiveNonTransactionalTest {
	
	@Autowired
	private CDCEventListener eventListener;
	
	@Autowired
	private SchedulerService schedulerService;
	
	@Autowired
	private PersonService personService;
	
	@Autowired
	private DebeziumInitializer debeziumInit;
	
	@Override
	public Properties getRuntimeProperties() {
		Properties props = super.getRuntimeProperties();
		// Workaround for DB locking issues in tests
		props.setProperty("debezium.snapshot.locking.mode", "none");
		// Use local-heap for Hibernate Search to avoid Lucene lock conflicts with other test contexts
		props.setProperty("hibernate.search.backend.directory.type", "local-heap");
		return props;
	}
	
	@Override
	public void ensureDatabaseRunning() {
		Containers.ensureDatabaseRunning(new MariaDBContainer<>(DockerImageName.parse("mariadb:10.5"))
				.withCommand("--log-bin=mariadb-bin", "--binlog-format=ROW", "--server-id=1", "--log-slave-updates=ON")
				.withDatabaseName("openmrs")
				.withUsername("root")
				.withPassword("test"), "mysql");
	}
	
	@Override
	public Boolean useInMemoryDatabase() {
		return false;
	}
	
	@BeforeEach
	public void clearCapturedEvents() {
		executeDataSet(INITIAL_XML_DATASET_PACKAGE_PATH);
		authenticate();
		debeziumInit.afterSingletonsInstantiated();
		eventListener.clearCapturedEvents();
	}
	
	@Test
	public void shouldCaptureAndPublishCdcEvent_whenDatabaseRowIsInserted() throws Exception {
		waitForDebeziumToStart();
		
		Person person = personService.getPerson(1);
		PersonName personName = new PersonName("Bob", "", "Smith");
		person.addName(personName);
		personService.savePerson(person);
		
		// Verify that our event listener caught the event within a timeout of 30 seconds
		// This prevents the test from flaking due to binlog processing delays
		long timeout = System.currentTimeMillis() + 120000;
		CDCEvent<PersonName> capturedEvent = null;
		while (capturedEvent == null && System.currentTimeMillis() < timeout) {
			for (CDCEvent<?> event : eventListener.getCapturedEvents()) {
				Map<String, Object> state = event.getNewState();
				if (state != null && "Bob".equals(state.get("given_name"))) {
					@SuppressWarnings("unchecked")
					CDCEvent<PersonName> matchedEvent = (CDCEvent<PersonName>) event;
					capturedEvent = matchedEvent;
					break;
				}
			}
			if (capturedEvent == null) {
				Thread.sleep(500);
			}
		}
		assertNotNull("Did not receive expected CDC event for Bob within the timeout", capturedEvent);
		
		assertEquals(CDCEvent.Operation.CREATE, capturedEvent.getOperation());
		assertEquals(PersonName.class, capturedEvent.getEntityType());
		assertEquals("person_name", capturedEvent.getTableName());
		assertEquals(personName.getPersonNameId(), capturedEvent.getPrimaryKey().get("person_name_id"));
		assertNotNull(capturedEvent.getTransactionId());
		assertTrue(!capturedEvent.getTransactionId().isBlank());
		
		Map<String, Object> newState = capturedEvent.getNewState();
		assertNotNull(newState);
		assertEquals("Bob", newState.get("given_name"));
		assertEquals("Smith", newState.get("family_name"));
	}
	
	private void waitForDebeziumToStart() throws InterruptedException {
		// Wait 60s until Debezium started
		long timeout = System.currentTimeMillis() + 60000;
		while (!schedulerService.getTasks(TaskState.PROCESSING, Instant.now()).anyMatch(
				task -> DebeziumInitializer.EMBEDDED_DEBEZIUM_TASK_UUID.equals(task.getRecurringTaskUuid().orElse(null))) &&
				System.currentTimeMillis() < timeout) {
			Thread.sleep(500);
		}
		// Give 10s for Debezium to initialize
		Thread.sleep(10000);
	}
}
