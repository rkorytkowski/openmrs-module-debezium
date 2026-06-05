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

import org.junit.jupiter.api.Test;
import org.openmrs.PersonName;
import org.openmrs.test.jupiter.BaseModuleContextSensitiveTest;
import org.springframework.beans.factory.annotation.Autowired;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class DebeziumTaskTest extends BaseModuleContextSensitiveTest {
	
	@Autowired
	DebeziumTask debeziumTask;
	
	@Test
	public void getEntityClassByTableName_shouldReturnCorrectClassForExistingTable() {
		Class<?> clazz = debeziumTask.getEntityClassByTableName("person_name");
		assertEquals(PersonName.class, clazz);
	}
	
	@Test
	public void getEntityClassByTableName_shouldReturnCorrectClassIgnoringCase() {
		Class<?> clazz = debeziumTask.getEntityClassByTableName("PERSON_NAME");
		assertEquals(PersonName.class, clazz);
	}
	
	@Test
	public void getEntityClassByTableName_shouldReturnNullForNonExistingTable() {
		Class<?> clazz = debeziumTask.getEntityClassByTableName("unknown_table");
		assertNull(clazz);
	}
	
	@Test
	public void getEntityClassByTableName_shouldReturnNullForNullInput() {
		Class<?> clazz = debeziumTask.getEntityClassByTableName(null);
		assertNull(clazz);
	}
}
