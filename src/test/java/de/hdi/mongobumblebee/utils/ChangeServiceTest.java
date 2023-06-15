package de.hdi.mongobumblebee.utils;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import de.hdi.mongobumblebee.MongoBumblebeeTest;
import de.hdi.mongobumblebee.changeset.ChangeEntry;
import de.hdi.mongobumblebee.exception.MongobeeChangeSetException;
import de.hdi.mongobumblebee.test.changelogs.AnotherMongobeeTestResource;
import de.hdi.mongobumblebee.test.changelogs.MongobeeTestResource;
import de.hdi.mongobumblebee.utils.ChangeService;

/**
 * @author lstolowski
 * @since 27/07/2014
 */
class ChangeServiceTest {

	@Test
	void shouldFindChangeLogClasses(){
		// given
		String scanPackage = MongobeeTestResource.class.getPackage().getName();
		ChangeService service = new ChangeService(scanPackage);

		// when
		List<Class<?>> foundClasses = service.fetchChangeLogs();

		// then
		assertTrue(foundClasses != null && foundClasses.size() > 0);
	}

	@Test
	void shouldFindChangeSetMethods() throws MongobeeChangeSetException {
		// given
		String scanPackage = MongobeeTestResource.class.getPackage().getName();
		ChangeService service = new ChangeService(scanPackage);

		// when
		List<Method> foundMethods = service.fetchChangeSets(MongobeeTestResource.class);

		// then
		assertNotNull(foundMethods);
		assertEquals(3, foundMethods.size());
	}

	@Test
	void shouldFindAnotherChangeSetMethods() throws MongobeeChangeSetException {
		// given
		String scanPackage = MongobeeTestResource.class.getPackage().getName();
		ChangeService service = new ChangeService(scanPackage);

		// when
		List<Method> foundMethods = service.fetchChangeSets(AnotherMongobeeTestResource.class);

		// then
		assertNotNull(foundMethods);
		assertEquals(4, foundMethods.size());
	}


	@Test
	void shouldFindIsRunAlwaysMethod() throws MongobeeChangeSetException {
		// given
		String scanPackage = MongobeeTestResource.class.getPackage().getName();
		ChangeService service = new ChangeService(scanPackage);

		// when
		List<Method> foundMethods = service.fetchChangeSets(AnotherMongobeeTestResource.class);
		// then
		for (Method foundMethod : foundMethods) {
			if (foundMethod.getName().equals("testChangeSetWithAlways")){
				assertTrue(service.isRunAlwaysChangeSet(foundMethod));
			} else {
				assertFalse(service.isRunAlwaysChangeSet(foundMethod));
			}
		}
	}

	@Test
	void shouldCreateEntry() throws MongobeeChangeSetException {

		// given
		String scanPackage = MongobeeTestResource.class.getPackage().getName();
		ChangeService service = new ChangeService(scanPackage);
		List<Method> foundMethods = service.fetchChangeSets(MongobeeTestResource.class);

		for (Method foundMethod : foundMethods) {

			// when
			ChangeEntry entry = service.createChangeEntry(foundMethod);

			// then
			assertEquals(MongoBumblebeeTest.USER, entry.getAuthor());
			assertEquals(MongobeeTestResource.class.getName(), entry.getChangeLogClass());
			assertNotNull(entry.getTimestamp());
			assertNotNull(entry.getChangeId());
			assertNotNull(entry.getChangeSetMethodName());
		}
	}

	@Test
	void shouldFailOnDuplicatedChangeSets() throws MongobeeChangeSetException {
		String scanPackage = ChangeLogWithDuplicate.class.getPackage().getName();
		final ChangeService service = new ChangeService(scanPackage);
		Assertions.assertThrows(MongobeeChangeSetException.class, () -> service.fetchChangeSets(ChangeLogWithDuplicate.class));
	}

}
