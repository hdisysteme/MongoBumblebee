package de.hdi.mongobumblebee.test.changelogs;


import com.mongodb.client.MongoDatabase;

import de.hdi.mongobumblebee.MongoBumblebeeTest;
import de.hdi.mongobumblebee.changeset.ChangeLog;
import de.hdi.mongobumblebee.changeset.ChangeSet;

/**
 * @author lstolowski
 * @since 27/07/2014
 */
@ChangeLog(order = "1")
public class MongoBumblebeeTestResource {

	@ChangeSet(author = MongoBumblebeeTest.USER, id = "test1", order = "01")
	public void testChangeSet() {
		System.out.println("invoked 1");
	}

	@ChangeSet(author = MongoBumblebeeTest.USER, id = "test2", order = "02")
	public void testChangeSet2() {
		System.out.println("invoked 2");
	}

	@ChangeSet(author = MongoBumblebeeTest.USER, id = "test3", order = "03")
	public void testChangeSet5(MongoDatabase mongoDatabase) {
		System.out.println("invoked 3 with mongoDatabase=" + mongoDatabase.toString());
	}

}
