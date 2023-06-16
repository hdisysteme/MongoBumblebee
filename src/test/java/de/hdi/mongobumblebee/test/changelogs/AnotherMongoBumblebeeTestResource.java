package de.hdi.mongobumblebee.test.changelogs;


import com.mongodb.client.MongoDatabase;

import de.hdi.mongobumblebee.MongoBumblebeeTest;
import de.hdi.mongobumblebee.changeset.ChangeLog;
import de.hdi.mongobumblebee.changeset.ChangeSet;

/**
 * @author lstolowski
 * @since 30.07.14
 */
@ChangeLog(order = "2")
public class AnotherMongoBumblebeeTestResource {

	@ChangeSet(author = MongoBumblebeeTest.USER, id = "Btest1", order = "01")
	public void testChangeSet(){
		System.out.println("invoked B1");
	}

	@ChangeSet(author = MongoBumblebeeTest.USER, id = "Btest2", order = "02")
	public void testChangeSet2(){
		System.out.println("invoked B2");
	}

	@ChangeSet(author = MongoBumblebeeTest.USER, id = "Btest3", order = "03", runAlways = true)
	public void testChangeSetWithAlways(MongoDatabase mongoDatabase) {
		System.out.println("invoked B3 with always + db=" + mongoDatabase.toString());
	}

	@ChangeSet(author = MongoBumblebeeTest.USER, id = "Btest4", order = "04")
	public void testChangeSet6(MongoDatabase mongoDatabase) {
		System.out.println("invoked B4 with db=" + mongoDatabase.toString());
	}

}
