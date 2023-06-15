package de.hdi.mongobumblebee.utils;

import de.hdi.mongobumblebee.MongoBumblebeeTest;
import de.hdi.mongobumblebee.changeset.ChangeLog;
import de.hdi.mongobumblebee.changeset.ChangeSet;

@ChangeLog
public class ChangeLogWithDuplicate {

	@ChangeSet(author = MongoBumblebeeTest.USER, id = "Btest1", order = "01")
	public void testChangeSet() {
		System.out.println("invoked B1");
	}

	@ChangeSet(author = MongoBumblebeeTest.USER, id = "Btest2", order = "02")
	public void testChangeSet2() {
		System.out.println("invoked B2");
	}

	@ChangeSet(author = MongoBumblebeeTest.USER, id = "Btest2", order = "03")
	public void testChangeSet3() {
		System.out.println("invoked B3");
	}
}
