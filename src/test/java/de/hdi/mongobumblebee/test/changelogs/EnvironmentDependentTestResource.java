package de.hdi.mongobumblebee.test.changelogs;

import org.springframework.core.env.Environment;
import org.springframework.data.mongodb.core.MongoTemplate;

import de.hdi.mongobumblebee.MongoBumblebeeTest;
import de.hdi.mongobumblebee.changeset.ChangeLog;
import de.hdi.mongobumblebee.changeset.ChangeSet;

@ChangeLog(order = "3")
public class EnvironmentDependentTestResource {

	@ChangeSet(author = MongoBumblebeeTest.USER, id = "Envtest1", order = "01")
	public void testChangeSet7WithEnvironment(MongoTemplate template, Environment env) {
		System.out.println("invoked Envtest1 with mongotemplate=" + template.toString() + " and Environment " + env);
	}
}
