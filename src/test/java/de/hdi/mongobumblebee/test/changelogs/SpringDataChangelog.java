package de.hdi.mongobumblebee.test.changelogs;

import org.springframework.data.mongodb.core.MongoTemplate;

import de.hdi.mongobumblebee.changeset.ChangeLog;
import de.hdi.mongobumblebee.changeset.ChangeSet;

/**
 * @author abelski
 */
@ChangeLog
public class SpringDataChangelog {

	@ChangeSet(author = "abelski", id = "spring_test1", order = "01")
	public void testChangeSet(MongoTemplate mongoTemplate) {
		System.out.println("invoked  with mongoTemplate=" + mongoTemplate.toString());
		System.out.println("invoked  with mongoTemplate=" + mongoTemplate.getCollectionNames());
	}
}
