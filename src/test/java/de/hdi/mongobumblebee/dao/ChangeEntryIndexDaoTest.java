package de.hdi.mongobumblebee.dao;


import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

import de.hdi.mongobumblebee.MongoBumblebeeTest;
import de.hdi.mongobumblebee.dao.ChangeEntryIndexDao;

/**
 * @author lstolowski
 * @since 10.12.14
 */
class ChangeEntryIndexDaoTest {
	
	private static final String CHANGEID_AUTHOR_INDEX_NAME = "changeId_1_author_1";

	private ChangeEntryIndexDao dao = new ChangeEntryIndexDao(MongoBumblebeeTest.CHANGELOG_COLLECTION_NAME);

	@Test
	void shouldCreateRequiredUniqueIndex() {
		// given
		MongoClient mongo = mock(MongoClient.class);
		MongoDatabase db = MongoClients.create().getDatabase(MongoBumblebeeTest.DB_NAME);
		when(mongo.getDatabase(Mockito.anyString())).thenReturn(db);

		// when
		dao.createRequiredUniqueIndex(db.getCollection(MongoBumblebeeTest.CHANGELOG_COLLECTION_NAME));

		// then
		Document createdIndex = findIndex(db, CHANGEID_AUTHOR_INDEX_NAME);
		assertNotNull(createdIndex);
		assertTrue(dao.isUnique(createdIndex));
	}

	private Document findIndex(MongoDatabase db, String indexName) {

		for (MongoCursor<Document> iterator = db.getCollection(MongoBumblebeeTest.CHANGELOG_COLLECTION_NAME).listIndexes().iterator(); iterator.hasNext(); ) {
			Document index = iterator.next();
			String name = (String) index.get("name");
			if (indexName.equals(name)) {
				return index;
			}
		}
		return null;
	}

}
