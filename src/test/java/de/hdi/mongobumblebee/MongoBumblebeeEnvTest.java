package de.hdi.mongobumblebee;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.when;

import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.mongodb.core.MongoTemplate;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;

import de.hdi.mongobumblebee.changeset.ChangeEntry;
import de.hdi.mongobumblebee.dao.ChangeEntryDao;
import de.hdi.mongobumblebee.dao.ChangeEntryIndexDao;
import de.hdi.mongobumblebee.resources.EnvironmentMock;
import de.hdi.mongobumblebee.test.changelogs.EnvironmentDependentTestResource;
import de.hdi.mongobumblebee.utils.EmbeddedMongoDBHelper;

/**
 * Created by lstolowski on 13.07.2017.
 */
@ExtendWith(MockitoExtension.class)
class MongoBumblebeeEnvTest {

	@InjectMocks
	private MongoBumblebee runner = new MongoBumblebee(EmbeddedMongoDBHelper.startMongoClient(), MongoBumblebeeTest.DB_NAME);

	@Mock
	private ChangeEntryDao dao;

	@Mock
	private ChangeEntryIndexDao indexDao;

	private MongoDatabase mongoDatabase;

	@BeforeEach
	void init() throws Exception {
		MongoClient mongoClient = MongoClients.create();
		mongoDatabase = mongoClient.getDatabase(MongoBumblebeeTest.DB_NAME);
		MongoTemplate mongoTemplate = new MongoTemplate(mongoClient, MongoBumblebeeTest.DB_NAME);
		mongoTemplate.dropCollection(MongoBumblebeeTest.CHANGELOG_COLLECTION_NAME);

		when(dao.connectMongoDb(any(MongoClient.class), anyString())).thenReturn(mongoDatabase);
		when(dao.getMongoDatabase()).thenReturn(mongoDatabase);
		when(dao.acquireProcessLock()).thenReturn(true);
		doCallRealMethod().when(dao).save(any(ChangeEntry.class));
		doCallRealMethod().when(dao).setChangelogCollectionName(anyString());
		doCallRealMethod().when(dao).setIndexDao(any(ChangeEntryIndexDao.class));
		dao.setIndexDao(indexDao);
		dao.setChangelogCollectionName(MongoBumblebeeTest.CHANGELOG_COLLECTION_NAME);

		runner.setEnabled(true);
	} // TODO code duplication

	@Test
	void shouldRunChangesetWithEnvironment() throws Exception {
		// given
		runner.setSpringEnvironment(new EnvironmentMock());
		runner.setChangeLogsScanPackage(EnvironmentDependentTestResource.class.getPackage().getName());
		when(dao.isNewChange(any(ChangeEntry.class))).thenReturn(true);

		// when
		runner.execute();

		// then
		long change1 = mongoDatabase.getCollection(MongoBumblebeeTest.CHANGELOG_COLLECTION_NAME).countDocuments(new Document().append(ChangeEntry.KEY_CHANGEID, "Envtest1").append(ChangeEntry.KEY_AUTHOR, MongoBumblebeeTest.USER));
		assertEquals(1, change1);
	}

	@Test
	void shouldRunChangesetWithNullEnvironment() throws Exception {
		// given
		runner.setSpringEnvironment(null);
		runner.setChangeLogsScanPackage(EnvironmentDependentTestResource.class.getPackage().getName());
		when(dao.isNewChange(any(ChangeEntry.class))).thenReturn(true);

		// when
		runner.execute();

		// then
		long change1 = mongoDatabase.getCollection(MongoBumblebeeTest.CHANGELOG_COLLECTION_NAME).countDocuments(new Document().append(ChangeEntry.KEY_CHANGEID, "Envtest1").append(ChangeEntry.KEY_AUTHOR, MongoBumblebeeTest.USER));
		assertEquals(1, change1);
	}
	
	@AfterAll
	public static void close() {
		EmbeddedMongoDBHelper.close();
	}

}
