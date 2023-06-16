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
import de.hdi.mongobumblebee.test.changelogs.AnotherMongoBumblebeeTestResource;
import de.hdi.mongobumblebee.test.profiles.def.UnProfiledChangeLog;
import de.hdi.mongobumblebee.test.profiles.dev.ProfiledDevChangeLog;
import de.hdi.mongobumblebee.utils.EmbeddedMongoDBHelper;

/**
 * Tests for Spring profiles integration
 *
 * @author lstolowski
 * @since 2014-09-17
 */
@ExtendWith(MockitoExtension.class)
class MongoBumblebeeProfileTest {

	/**
	 * Number of all changelogs in de.hdi.mongobumblebee.test.changelogs
	 */
	private static final int CHANGELOG_COUNT = 9;

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
//		when(dao.getMongoDatabase()).thenReturn(mongoDatabase);
		when(dao.acquireProcessLock()).thenReturn(true);
//		doCallRealMethod().when(dao).save(any(ChangeEntry.class));
		doCallRealMethod().when(dao).setChangelogCollectionName(anyString());
		doCallRealMethod().when(dao).setIndexDao(any(ChangeEntryIndexDao.class));
		dao.setIndexDao(indexDao);
		dao.setChangelogCollectionName(MongoBumblebeeTest.CHANGELOG_COLLECTION_NAME);

		runner.setEnabled(true);
	} // TODO code duplication

	@Test
	void shouldRunDevProfileAndNonAnnotated() throws Exception {
		// given
		runner.setSpringEnvironment(new EnvironmentMock("dev", "test"));
		runner.setChangeLogsScanPackage(ProfiledDevChangeLog.class.getPackage().getName());
		when(dao.isNewChange(any(ChangeEntry.class))).thenReturn(true);
		when(dao.getMongoDatabase()).thenReturn(mongoDatabase);
		doCallRealMethod().when(dao).save(any(ChangeEntry.class));

		// when
		runner.execute();

		// then
		long change1 = mongoDatabase.getCollection(MongoBumblebeeTest.CHANGELOG_COLLECTION_NAME)
			.countDocuments(new Document().append(ChangeEntry.KEY_CHANGEID, "Pdev1")
			.append(ChangeEntry.KEY_AUTHOR, MongoBumblebeeTest.USER));
		assertEquals(1, change1); // no-@Profile should not match

		long change2 = mongoDatabase.getCollection(MongoBumblebeeTest.CHANGELOG_COLLECTION_NAME)
			.countDocuments(new Document().append(ChangeEntry.KEY_CHANGEID, "Pdev4")
			.append(ChangeEntry.KEY_AUTHOR, MongoBumblebeeTest.USER));
		assertEquals(1, change2); // @Profile("dev") should not match

		long change3 = mongoDatabase.getCollection(MongoBumblebeeTest.CHANGELOG_COLLECTION_NAME)
			.countDocuments(new Document().append(ChangeEntry.KEY_CHANGEID, "Pdev3")
			.append(ChangeEntry.KEY_AUTHOR, MongoBumblebeeTest.USER));
		assertEquals(0, change3); // @Profile("default") should not match
	}

	@Test
	void shouldRunUnprofiledChangeLog() throws Exception {
		// given
		runner.setSpringEnvironment(new EnvironmentMock("test"));
		runner.setChangeLogsScanPackage(UnProfiledChangeLog.class.getPackage().getName());
		when(dao.isNewChange(any(ChangeEntry.class))).thenReturn(true);
		when(dao.getMongoDatabase()).thenReturn(mongoDatabase);
		doCallRealMethod().when(dao).save(any(ChangeEntry.class));

		// when
		runner.execute();

		// then
		long change1 = mongoDatabase.getCollection(MongoBumblebeeTest.CHANGELOG_COLLECTION_NAME)
			.countDocuments(new Document()
			.append(ChangeEntry.KEY_CHANGEID, "Pdev1")
			.append(ChangeEntry.KEY_AUTHOR, MongoBumblebeeTest.USER));
		assertEquals(1, change1);

		long change2 = mongoDatabase.getCollection(MongoBumblebeeTest.CHANGELOG_COLLECTION_NAME)
			.countDocuments(new Document()
			.append(ChangeEntry.KEY_CHANGEID, "Pdev2")
			.append(ChangeEntry.KEY_AUTHOR, MongoBumblebeeTest.USER));
		assertEquals(1, change2);

		long change3 = mongoDatabase.getCollection(MongoBumblebeeTest.CHANGELOG_COLLECTION_NAME)
			.countDocuments(new Document()
			.append(ChangeEntry.KEY_CHANGEID, "Pdev3")
			.append(ChangeEntry.KEY_AUTHOR, MongoBumblebeeTest.USER));
		assertEquals(1, change3); // @Profile("dev") should not match

		long change4 = mongoDatabase.getCollection(MongoBumblebeeTest.CHANGELOG_COLLECTION_NAME)
			.countDocuments(new Document()
			.append(ChangeEntry.KEY_CHANGEID, "Pdev4")
			.append(ChangeEntry.KEY_AUTHOR, MongoBumblebeeTest.USER));
		assertEquals(0, change4); // @Profile("pro") should not match

		long change5 = mongoDatabase.getCollection(MongoBumblebeeTest.CHANGELOG_COLLECTION_NAME)
			.countDocuments(new Document()
			.append(ChangeEntry.KEY_CHANGEID, "Pdev5")
			.append(ChangeEntry.KEY_AUTHOR, MongoBumblebeeTest.USER));
		assertEquals(1, change5); // @Profile("!pro") should match
	}

	@Test
	void shouldNotRunAnyChangeSet() throws Exception {
		// given
		runner.setSpringEnvironment(new EnvironmentMock("foobar"));
		runner.setChangeLogsScanPackage(ProfiledDevChangeLog.class.getPackage().getName());
//		when(dao.isNewChange(any(ChangeEntry.class))).thenReturn(true);

		// when
		runner.execute();

		// then
		long changes = mongoDatabase.getCollection(MongoBumblebeeTest.CHANGELOG_COLLECTION_NAME).countDocuments(new Document());
		assertEquals(0, changes);
	}

	@Test
	void shouldRunChangeSetsWhenNoEnv() throws Exception {
		// given
		runner.setSpringEnvironment(null);
		runner.setChangeLogsScanPackage(AnotherMongoBumblebeeTestResource.class.getPackage().getName());
		when(dao.isNewChange(any(ChangeEntry.class))).thenReturn(true);
		when(dao.getMongoDatabase()).thenReturn(mongoDatabase);
		doCallRealMethod().when(dao).save(any(ChangeEntry.class));

		// when
		runner.execute();

		// then
		long changes = mongoDatabase.getCollection(MongoBumblebeeTest.CHANGELOG_COLLECTION_NAME).countDocuments(new Document());
		assertEquals(CHANGELOG_COUNT, changes);
	}

	@Test
	void shouldRunChangeSetsWhenEmptyEnv() throws Exception {
		// given
		runner.setSpringEnvironment(new EnvironmentMock());
		runner.setChangeLogsScanPackage(AnotherMongoBumblebeeTestResource.class.getPackage().getName());
		when(dao.isNewChange(any(ChangeEntry.class))).thenReturn(true);
		when(dao.getMongoDatabase()).thenReturn(mongoDatabase);
		doCallRealMethod().when(dao).save(any(ChangeEntry.class));

		// when
		runner.execute();

		// then
		long changes = mongoDatabase.getCollection(MongoBumblebeeTest.CHANGELOG_COLLECTION_NAME).countDocuments(new Document());
		assertEquals(CHANGELOG_COUNT, changes);
	}

	@Test
	void shouldRunAllChangeSets() throws Exception {
		// given
		runner.setSpringEnvironment(new EnvironmentMock("dev"));
		runner.setChangeLogsScanPackage(AnotherMongoBumblebeeTestResource.class.getPackage().getName());
		when(dao.isNewChange(any(ChangeEntry.class))).thenReturn(true);
		when(dao.getMongoDatabase()).thenReturn(mongoDatabase);
		doCallRealMethod().when(dao).save(any(ChangeEntry.class));

		// when
		runner.execute();

		// then
		long changes = mongoDatabase.getCollection(MongoBumblebeeTest.CHANGELOG_COLLECTION_NAME).countDocuments(new Document());
		assertEquals(CHANGELOG_COUNT, changes);
	}
	
	@AfterAll
	public static void close() {
		EmbeddedMongoDBHelper.close();
	}

}
