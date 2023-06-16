package de.hdi.mongobumblebee;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.UnknownHostException;

import org.bson.Document;
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
import de.hdi.mongobumblebee.exception.MongoBumblebeeConfigurationException;
import de.hdi.mongobumblebee.exception.MongoBumblebeeException;
import de.hdi.mongobumblebee.test.changelogs.MongoBumblebeeTestResource;

@ExtendWith(MockitoExtension.class)
public class MongoBumblebeeTest {

	public static final String USER = "testuser";
	
	public static final String DB_NAME = "MongoBumblebeeTest";
	
	public static final String CHANGELOG_COLLECTION_NAME = "changelog";
	
	public static final String LOCK_COLLECTION_NAME = "lock";

	@InjectMocks
	private MongoBumblebee runner = new MongoBumblebee("mongodb://localhost:27017/", MongoBumblebeeTest.DB_NAME);

	@Mock
	private ChangeEntryDao dao;

	@Mock
	private ChangeEntryIndexDao indexDao;

	private MongoDatabase mongoDatabase;

	@BeforeEach
	void init() throws MongoBumblebeeException, UnknownHostException {
		MongoClient mongoClient = MongoClients.create();
		mongoDatabase = mongoClient.getDatabase(DB_NAME);
		MongoTemplate mongoTemplate = new MongoTemplate(mongoClient, DB_NAME);
		mongoTemplate.dropCollection(CHANGELOG_COLLECTION_NAME);

		doCallRealMethod().when(dao).setChangelogCollectionName(anyString());
		doCallRealMethod().when(dao).setIndexDao(any(ChangeEntryIndexDao.class));
		dao.setIndexDao(indexDao);
		dao.setChangelogCollectionName(CHANGELOG_COLLECTION_NAME);

		runner.setEnabled(true);
		runner.setChangeLogsScanPackage(MongoBumblebeeTestResource.class.getPackage().getName());
	}

	@Test
	void shouldThrowAnExceptionIfNoDbNameSet() throws Exception {
		MongoBumblebee runner = new MongoBumblebee("mongodb://localhost:27017/", "");
		runner.setEnabled(true);
		runner.setChangeLogsScanPackage(MongoBumblebeeTestResource.class.getPackage().getName());
		assertThrows(MongoBumblebeeConfigurationException.class, () -> runner.execute());
	}

	@Test
	void shouldExecuteAllChangeSets() throws Exception {
		// given
		when(dao.acquireProcessLock()).thenReturn(true);
		when(dao.isNewChange(any(ChangeEntry.class))).thenReturn(true);
		when(dao.connectMongoDb(any(MongoClient.class), anyString())).thenReturn(mongoDatabase);
		when(dao.getMongoDatabase()).thenReturn(mongoDatabase);
		doCallRealMethod().when(dao).save(any(ChangeEntry.class));

		// when
		runner.execute();

		// then
		verify(dao, times(9)).save(any(ChangeEntry.class)); // 9 changesets saved to dbchangelog

		// dbchangelog collection checking
		long change1 = mongoDatabase.getCollection(CHANGELOG_COLLECTION_NAME)
			.countDocuments(new Document()
			.append(ChangeEntry.KEY_CHANGEID, "test1")
			.append(ChangeEntry.KEY_AUTHOR, MongoBumblebeeTest.USER));
		assertEquals(1, change1);
		long change2 = mongoDatabase.getCollection(CHANGELOG_COLLECTION_NAME)
			.countDocuments(new Document()
			.append(ChangeEntry.KEY_CHANGEID, "test2")
			.append(ChangeEntry.KEY_AUTHOR, MongoBumblebeeTest.USER));
		assertEquals(1, change2);
		long change3 = mongoDatabase.getCollection(CHANGELOG_COLLECTION_NAME)
			.countDocuments(new Document()
			.append(ChangeEntry.KEY_CHANGEID, "test3")
			.append(ChangeEntry.KEY_AUTHOR, MongoBumblebeeTest.USER));
		assertEquals(1, change3);
		long change4 = mongoDatabase.getCollection(CHANGELOG_COLLECTION_NAME)
			.countDocuments(new Document()
			.append(ChangeEntry.KEY_CHANGEID, "test4")
			.append(ChangeEntry.KEY_AUTHOR, MongoBumblebeeTest.USER));
		assertEquals(0, change4); // changeset does not exist

		long changeAll = mongoDatabase.getCollection(CHANGELOG_COLLECTION_NAME)
			.countDocuments(new Document()
			.append(ChangeEntry.KEY_AUTHOR, MongoBumblebeeTest.USER));
		assertEquals(8, changeAll);
	}

	@Test
	void shouldPassOverChangeSets() throws Exception {
		// given

		// when
		runner.execute();

		// then
		verify(dao, times(0)).save(any(ChangeEntry.class)); // no changesets saved to dbchangelog
	}

	@Test
	void shouldExecuteProcessWhenLockAcquired() throws Exception {
		// given
		when(dao.acquireProcessLock()).thenReturn(true);
		when(dao.connectMongoDb(any(MongoClient.class), anyString())).thenReturn(mongoDatabase);
		when(dao.getMongoDatabase()).thenReturn(mongoDatabase);

		// when
		runner.execute();

		// then
		verify(dao, atLeastOnce()).isNewChange(any(ChangeEntry.class));
	}

	@Test
	void shouldReleaseLockAfterWhenLockAcquired() throws Exception {
		// given
		when(dao.acquireProcessLock()).thenReturn(true);
		when(dao.connectMongoDb(any(MongoClient.class), anyString())).thenReturn(mongoDatabase);
		when(dao.getMongoDatabase()).thenReturn(mongoDatabase);

		// when
		runner.execute();

		// then
		verify(dao).releaseProcessLock();
	}

	@Test
	void shouldNotExecuteProcessWhenLockNotAcquired() throws Exception {
		// given
		when(dao.acquireProcessLock()).thenReturn(false);
		when(dao.connectMongoDb(any(MongoClient.class), anyString())).thenReturn(mongoDatabase);

		// when
		runner.execute();

		// then
		verify(dao, never()).isNewChange(any(ChangeEntry.class));
	}

	@Test
	void shouldReturnExecutionStatusBasedOnDao() throws Exception {
		// given
		when(dao.isProccessLockHeld()).thenReturn(true);

		boolean inProgress = runner.isExecutionInProgress();

		// then
		assertTrue(inProgress);
	}

	@Test
	void shouldReleaseLockWhenExceptionInMigration() throws Exception {

		// given
		// would be nicer with a mock for the whole execution, but this would mean breaking out to separate class..
		// this should be "good enough"
		when(dao.acquireProcessLock()).thenReturn(true);
		when(dao.connectMongoDb(any(MongoClient.class), anyString())).thenReturn(mongoDatabase);
		when(dao.isNewChange(any(ChangeEntry.class))).thenThrow(RuntimeException.class);

		// when
		// have to catch the exception to be able to verify after
		try {
			runner.execute();
		} catch (Exception e) {
			// do nothing
		}
		// then
		verify(dao).releaseProcessLock();

	}

}
