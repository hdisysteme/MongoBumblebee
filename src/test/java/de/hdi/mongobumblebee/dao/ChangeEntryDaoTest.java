package de.hdi.mongobumblebee.dao;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.MongoTemplate;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import de.hdi.mongobumblebee.MongoBumblebee;
import de.hdi.mongobumblebee.MongoBumblebeeTest;
import de.hdi.mongobumblebee.exception.MongoBumblebeeConfigurationException;
import de.hdi.mongobumblebee.exception.MongoBumblebeeLockException;
import de.hdi.mongobumblebee.utils.EmbeddedMongoDBHelper;

/**
 * @author lstolowski
 * @since 10.12.14
 */
class ChangeEntryDaoTest {

	@Test
	void shouldCreateChangeIdAuthorIndexIfNotFound() throws MongoBumblebeeConfigurationException {

		// given
		ChangeEntryDao dao = new ChangeEntryDao(MongoBumblebeeTest.CHANGELOG_COLLECTION_NAME, MongoBumblebeeTest.LOCK_COLLECTION_NAME, MongoBumblebee.DEFAULT_WAIT_FOR_LOCK,
				MongoBumblebee.DEFAULT_CHANGE_LOG_LOCK_WAIT_TIME, MongoBumblebee.DEFAULT_CHANGE_LOG_LOCK_POLL_RATE, MongoBumblebee.DEFAULT_THROW_EXCEPTION_IF_CANNOT_OBTAIN_LOCK);

		MongoClient mongoClient = mock(MongoClient.class);
		MongoDatabase db = EmbeddedMongoDBHelper.startMongoClient().getDatabase(MongoBumblebeeTest.DB_NAME);

		when(mongoClient.getDatabase(anyString())).thenReturn(db);

		ChangeEntryIndexDao indexDaoMock = mock(ChangeEntryIndexDao.class);
		when(indexDaoMock.findRequiredChangeAndAuthorIndex(any(MongoCollection.class))).thenReturn(null);
		dao.setIndexDao(indexDaoMock);

		// when
		dao.connectMongoDb(mongoClient, MongoBumblebeeTest.DB_NAME);

		//then
		verify(indexDaoMock, times(0)).createRequiredUniqueIndex(any(MongoCollection.class));
		// and not
		verify(indexDaoMock, times(0)).dropIndex(any(MongoCollection.class), any(Document.class));
	}

	@Test
	void shouldNotCreateChangeIdAuthorIndexIfFound() throws MongoBumblebeeConfigurationException {

		// given
		MongoClient mongoClient = mock(MongoClient.class);
		MongoDatabase db = EmbeddedMongoDBHelper.startMongoClient().getDatabase(MongoBumblebeeTest.DB_NAME);
		when(mongoClient.getDatabase(anyString())).thenReturn(db);

		ChangeEntryDao dao = new ChangeEntryDao(MongoBumblebeeTest.CHANGELOG_COLLECTION_NAME, MongoBumblebeeTest.LOCK_COLLECTION_NAME, MongoBumblebee.DEFAULT_WAIT_FOR_LOCK,
				MongoBumblebee.DEFAULT_CHANGE_LOG_LOCK_WAIT_TIME, MongoBumblebee.DEFAULT_CHANGE_LOG_LOCK_POLL_RATE, MongoBumblebee.DEFAULT_THROW_EXCEPTION_IF_CANNOT_OBTAIN_LOCK);
		ChangeEntryIndexDao indexDaoMock = mock(ChangeEntryIndexDao.class);
		when(indexDaoMock.findRequiredChangeAndAuthorIndex(any(MongoCollection.class))).thenReturn(new Document());
		when(indexDaoMock.isUnique(any(Document.class))).thenReturn(true);
		dao.setIndexDao(indexDaoMock);

		// when
		dao.connectMongoDb(mongoClient, MongoBumblebeeTest.DB_NAME);

		//then
		verify(indexDaoMock, times(0)).createRequiredUniqueIndex(db.getCollection(MongoBumblebeeTest.CHANGELOG_COLLECTION_NAME));
		// and not
		verify(indexDaoMock, times(0)).dropIndex(db.getCollection(MongoBumblebeeTest.CHANGELOG_COLLECTION_NAME), new Document());
	}

	@Test
	void shouldRecreateChangeIdAuthorIndexIfFoundNotUnique() throws MongoBumblebeeConfigurationException {

		// given
		MongoClient mongoClient = mock(MongoClient.class);
		MongoDatabase db = EmbeddedMongoDBHelper.startMongoClient().getDatabase(MongoBumblebeeTest.DB_NAME);
		when(mongoClient.getDatabase(anyString())).thenReturn(db);

		ChangeEntryDao dao = new ChangeEntryDao(MongoBumblebeeTest.CHANGELOG_COLLECTION_NAME, MongoBumblebeeTest.LOCK_COLLECTION_NAME, MongoBumblebee.DEFAULT_WAIT_FOR_LOCK,
				MongoBumblebee.DEFAULT_CHANGE_LOG_LOCK_WAIT_TIME, MongoBumblebee.DEFAULT_CHANGE_LOG_LOCK_POLL_RATE, MongoBumblebee.DEFAULT_THROW_EXCEPTION_IF_CANNOT_OBTAIN_LOCK);
		ChangeEntryIndexDao indexDaoMock = mock(ChangeEntryIndexDao.class);
		when(indexDaoMock.findRequiredChangeAndAuthorIndex(any(MongoCollection.class))).thenReturn(new Document());
		when(indexDaoMock.isUnique(any(Document.class))).thenReturn(false);
		dao.setIndexDao(indexDaoMock);

		// when
		dao.connectMongoDb(mongoClient, MongoBumblebeeTest.DB_NAME);

		//then
		verify(indexDaoMock, times(0)).dropIndex(any(MongoCollection.class), any(Document.class));
		// and
		verify(indexDaoMock, times(0)).createRequiredUniqueIndex(any(MongoCollection.class));
	}

	@Test
	void shouldInitiateLock() throws MongoBumblebeeConfigurationException {

		// given
		MongoClient mongoClient = mock(MongoClient.class);
		MongoDatabase db = EmbeddedMongoDBHelper.startMongoClient().getDatabase(MongoBumblebeeTest.DB_NAME);
		when(mongoClient.getDatabase(anyString())).thenReturn(db);

		ChangeEntryDao dao = new ChangeEntryDao(MongoBumblebeeTest.CHANGELOG_COLLECTION_NAME, MongoBumblebeeTest.LOCK_COLLECTION_NAME, MongoBumblebee.DEFAULT_WAIT_FOR_LOCK,
				MongoBumblebee.DEFAULT_CHANGE_LOG_LOCK_WAIT_TIME, MongoBumblebee.DEFAULT_CHANGE_LOG_LOCK_POLL_RATE, MongoBumblebee.DEFAULT_THROW_EXCEPTION_IF_CANNOT_OBTAIN_LOCK);
		ChangeEntryIndexDao indexDaoMock = mock(ChangeEntryIndexDao.class);
		dao.setIndexDao(indexDaoMock);

		LockDao lockDao = mock(LockDao.class);
		dao.setLockDao(lockDao);

		// when
		dao.connectMongoDb(mongoClient, MongoBumblebeeTest.DB_NAME);

		// then
		verify(lockDao).intitializeLock(any(MongoTemplate.class));

	}

	@Test
	void shouldGetLockWhenLockDaoGetsLock() throws Exception {

		// given
		MongoClient mongoClient = mock(MongoClient.class);
		MongoDatabase db = EmbeddedMongoDBHelper.startMongoClient().getDatabase(MongoBumblebeeTest.DB_NAME);
		when(mongoClient.getDatabase(anyString())).thenReturn(db);

		ChangeEntryDao dao = new ChangeEntryDao(MongoBumblebeeTest.CHANGELOG_COLLECTION_NAME, MongoBumblebeeTest.LOCK_COLLECTION_NAME, MongoBumblebee.DEFAULT_WAIT_FOR_LOCK,
				MongoBumblebee.DEFAULT_CHANGE_LOG_LOCK_WAIT_TIME, MongoBumblebee.DEFAULT_CHANGE_LOG_LOCK_POLL_RATE, MongoBumblebee.DEFAULT_THROW_EXCEPTION_IF_CANNOT_OBTAIN_LOCK);

		LockDao lockDao = mock(LockDao.class);
		when(lockDao.acquireLock(any(MongoDatabase.class))).thenReturn(true);
		dao.setLockDao(lockDao);

		dao.connectMongoDb(mongoClient, MongoBumblebeeTest.DB_NAME);

		// when
		boolean hasLock = dao.acquireProcessLock();

		// then
		assertTrue(hasLock);
	}

	@Test
	void shouldWaitForLockIfWaitForLockIsTrue() throws Exception {
		// given
		MongoClient mongoClient = mock(MongoClient.class);
		MongoDatabase db = EmbeddedMongoDBHelper.startMongoClient().getDatabase(MongoBumblebeeTest.DB_NAME);
		when(mongoClient.getDatabase(anyString())).thenReturn(db);

		ChangeEntryDao dao = new ChangeEntryDao(MongoBumblebeeTest.CHANGELOG_COLLECTION_NAME, MongoBumblebeeTest.LOCK_COLLECTION_NAME, true,
				MongoBumblebee.DEFAULT_CHANGE_LOG_LOCK_WAIT_TIME, MongoBumblebee.DEFAULT_CHANGE_LOG_LOCK_POLL_RATE, MongoBumblebee.DEFAULT_THROW_EXCEPTION_IF_CANNOT_OBTAIN_LOCK);

		LockDao lockDao = mock(LockDao.class);
		when(lockDao.acquireLock(any(MongoDatabase.class))).thenReturn(false,true);
		dao.setLockDao(lockDao);

		dao.connectMongoDb(mongoClient, MongoBumblebeeTest.DB_NAME);

		// when
		boolean hasLock = dao.acquireProcessLock();

		// then
		verify(lockDao, times(2)).acquireLock(any(MongoDatabase.class));
		assertTrue(hasLock);
	}

	@Test
	void shouldThrowLockExceptionIfThrowExceptionIsTrue() throws Exception {
		// given
		MongoClient mongoClient = mock(MongoClient.class);
		MongoDatabase db = EmbeddedMongoDBHelper.startMongoClient().getDatabase(MongoBumblebeeTest.DB_NAME);
		when(mongoClient.getDatabase(anyString())).thenReturn(db);

		ChangeEntryDao dao = new ChangeEntryDao(MongoBumblebeeTest.CHANGELOG_COLLECTION_NAME, MongoBumblebeeTest.LOCK_COLLECTION_NAME, MongoBumblebee.DEFAULT_WAIT_FOR_LOCK,
				MongoBumblebee.DEFAULT_CHANGE_LOG_LOCK_WAIT_TIME, MongoBumblebee.DEFAULT_CHANGE_LOG_LOCK_POLL_RATE, true);

		LockDao lockDao = mock(LockDao.class);
		when(lockDao.acquireLock(any(MongoDatabase.class))).thenReturn(false);
		dao.setLockDao(lockDao);

		dao.connectMongoDb(mongoClient, MongoBumblebeeTest.DB_NAME);

		assertThrows(MongoBumblebeeLockException.class, () -> dao.acquireProcessLock());
	}

	@Test
	void shouldReleaseLockFromLockDao() throws Exception {

		// given
		MongoClient mongoClient = mock(MongoClient.class);
		MongoDatabase db = EmbeddedMongoDBHelper.startMongoClient().getDatabase(MongoBumblebeeTest.DB_NAME);
		when(mongoClient.getDatabase(anyString())).thenReturn(db);

		ChangeEntryDao dao = new ChangeEntryDao(MongoBumblebeeTest.CHANGELOG_COLLECTION_NAME, MongoBumblebeeTest.LOCK_COLLECTION_NAME, MongoBumblebee.DEFAULT_WAIT_FOR_LOCK,
				MongoBumblebee.DEFAULT_CHANGE_LOG_LOCK_WAIT_TIME, MongoBumblebee.DEFAULT_CHANGE_LOG_LOCK_POLL_RATE, MongoBumblebee.DEFAULT_THROW_EXCEPTION_IF_CANNOT_OBTAIN_LOCK);

		LockDao lockDao = mock(LockDao.class);
		dao.setLockDao(lockDao);

		dao.connectMongoDb(mongoClient, MongoBumblebeeTest.DB_NAME);

		// when
		dao.releaseProcessLock();

		// then
		verify(lockDao).releaseLock(any(MongoDatabase.class));
	}

	@Test
	void shouldCheckLockHeldFromFromLockDao() throws Exception {

		// given
		MongoClient mongoClient = mock(MongoClient.class);
		MongoDatabase db = EmbeddedMongoDBHelper.startMongoClient().getDatabase(MongoBumblebeeTest.DB_NAME);
		when(mongoClient.getDatabase(anyString())).thenReturn(db);

		ChangeEntryDao dao = new ChangeEntryDao(MongoBumblebeeTest.CHANGELOG_COLLECTION_NAME, MongoBumblebeeTest.LOCK_COLLECTION_NAME, MongoBumblebee.DEFAULT_WAIT_FOR_LOCK,
				MongoBumblebee.DEFAULT_CHANGE_LOG_LOCK_WAIT_TIME, MongoBumblebee.DEFAULT_CHANGE_LOG_LOCK_POLL_RATE, MongoBumblebee.DEFAULT_THROW_EXCEPTION_IF_CANNOT_OBTAIN_LOCK);

		LockDao lockDao = mock(LockDao.class);
		dao.setLockDao(lockDao);

		dao.connectMongoDb(mongoClient, MongoBumblebeeTest.DB_NAME);

		// when
		when(lockDao.isLockHeld(db)).thenReturn(true);

		boolean lockHeld = dao.isProccessLockHeld();

		// then
		assertTrue(lockHeld);
	}

}
