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

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import de.hdi.mongobumblebee.MongoBumblebeeTest;
import de.hdi.mongobumblebee.dao.ChangeEntryDao;
import de.hdi.mongobumblebee.dao.ChangeEntryIndexDao;
import de.hdi.mongobumblebee.dao.LockDao;
import de.hdi.mongobumblebee.exception.MongoBumblebeeConfigurationException;
import de.hdi.mongobumblebee.exception.MongoBumblebeeLockException;

/**
 * @author lstolowski
 * @since 10.12.14
 */
class ChangeEntryDaoTest {

	private static final boolean WAIT_FOR_LOCK = false;
	private static final long CHANGE_LOG_LOCK_WAIT_TIME = 5L;
	private static final long CHANGE_LOG_LOCK_POLL_RATE = 10L;
	private static final boolean THROW_EXCEPTION_IF_CANNOT_OBTAIN_LOCK = false;

	@Test
	void shouldCreateChangeIdAuthorIndexIfNotFound() throws MongoBumblebeeConfigurationException {

		// given
		ChangeEntryDao dao = new ChangeEntryDao(MongoBumblebeeTest.CHANGELOG_COLLECTION_NAME, MongoBumblebeeTest.LOCK_COLLECTION_NAME, WAIT_FOR_LOCK,
				CHANGE_LOG_LOCK_WAIT_TIME, CHANGE_LOG_LOCK_POLL_RATE, THROW_EXCEPTION_IF_CANNOT_OBTAIN_LOCK);

		MongoClient mongoClient = mock(MongoClient.class);
		MongoDatabase db = MongoClients.create().getDatabase(MongoBumblebeeTest.DB_NAME);

		when(mongoClient.getDatabase(anyString())).thenReturn(db);

		ChangeEntryIndexDao indexDaoMock = mock(ChangeEntryIndexDao.class);
		when(indexDaoMock.findRequiredChangeAndAuthorIndex(db)).thenReturn(null);
		dao.setIndexDao(indexDaoMock);

		// when
		dao.connectMongoDb(mongoClient, MongoBumblebeeTest.DB_NAME);

		//then
		verify(indexDaoMock, times(1)).createRequiredUniqueIndex(any(MongoCollection.class));
		// and not
		verify(indexDaoMock, times(0)).dropIndex(any(MongoCollection.class), any(Document.class));
	}

	@Test
	void shouldNotCreateChangeIdAuthorIndexIfFound() throws MongoBumblebeeConfigurationException {

		// given
		MongoClient mongoClient = mock(MongoClient.class);
		MongoDatabase db = MongoClients.create().getDatabase(MongoBumblebeeTest.DB_NAME);
		when(mongoClient.getDatabase(anyString())).thenReturn(db);

		ChangeEntryDao dao = new ChangeEntryDao(MongoBumblebeeTest.CHANGELOG_COLLECTION_NAME, MongoBumblebeeTest.LOCK_COLLECTION_NAME, WAIT_FOR_LOCK,
				CHANGE_LOG_LOCK_WAIT_TIME, CHANGE_LOG_LOCK_POLL_RATE, THROW_EXCEPTION_IF_CANNOT_OBTAIN_LOCK);
		ChangeEntryIndexDao indexDaoMock = mock(ChangeEntryIndexDao.class);
		when(indexDaoMock.findRequiredChangeAndAuthorIndex(db)).thenReturn(new Document());
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
		MongoDatabase db = MongoClients.create().getDatabase(MongoBumblebeeTest.DB_NAME);
		when(mongoClient.getDatabase(anyString())).thenReturn(db);

		ChangeEntryDao dao = new ChangeEntryDao(MongoBumblebeeTest.CHANGELOG_COLLECTION_NAME, MongoBumblebeeTest.LOCK_COLLECTION_NAME, WAIT_FOR_LOCK,
				CHANGE_LOG_LOCK_WAIT_TIME, CHANGE_LOG_LOCK_POLL_RATE, THROW_EXCEPTION_IF_CANNOT_OBTAIN_LOCK);
		ChangeEntryIndexDao indexDaoMock = mock(ChangeEntryIndexDao.class);
		when(indexDaoMock.findRequiredChangeAndAuthorIndex(db)).thenReturn(new Document());
		when(indexDaoMock.isUnique(any(Document.class))).thenReturn(false);
		dao.setIndexDao(indexDaoMock);

		// when
		dao.connectMongoDb(mongoClient, MongoBumblebeeTest.DB_NAME);

		//then
		verify(indexDaoMock, times(1)).dropIndex(any(MongoCollection.class), any(Document.class));
		// and
		verify(indexDaoMock, times(1)).createRequiredUniqueIndex(any(MongoCollection.class));
	}

	@Test
	void shouldInitiateLock() throws MongoBumblebeeConfigurationException {

		// given
		MongoClient mongoClient = mock(MongoClient.class);
		MongoDatabase db = MongoClients.create().getDatabase(MongoBumblebeeTest.DB_NAME);
		when(mongoClient.getDatabase(anyString())).thenReturn(db);

		ChangeEntryDao dao = new ChangeEntryDao(MongoBumblebeeTest.CHANGELOG_COLLECTION_NAME, MongoBumblebeeTest.LOCK_COLLECTION_NAME, WAIT_FOR_LOCK,
				CHANGE_LOG_LOCK_WAIT_TIME, CHANGE_LOG_LOCK_POLL_RATE, THROW_EXCEPTION_IF_CANNOT_OBTAIN_LOCK);
		ChangeEntryIndexDao indexDaoMock = mock(ChangeEntryIndexDao.class);
		dao.setIndexDao(indexDaoMock);

		LockDao lockDao = mock(LockDao.class);
		dao.setLockDao(lockDao);

		// when
		dao.connectMongoDb(mongoClient, MongoBumblebeeTest.DB_NAME);

		// then
		verify(lockDao).intitializeLock(db);

	}

	@Test
	void shouldGetLockWhenLockDaoGetsLock() throws Exception {

		// given
		MongoClient mongoClient = mock(MongoClient.class);
		MongoDatabase db = MongoClients.create().getDatabase(MongoBumblebeeTest.DB_NAME);
		when(mongoClient.getDatabase(anyString())).thenReturn(db);

		ChangeEntryDao dao = new ChangeEntryDao(MongoBumblebeeTest.CHANGELOG_COLLECTION_NAME, MongoBumblebeeTest.LOCK_COLLECTION_NAME, WAIT_FOR_LOCK,
				CHANGE_LOG_LOCK_WAIT_TIME, CHANGE_LOG_LOCK_POLL_RATE, THROW_EXCEPTION_IF_CANNOT_OBTAIN_LOCK);

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
		MongoDatabase db = MongoClients.create().getDatabase(MongoBumblebeeTest.DB_NAME);
		when(mongoClient.getDatabase(anyString())).thenReturn(db);

		ChangeEntryDao dao = new ChangeEntryDao(MongoBumblebeeTest.CHANGELOG_COLLECTION_NAME, MongoBumblebeeTest.LOCK_COLLECTION_NAME, true,
				CHANGE_LOG_LOCK_WAIT_TIME, CHANGE_LOG_LOCK_POLL_RATE, THROW_EXCEPTION_IF_CANNOT_OBTAIN_LOCK);

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
		MongoDatabase db = MongoClients.create().getDatabase(MongoBumblebeeTest.DB_NAME);
		when(mongoClient.getDatabase(anyString())).thenReturn(db);

		ChangeEntryDao dao = new ChangeEntryDao(MongoBumblebeeTest.CHANGELOG_COLLECTION_NAME, MongoBumblebeeTest.LOCK_COLLECTION_NAME, WAIT_FOR_LOCK,
				CHANGE_LOG_LOCK_WAIT_TIME, CHANGE_LOG_LOCK_POLL_RATE, true);

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
		MongoDatabase db = MongoClients.create().getDatabase(MongoBumblebeeTest.DB_NAME);
		when(mongoClient.getDatabase(anyString())).thenReturn(db);

		ChangeEntryDao dao = new ChangeEntryDao(MongoBumblebeeTest.CHANGELOG_COLLECTION_NAME, MongoBumblebeeTest.LOCK_COLLECTION_NAME, WAIT_FOR_LOCK,
				CHANGE_LOG_LOCK_WAIT_TIME, CHANGE_LOG_LOCK_POLL_RATE, THROW_EXCEPTION_IF_CANNOT_OBTAIN_LOCK);

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
		MongoDatabase db = MongoClients.create().getDatabase(MongoBumblebeeTest.DB_NAME);
		when(mongoClient.getDatabase(anyString())).thenReturn(db);

		ChangeEntryDao dao = new ChangeEntryDao(MongoBumblebeeTest.CHANGELOG_COLLECTION_NAME, MongoBumblebeeTest.LOCK_COLLECTION_NAME, WAIT_FOR_LOCK,
				CHANGE_LOG_LOCK_WAIT_TIME, CHANGE_LOG_LOCK_POLL_RATE, THROW_EXCEPTION_IF_CANNOT_OBTAIN_LOCK);

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
