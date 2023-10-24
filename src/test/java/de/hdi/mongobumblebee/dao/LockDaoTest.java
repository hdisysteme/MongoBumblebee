package de.hdi.mongobumblebee.dao;


import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.MongoTemplate;

import com.mongodb.client.MongoDatabase;

import de.hdi.mongobumblebee.MongoBumblebeeTest;
import de.hdi.mongobumblebee.utils.EmbeddedMongoDBHelper;

/**
 * @author colsson11
 * @since 13.01.15
 */
class LockDaoTest {

	private LockDao createDao(MongoDatabase db) {
		
		LockDao dao = new LockDao(MongoBumblebeeTest.LOCK_COLLECTION_NAME);
		dao.intitializeLock(new MongoTemplate(EmbeddedMongoDBHelper.startMongoClient(), db.getName()));
		return dao;
	}

	@Test
	void shouldGetLockWhenNotPreviouslyHeld() throws Exception {

		// given
		MongoDatabase db = EmbeddedMongoDBHelper.startMongoClient().getDatabase(MongoBumblebeeTest.DB_NAME);
		LockDao dao = createDao(db);

		// when
		boolean hasLock = dao.acquireLock(db);

		// then
		assertTrue(hasLock);
	}

	@Test
	void shouldNotGetLockWhenPreviouslyHeld() throws Exception {

		// given
		MongoDatabase db = EmbeddedMongoDBHelper.startMongoClient().getDatabase(MongoBumblebeeTest.DB_NAME);
		LockDao dao = createDao(db);

		// when
		dao.acquireLock(db);
		boolean hasLock = dao.acquireLock(db);

		// then
		assertFalse(hasLock);
	}

	@Test
	void shouldGetLockWhenPreviouslyHeldAndReleased() throws Exception {

		// given
		MongoDatabase db = EmbeddedMongoDBHelper.startMongoClient().getDatabase(MongoBumblebeeTest.DB_NAME);
		LockDao dao = createDao(db);

		// when
		dao.acquireLock(db);
		dao.releaseLock(db);
		boolean hasLock = dao.acquireLock(db);

		// then
		assertTrue(hasLock);
	}

	@Test
	void releaseLockShouldBeIdempotent() {

		// given
		MongoDatabase db = EmbeddedMongoDBHelper.startMongoClient().getDatabase(MongoBumblebeeTest.DB_NAME);
		LockDao dao = createDao(db);

		// when
		dao.releaseLock(db);
		dao.releaseLock(db);
		boolean hasLock = dao.acquireLock(db);

		// then
		assertTrue(hasLock);
	}

	@Test
	void whenLockNotHeldCheckReturnsFalse() {
		
		MongoDatabase db = EmbeddedMongoDBHelper.startMongoClient().getDatabase(MongoBumblebeeTest.DB_NAME);
		LockDao dao = createDao(db);

		assertFalse(dao.isLockHeld(db));
	}

	@Test
	void whenLockHeldCheckReturnsTrue() {

		MongoDatabase db = EmbeddedMongoDBHelper.startMongoClient().getDatabase(MongoBumblebeeTest.DB_NAME);
		LockDao dao = createDao(db);

		dao.acquireLock(db);

		assertTrue(dao.isLockHeld(db));
	}

	@AfterEach
	void cleanup() {
		
		MongoDatabase db = EmbeddedMongoDBHelper.startMongoClient().getDatabase(MongoBumblebeeTest.DB_NAME);
		LockDao dao = createDao(db);

		dao.releaseLock(db);
	}

}
