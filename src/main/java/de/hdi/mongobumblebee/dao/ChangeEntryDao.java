package de.hdi.mongobumblebee.dao;

import static org.springframework.util.StringUtils.hasText;

import java.time.LocalDateTime;
import java.util.Date;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

import de.hdi.mongobumblebee.changeset.ChangeEntry;
import de.hdi.mongobumblebee.exception.MongoBumblebeeConfigurationException;
import de.hdi.mongobumblebee.exception.MongoBumblebeeConnectionException;
import de.hdi.mongobumblebee.exception.MongoBumblebeeLockException;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * @author lstolowski
 * @since 27/07/2014
 */
@Slf4j
public class ChangeEntryDao {
	
	@Getter
	private MongoDatabase mongoDatabase;
	private MongoClient mongoClient;
	private ChangeEntryIndexDao indexDao;
	private String changelogCollectionName;
	@Getter
	@Setter
	private boolean waitForLock;
	@Getter
	@Setter
	private long changeLogLockWaitTime;
	@Getter
	@Setter
	private long changeLogLockPollRate;
	private boolean throwExceptionIfCannotObtainLock;

	private LockDao lockDao;

	public ChangeEntryDao(String changelogCollectionName, String lockCollectionName, boolean waitForLock, long changeLogLockWaitTime,
			long changeLogLockPollRate, boolean throwExceptionIfCannotObtainLock) {
		this.indexDao = new ChangeEntryIndexDao();
		this.lockDao = new LockDao(lockCollectionName);
		this.changelogCollectionName = changelogCollectionName;
		this.waitForLock = waitForLock;
		this.changeLogLockWaitTime = changeLogLockWaitTime;
		this.changeLogLockPollRate = changeLogLockPollRate;
		this.throwExceptionIfCannotObtainLock = throwExceptionIfCannotObtainLock;
	}

	public MongoDatabase connectMongoDb(MongoClient mongoClient, String dbName) throws MongoBumblebeeConfigurationException {
		if (!hasText(dbName)) {
			throw new MongoBumblebeeConfigurationException("DB name is not set. Should be defined in MongoDB URI or via setter");
		} 

		this.mongoClient = mongoClient;

		mongoDatabase = mongoClient.getDatabase(dbName);

		ensureChangeLogCollectionIndex(mongoDatabase.getCollection(changelogCollectionName));
		initializeLock();
		return mongoDatabase;		
	}

	/**
	 * Try to acquire process lock
	 *
	 * @return true if successfully acquired, false otherwise
	 * @throws MongoBumblebeeConnectionException exception
	 * @throws MongoBumblebeeLockException exception
	 */
	public boolean acquireProcessLock() throws MongoBumblebeeConnectionException, MongoBumblebeeLockException {
		verifyDbConnection();
		boolean acquired = lockDao.acquireLock(getMongoDatabase());

		if (!acquired && waitForLock) {
			long timeToGiveUp = new Date().getTime() + (changeLogLockWaitTime * 1000 * 60);
			while (!acquired && new Date().getTime() < timeToGiveUp) {
				acquired = lockDao.acquireLock(getMongoDatabase());
				if (!acquired) {
					log.info("Waiting for process lock....");
					try {
						Thread.sleep(changeLogLockPollRate * 1000);
					} catch (InterruptedException e) {
						// nothing
					}
				}
			}
		}
		
		LocalDateTime lastAccess = lockDao.getLastAccess(getMongoDatabase());
		if (!acquired && lastAccess != null && lastAccess.plusMinutes(changeLogLockWaitTime).isBefore(LocalDateTime.now())) {
			log.info("Process lock released because it wasn't updated for " + changeLogLockWaitTime + " minutes.");
			lockDao.releaseLock(getMongoDatabase());
			acquired = lockDao.acquireLock(getMongoDatabase());
		}

		if (!acquired && throwExceptionIfCannotObtainLock) {
			log.info("MongoBumblebee did not acquire process lock. Throwing exception.");
			throw new MongoBumblebeeLockException("Could not acquire process lock");
		}

		return acquired;
	}
	
	public void updateLock() {
		lockDao.updateLock(getMongoDatabase());
	}

	public void releaseProcessLock() throws MongoBumblebeeConnectionException {
		verifyDbConnection();
		lockDao.releaseLock(getMongoDatabase());
	}

	public boolean isProccessLockHeld() throws MongoBumblebeeConnectionException {
		verifyDbConnection();
		return lockDao.isLockHeld(getMongoDatabase());
	}

	public boolean isNewChange(ChangeEntry changeEntry) throws MongoBumblebeeConnectionException {
		verifyDbConnection();

		MongoCollection<Document> changeLogCollection = getMongoDatabase().getCollection(changelogCollectionName);
		Document entry = changeLogCollection.find(changeEntry.buildSearchQueryDBObject()).first();

		return entry == null;
	}

	public void save(ChangeEntry changeEntry) throws MongoBumblebeeConnectionException {
		verifyDbConnection();

		MongoCollection<Document> changeLogCollection = getMongoDatabase().getCollection(changelogCollectionName);

		if (isNewChange(changeEntry)) {
			changeLogCollection.insertOne(changeEntry.buildFullDBObject());
		} else {
			Bson filter = Filters.and(Filters.eq(ChangeEntry.KEY_CHANGEID, changeEntry.getChangeId()), Filters.eq(ChangeEntry.KEY_AUTHOR, changeEntry.getAuthor()));
			var document = changeEntry.buildFullDBObject();
			changeLogCollection.findOneAndReplace(filter, document);
		}
	}

	private void verifyDbConnection() throws MongoBumblebeeConnectionException {
		if (getMongoDatabase() == null) {
			throw new MongoBumblebeeConnectionException("Database is not connected. MongoBumblebee has thrown an unexpected error",
					new NullPointerException());
		}
	}

	private void ensureChangeLogCollectionIndex(MongoCollection<Document> collection) {
		Document index = indexDao.findRequiredChangeAndAuthorIndex(collection);
		if (index == null) {
			indexDao.createRequiredUniqueIndex(collection);
			log.debug("Index in collection " + changelogCollectionName + " was created");
		} else if (!indexDao.isUnique(index)) {
			indexDao.dropIndex(collection, index);
			indexDao.createRequiredUniqueIndex(collection);
			log.debug("Index in collection " + changelogCollectionName + " was recreated");
		}
	}

	public void close() {
		this.mongoClient.close();
	}

	private void initializeLock() {
		lockDao.intitializeLock(mongoDatabase);
	}

	public void setIndexDao(ChangeEntryIndexDao changeEntryIndexDao) {
		this.indexDao = changeEntryIndexDao;
	}

	/* Visible for testing */
	void setLockDao(LockDao lockDao) {
		this.lockDao = lockDao;
	}

	public void setChangelogCollectionName(String changelogCollectionName) {
		this.changelogCollectionName = changelogCollectionName;
	}

	public void setLockCollectionName(String lockCollectionName) {
		this.lockDao.setLockCollectionName(lockCollectionName);
	}

	public boolean isThrowExceptionIfCannotObtainLock() {
		return throwExceptionIfCannotObtainLock;
	}

	public void setThrowExceptionIfCannotObtainLock(boolean throwExceptionIfCannotObtainLock) {
		this.throwExceptionIfCannotObtainLock = throwExceptionIfCannotObtainLock;
	}

}
