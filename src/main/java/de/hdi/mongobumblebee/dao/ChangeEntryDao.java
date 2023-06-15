package de.hdi.mongobumblebee.dao;

import static org.springframework.util.StringUtils.hasText;

import java.util.Date;

import org.bson.Document;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import de.hdi.mongobumblebee.changeset.ChangeEntry;
import de.hdi.mongobumblebee.exception.MongobeeConfigurationException;
import de.hdi.mongobumblebee.exception.MongobeeConnectionException;
import de.hdi.mongobumblebee.exception.MongobeeLockException;
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
		this.indexDao = new ChangeEntryIndexDao(changelogCollectionName);
		this.lockDao = new LockDao(lockCollectionName);
		this.changelogCollectionName = changelogCollectionName;
		this.waitForLock = waitForLock;
		this.changeLogLockWaitTime = changeLogLockWaitTime;
		this.changeLogLockPollRate = changeLogLockPollRate;
		this.throwExceptionIfCannotObtainLock = throwExceptionIfCannotObtainLock;
	}

	public MongoDatabase connectMongoDb(MongoClient mongoClient, String dbName) throws MongobeeConfigurationException {
		if (!hasText(dbName)) {
			throw new MongobeeConfigurationException("DB name is not set. Should be defined in MongoDB URI or via setter");
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
	 * @throws MongobeeConnectionException exception
	 * @throws MongobeeLockException exception
	 */
	public boolean acquireProcessLock() throws MongobeeConnectionException, MongobeeLockException {
		verifyDbConnection();
		boolean acquired = lockDao.acquireLock(getMongoDatabase());

		if (!acquired && waitForLock) {
			long timeToGiveUp = new Date().getTime() + (changeLogLockWaitTime * 1000 * 60);
			while (!acquired && new Date().getTime() < timeToGiveUp) {
				acquired = lockDao.acquireLock(getMongoDatabase());
				if (!acquired) {
					log.info("Waiting for changelog lock....");
					try {
						Thread.sleep(changeLogLockPollRate * 1000);
					} catch (InterruptedException e) {
						// nothing
					}
				}
			}
		}

		if (!acquired && throwExceptionIfCannotObtainLock) {
			log.info("Mongobee did not acquire process lock. Throwing exception.");
			throw new MongobeeLockException("Could not acquire process lock");
		}

		return acquired;
	}

	public void releaseProcessLock() throws MongobeeConnectionException {
		verifyDbConnection();
		lockDao.releaseLock(getMongoDatabase());
	}

	public boolean isProccessLockHeld() throws MongobeeConnectionException {
		verifyDbConnection();
		return lockDao.isLockHeld(getMongoDatabase());
	}

	public boolean isNewChange(ChangeEntry changeEntry) throws MongobeeConnectionException {
		verifyDbConnection();

		MongoCollection<Document> mongobeeChangeLog = getMongoDatabase().getCollection(changelogCollectionName);
		Document entry = mongobeeChangeLog.find(changeEntry.buildSearchQueryDBObject()).first();

		return entry == null;
	}

	public void save(ChangeEntry changeEntry) throws MongobeeConnectionException {
		verifyDbConnection();

		MongoCollection<Document> mongobeeLog = getMongoDatabase().getCollection(changelogCollectionName);

		mongobeeLog.insertOne(changeEntry.buildFullDBObject());
	}

	private void verifyDbConnection() throws MongobeeConnectionException {
		if (getMongoDatabase() == null) {
			throw new MongobeeConnectionException("Database is not connected. Mongobee has thrown an unexpected error",
					new NullPointerException());
		}
	}

	private void ensureChangeLogCollectionIndex(MongoCollection<Document> collection) {
		Document index = indexDao.findRequiredChangeAndAuthorIndex(mongoDatabase);
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
		this.indexDao.setChangelogCollectionName(changelogCollectionName);
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
