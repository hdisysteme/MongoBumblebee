package de.hdi.mongobumblebee.dao;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;

import com.mongodb.ErrorCategory;
import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;

import de.hdi.mongobumblebee.MongoBumblebee;
import lombok.extern.slf4j.Slf4j;

/**
 * @author colsson11
 * @since 13.01.15
 */
@Slf4j
public class LockDao {
	
	private static final String KEY_PROP_NAME = "key";

	private static final String LOCK_ENTRY_KEY_VAL = "LOCK";
	
	private String lockCollectionName;

	public LockDao(String lockCollectionName) {
		this.lockCollectionName = lockCollectionName;
	}

	public void intitializeLock(MongoTemplate template) {
		template.indexOps(lockCollectionName).ensureIndex(new Index().named(MongoBumblebee.MB_PREFIX + "lock_key_idx").unique().on(KEY_PROP_NAME, Sort.Direction.ASC));
	}

	public boolean acquireLock(MongoDatabase db) {

		Document insertObj = new Document(KEY_PROP_NAME, LOCK_ENTRY_KEY_VAL)
				.append("status", "LOCK_HELD")
				.append("lastAccess", LocalDateTime.now());

		// acquire lock by attempting to insert the same value in the collection - if it already exists (i.e. lock held)
		// there will be an exception
		try {
			db.getCollection(lockCollectionName).insertOne(insertObj);
		} catch (MongoWriteException ex) {
			if (ex.getError().getCategory() == ErrorCategory.DUPLICATE_KEY) {
				log.warn("Duplicate key exception while acquireLock. Probably the lock has been already acquired by another process.");
			}
			return false;
		}
		return true;
	}
	
	public void updateLock(MongoDatabase db) {
		Bson filter = Filters.eq("status", "LOCK_HELD");
		Bson update = Updates.set("lastAccess", LocalDateTime.now());
		
		var result = db.getCollection(lockCollectionName).updateOne(filter, update);
		if ( result.getModifiedCount() != 1 )
			log.warn("Lock couldn't be updated");
	}

	public void releaseLock(MongoDatabase db) {
		// release lock by deleting collection entry
		db.getCollection(lockCollectionName).deleteMany(new Document(KEY_PROP_NAME, LOCK_ENTRY_KEY_VAL));
	}

	/**
	 * Check if the lock is held. Could be used by external process for example.
	 *
	 * @param db MongoDatabase object
	 * @return true if the lock is currently held
	 */
	public boolean isLockHeld(MongoDatabase db) {
		return db.getCollection(lockCollectionName).countDocuments() == 1;
	}
	
	/**
	 * Return the date and time the lock has been the last time updated or the creation time
	 * 
	 * @param db MongoDatabase object
	 * @return last access time or null if no lock exisits
	 */
	public LocalDateTime getLastAccess(MongoDatabase db) {
		Document doc = db.getCollection(lockCollectionName).find().first();
		if ( doc != null ) {
			Date dateToConvert = doc.get("lastAccess", Date.class);
			return dateToConvert
					.toInstant()
					.atZone(ZoneId.systemDefault())
				    .toLocalDateTime();
		}
		return null;
	}


	public void setLockCollectionName(String lockCollectionName) {
		this.lockCollectionName = lockCollectionName;
	}

}
