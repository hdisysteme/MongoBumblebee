package de.hdi.mongobumblebee.dao;

import org.bson.Document;

import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;

import de.hdi.mongobumblebee.changeset.ChangeEntry;

/**
 * @author lstolowski
 * @since 10.12.14
 */
@Deprecated(forRemoval = true, since = "1.1.1")
public class ChangeEntryIndexDao {

	public void createRequiredUniqueIndex(MongoCollection<Document> changelogCollection) {
		changelogCollection.createIndex(new Document()
				.append(ChangeEntry.KEY_CHANGEID, 1)
				.append(ChangeEntry.KEY_AUTHOR, 1),
				new IndexOptions().unique(true)
				);
	}

	public Document findRequiredChangeAndAuthorIndex(MongoCollection<Document> changelogCollection) {
		final ListIndexesIterable<Document> indexes = changelogCollection.listIndexes();
		
		Document resultIndex = null;
        for (Document index : indexes) {
            final Document indexKey = index.get("key", Document.class);
            if (indexKey.containsKey(ChangeEntry.KEY_CHANGEID) && indexKey.getInteger(ChangeEntry.KEY_CHANGEID) == 1 &&
                    indexKey.containsKey(ChangeEntry.KEY_AUTHOR) && indexKey.getInteger(ChangeEntry.KEY_AUTHOR) == 1) {
                resultIndex = index;
                break;
            }
        }

        return resultIndex;
	}

	public boolean isUnique(Document index) {
		Object unique = index.get("unique");
		if (unique instanceof Boolean) {
			return (Boolean) unique;
		} else {
			return false;
		}
	}

	public void dropIndex(MongoCollection<Document> collection, Document index) {
		collection.dropIndex(index.get("name").toString());
	}

}
