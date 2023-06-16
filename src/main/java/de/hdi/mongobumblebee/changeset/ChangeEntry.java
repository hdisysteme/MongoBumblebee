package de.hdi.mongobumblebee.changeset;

import java.util.Date;

import org.bson.Document;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

/**
 * Entry in the changes collection log {@link de.hdi.mongobumblebee.MongoBumblebee#DEFAULT_CHANGELOG_COLLECTION_NAME}
 * Type: entity class.
 *
 * @author lstolowski
 * @since 27/07/2014
 */
@AllArgsConstructor
@Getter
public class ChangeEntry {
	
	public static final String KEY_CHANGEID = "changeId";
	public static final String KEY_AUTHOR = "author";
	public static final String KEY_TIMESTAMP = "timestamp";
	public static final String KEY_CHANGELOGCLASS = "changeLogClass";
	public static final String KEY_CHANGESETMETHOD = "changeSetMethod";
	public static final String KEY_RESULT = "result";

	private String changeId;
	private String author;
	private Date timestamp;
	private String changeLogClass;
	private String changeSetMethodName;
	@Setter
	private Object result;

	public Document buildFullDBObject() {
		Document entry = new Document();

		entry
			.append(KEY_CHANGEID, this.changeId)
			.append(KEY_AUTHOR, this.author)
			.append(KEY_TIMESTAMP, this.timestamp)
			.append(KEY_CHANGELOGCLASS, this.changeLogClass)
			.append(KEY_CHANGESETMETHOD, this.changeSetMethodName);

		return entry;
	}

	public Document buildSearchQueryDBObject() {
		return new Document()
				.append(KEY_CHANGEID, this.changeId)
				.append(KEY_AUTHOR, this.author);
	}

	@Override
	public String toString() {
		return "[ChangeSet: id=" + this.changeId +
				", author=" + this.author +
				", changeLogClass=" + this.changeLogClass +
				", changeSetMethod=" + this.changeSetMethodName + "]";
	}

}
