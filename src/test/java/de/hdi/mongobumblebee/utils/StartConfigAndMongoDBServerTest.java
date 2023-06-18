package de.hdi.mongobumblebee.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.Set;

import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import com.mongodb.client.MongoClient;

class StartConfigAndMongoDBServerTest {
	
	@Test
	void testMongoClient1() {
		MongoClient mongoClient = EmbeddedMongoDBHelper.startMongoClient();
		mongoClient.getDatabase("test").createCollection("test");
		
		assertEquals(0, mongoClient.getDatabase("test").getCollection("test").countDocuments());
		mongoClient.getDatabase("test").getCollection("test").insertOne(new Document("key", "value"));
		assertEquals(1, mongoClient.getDatabase("test").getCollection("test").countDocuments());
		
		Set<String> databases = new HashSet<>();
		mongoClient.listDatabaseNames().into(databases);
		assertTrue(databases.containsAll(Set.of("admin","config","local")));
	}
	
	@Test
	void testMongoClient2() {
		MongoClient mongoClient = EmbeddedMongoDBHelper.startMongoClient();
		
		assertEquals(1, mongoClient.getDatabase("test").getCollection("test").countDocuments());
		
		Set<String> databases = new HashSet<>();
		mongoClient.listDatabaseNames().into(databases);
		assertTrue(databases.containsAll(Set.of("admin","config","local")));
	}
	
	@AfterAll
	public static void close() {
		EmbeddedMongoDBHelper.close();
	}
	
}