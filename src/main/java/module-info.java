
module de.hdi.mongobumblebee {

	exports de.hdi.mongobumblebee.changeset;
	exports de.hdi.mongobumblebee.exception;
	exports de.hdi.mongobumblebee;
	
	opens de.hdi.mongobumblebee;
	
	requires lombok;
	requires org.reflections;
	requires transitive org.mongodb.driver.sync.client;
	requires org.mongodb.driver.core;
	requires org.slf4j;
	requires transitive org.mongodb.bson;
	requires spring.beans;
	requires spring.context;
	requires transitive spring.core;
	requires spring.data.mongodb;
	
}