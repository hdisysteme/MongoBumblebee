package de.hdi.mongobumblebee.utils;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

import de.flapdoodle.embed.mongo.commands.MongodArguments;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.mongo.transitions.Mongod;
import de.flapdoodle.embed.mongo.transitions.RunningMongodProcess;
import de.flapdoodle.reverse.StateID;
import de.flapdoodle.reverse.TransitionWalker;
import de.flapdoodle.reverse.transitions.Start;

public class EmbeddedMongoDBHelper {
	
	private static TransitionWalker.ReachedState<RunningMongodProcess> runningMongod;
	
	public static MongoClient startMongoClient() {
		return startMongoClient(Version.Main.V6_0);
	}
	
	public static MongoClient startMongoClient(Version.Main version) {
		if ( runningMongod == null ) {
			runningMongod = Mongod.instance().transitions(version)
				.replace(Start.to(MongodArguments.class).initializedWith(MongodArguments.defaults()))
				.walker()
				.initState(StateID.of(RunningMongodProcess.class));
		}
		
		return MongoClients.create(String.format("mongodb://%s:%d/", runningMongod.current().getServerAddress().getHost(), runningMongod.current().getServerAddress().getPort()));
	}
	
	public static void close() {
		runningMongod.close();
		runningMongod = null;
	}
}