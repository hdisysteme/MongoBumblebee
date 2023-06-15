package de.hdi.mongobumblebee;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.lang.NonNull;
import org.springframework.util.StringUtils;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;

import de.hdi.mongobumblebee.changeset.ChangeEntry;
import de.hdi.mongobumblebee.dao.ChangeEntryDao;
import de.hdi.mongobumblebee.exception.MongobeeChangeSetException;
import de.hdi.mongobumblebee.exception.MongobeeConfigurationException;
import de.hdi.mongobumblebee.exception.MongobeeConnectionException;
import de.hdi.mongobumblebee.exception.MongobeeException;
import de.hdi.mongobumblebee.utils.ChangeService;
import lombok.extern.slf4j.Slf4j;

/**
 * Mongobee runner
 *
 * @author lstolowski
 * @since 26/07/2014
 */
@Slf4j
public class MongoBumblebee implements InitializingBean {
	
	@Autowired(required = false)
	private ApplicationContext applicationContext;
	
	private static final String DEFAULT_CHANGELOG_COLLECTION_NAME = "changelog";
	private static final String DEFAULT_LOCK_COLLECTION_NAME = "mongobeelock";
	private static final boolean DEFAULT_WAIT_FOR_LOCK = false;
	private static final long DEFAULT_CHANGE_LOG_LOCK_WAIT_TIME = 5L;
	private static final long DEFAULT_CHANGE_LOG_LOCK_POLL_RATE = 10L;
	private static final boolean DEFAULT_THROW_EXCEPTION_IF_CANNOT_OBTAIN_LOCK = false;

	private ChangeEntryDao dao;
	private boolean enabled = true;
	private String changeLogsScanPackage;
	private final MongoClient mongoClient;
	private final String dbName;
	private Environment springEnvironment;

	/**
	 * <p>
	 * Constructor takes MongoClient object as a parameter.
	 * </p>
	 * <p>
	 * For more details about <tt>MongoClient</tt> please see com.mongodb.client.MongoClient docs
	 * </p>
	 *
	 * @param mongoClient
	 *            database connection client
	 * @throws MongobeeConfigurationException 
	 * @see MongoClient
	 */
	public MongoBumblebee(@NonNull MongoClient mongoClient, @NonNull String dbName) {
		this.dbName = dbName;
		this.mongoClient = mongoClient;
		this.dao = new ChangeEntryDao(DEFAULT_CHANGELOG_COLLECTION_NAME, DEFAULT_LOCK_COLLECTION_NAME, DEFAULT_WAIT_FOR_LOCK, DEFAULT_CHANGE_LOG_LOCK_WAIT_TIME, DEFAULT_CHANGE_LOG_LOCK_POLL_RATE, DEFAULT_THROW_EXCEPTION_IF_CANNOT_OBTAIN_LOCK);
	}

	/**
	 * <p>
	 * Mongobee runner. Correct MongoDB URI should be provided.
	 * </p>
	 * <p>
	 * The format of the URI is:
	 * 
	 * <pre>
	 *   mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[database[.collection]][?options]]
	 * </pre>
	 * <ul>
	 * <li>{@code mongodb://} Required prefix</li>
	 * <li>{@code username:password@} are optional. If given, the driver will attempt to login to a database after connecting to a database server. For some
	 * authentication mechanisms, only the username is specified and the password is not, in which case the ":" after the username is left off as well.</li>
	 * <li>{@code host1} Required. It identifies a server address to connect to. More than one host can be provided.</li>
	 * <li>{@code :portX} is optional and defaults to :27017 if not provided.</li>
	 * <li>{@code /database} the name of the database to login to and thus is only relevant if the {@code username:password@} syntax is used. If not specified
	 * the "admin" database will be used by default. <b>Mongobee will operate on the database provided here or on the database overriden by setter
	 * setDbName(String).</b></li>
	 * <li>{@code ?options} are connection options. For list of options please see com.mongodb.MongoClientURI docs</li>
	 * </ul>
	 * <p>
	 * For details, please see com.mongodb.MongoClientURI
	 *
	 * @param mongoURI
	 *            with correct format
	 * @throws MongobeeConfigurationException 
	 * @see com.mongodb.MongoClientURI
	 */

	public MongoBumblebee(@NonNull String mongoURI, @NonNull String dbName) {
		this(MongoClients.create(mongoURI), dbName); 
	}

	/**
	 * For Spring users: executing mongobee after bean is created in the Spring context
	 *
	 * @throws Exception
	 *             exception
	 */
	@Override
	public void afterPropertiesSet() throws Exception {
		execute();
	}

	/**
	 * Executing migration
	 *
	 * @throws MongobeeException
	 *             exception
	 */
	public void execute() throws MongobeeException {
		if (!isEnabled()) {
			log.info("Mongobee is disabled. Exiting.");
			return;
		}

		validateConfig();

		if (this.mongoClient != null) {
			dao.connectMongoDb(this.mongoClient, dbName);
		} 

		if (!dao.acquireProcessLock()) {
			log.info("Mongobee did not acquire process lock. Exiting.");
			return;
		}

		log.info("Mongobee acquired process lock, starting the data migration sequence..");

		try {
			executeMigration();
		} finally {
			log.info("Mongobee is releasing process lock.");
			dao.releaseProcessLock();
		}

		log.info("Mongobee has finished his job.");
	}

	private void executeMigration() throws MongobeeConnectionException, MongobeeException {

		ChangeService service = new ChangeService(changeLogsScanPackage, springEnvironment);

		for (Class<?> changelogClass : service.fetchChangeLogs()) {

			Object changelogInstance = null;
			try {
				try {
					changelogInstance = changelogClass.getDeclaredConstructor(ApplicationContext.class).newInstance(applicationContext); 
				} catch ( NoSuchMethodException e ) {
					changelogInstance = changelogClass.getConstructor().newInstance();
				}
				List<Method> changesetMethods = service.fetchChangeSets(changelogInstance.getClass());

				for (Method changesetMethod : changesetMethods) {
					ChangeEntry changeEntry = service.createChangeEntry(changesetMethod);

					try {
						if (dao.isNewChange(changeEntry)) {
							executeChangeSetMethod(changesetMethod, changelogInstance, dao.getMongoDatabase());
							dao.save(changeEntry);
							log.info(changeEntry + " applied");
						} else if (service.isRunAlwaysChangeSet(changesetMethod)) {
							executeChangeSetMethod(changesetMethod, changelogInstance, dao.getMongoDatabase());
							log.info(changeEntry + " reapplied");
						} else {
							log.info(changeEntry + " passed over");
						}
					} catch (MongobeeChangeSetException e) {
						log.error(e.getMessage());
					}
				}
			} catch (NoSuchMethodException | IllegalAccessException |InstantiationException e) {
				throw new MongobeeException(e.getMessage(), e);
			} catch (InvocationTargetException e) {
				Throwable targetException = e.getTargetException();
				throw new MongobeeException(targetException.getMessage(), e);
			}

		}
	}

	private Object executeChangeSetMethod(Method changeSetMethod, Object changeLogInstance, MongoDatabase mongoDatabase) throws IllegalAccessException, InvocationTargetException, MongobeeChangeSetException {
		if (changeSetMethod.getParameterTypes().length == 1 && changeSetMethod.getParameterTypes()[0].equals(MongoTemplate.class)) {
			log.debug("method with MongoTemplate argument");

			return changeSetMethod.invoke(changeLogInstance, new MongoTemplate(mongoClient, dbName));
		} else if (changeSetMethod.getParameterTypes().length == 2 && changeSetMethod.getParameterTypes()[0].equals(MongoTemplate.class) && changeSetMethod.getParameterTypes()[1].equals(Environment.class)) {
			log.debug("method with MongoTemplate and environment arguments");

			return changeSetMethod.invoke(changeLogInstance, new MongoTemplate(mongoClient, dbName), springEnvironment);
		} else if (changeSetMethod.getParameterTypes().length == 1 && changeSetMethod.getParameterTypes()[0].equals(MongoDatabase.class)) {
			log.debug("method with DB argument");

			return changeSetMethod.invoke(changeLogInstance, mongoDatabase);
		} else if (changeSetMethod.getParameterTypes().length == 0) {
			log.debug("method with no params");

			return changeSetMethod.invoke(changeLogInstance);
		} else {
			throw new MongobeeChangeSetException("ChangeSet method " + changeSetMethod.getName() + " has wrong arguments list. Please see docs for more info!");
		}
	}

	private void validateConfig() throws MongobeeConfigurationException {
		if (!StringUtils.hasText(dbName)) {
			throw new MongobeeConfigurationException("DB name is not set. It should be defined in MongoDB URI");
		}
		if (!StringUtils.hasText(changeLogsScanPackage)) {
			throw new MongobeeConfigurationException("Scan package for changelogs is not set: use appropriate setter");
		}
	}

	/**
	 * @return true if an execution is in progress, in any process.
	 * @throws MongobeeConnectionException
	 *             exception
	 */
	public boolean isExecutionInProgress() throws MongobeeConnectionException {
		return dao.isProccessLockHeld();
	}

	/**
	 * Package name where @ChangeLog-annotated classes are kept.
	 *
	 * @param changeLogsScanPackage
	 *            package where your changelogs are
	 * @return Mongobee object for fluent interface
	 */
	public MongoBumblebee setChangeLogsScanPackage(String changeLogsScanPackage) {
		this.changeLogsScanPackage = changeLogsScanPackage;
		return this;
	}

	/**
	 * @return true if Mongobee runner is enabled and able to run, otherwise false
	 */
	public boolean isEnabled() {
		return enabled;
	}

	/**
	 * Feature which enables/disables Mongobee runner execution
	 *
	 * @param enabled
	 *            MOngobee will run only if this option is set to true
	 * @return Mongobee object for fluent interface
	 */
	public MongoBumblebee setEnabled(boolean enabled) {
		this.enabled = enabled;
		return this;
	}

	/**
	 * Feature which enables/disables waiting for lock if it's already obtained
	 *
	 * @param waitForLock
	 *            Mongobee will be waiting for lock if it's already obtained if this option is set to true
	 * @return Mongobee object for fluent interface
	 */
	public MongoBumblebee setWaitForLock(boolean waitForLock) {
		this.dao.setWaitForLock(waitForLock);
		return this;
	}

	/**
	 * Waiting time for acquiring lock if waitForLock is true
	 *
	 * @param changeLogLockWaitTime
	 *            Waiting time in minutes for acquiring lock
	 * @return Mongobee object for fluent interface
	 */
	public MongoBumblebee setChangeLogLockWaitTime(long changeLogLockWaitTime) {
		this.dao.setChangeLogLockWaitTime(changeLogLockWaitTime);
		return this;
	}

	/**
	 * Poll rate for acquiring lock if waitForLock is true
	 *
	 * @param changeLogLockPollRate
	 *            Poll rate in seconds for acquiring lock
	 * @return Mongobee object for fluent interface
	 */
	public MongoBumblebee setChangeLogLockPollRate(long changeLogLockPollRate) {
		this.dao.setChangeLogLockPollRate(changeLogLockPollRate);
		return this;
	}

	/**
	 * Feature which enables/disables throwing MongobeeLockException if Mongobee can not obtain lock
	 *
	 * @param throwExceptionIfCannotObtainLock
	 *            Mongobee will throw MongobeeLockException if lock can not be obtained
	 * @return Mongobee object for fluent interface
	 */
	public MongoBumblebee setThrowExceptionIfCannotObtainLock(boolean throwExceptionIfCannotObtainLock) {
		this.dao.setThrowExceptionIfCannotObtainLock(throwExceptionIfCannotObtainLock);
		return this;
	}

	/**
	 * Set Environment object for Spring Profiles (@Profile) integration
	 *
	 * @param environment
	 *            org.springframework.core.env.Environment object to inject
	 * @return Mongobee object for fluent interface
	 */
	public MongoBumblebee setSpringEnvironment(Environment environment) {
		this.springEnvironment = environment;
		return this;
	}

	/**
	 * Overwrites a default mongobee changelog collection hardcoded in DEFAULT_CHANGELOG_COLLECTION_NAME.
	 *
	 * CAUTION! Use this method carefully - when changing the name on a existing system, your changelogs will be executed again on your MongoDB instance
	 *
	 * @param changelogCollectionName
	 *            a new changelog collection name
	 * @return Mongobee object for fluent interface
	 */
	public MongoBumblebee setChangelogCollectionName(String changelogCollectionName) {
		this.dao.setChangelogCollectionName(changelogCollectionName);
		return this;
	}

	/**
	 * Overwrites a default mongobee lock collection hardcoded in DEFAULT_LOCK_COLLECTION_NAME
	 *
	 * @param lockCollectionName
	 *            a new lock collection name
	 * @return Mongobee object for fluent interface
	 */
	public MongoBumblebee setLockCollectionName(String lockCollectionName) {
		this.dao.setLockCollectionName(lockCollectionName);
		return this;
	}

	/**
	 * Closes the Mongo instance used by Mongobee. This will close either the connection Mongobee was initiated with or that which was internally created.
	 */
	public void close() {
		dao.close();
	}
}
