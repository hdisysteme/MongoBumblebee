![MongoBumblebee](https://github.com/hdisysteme/MongoBumblebee/blob/main/misc/mongobumblebee_min.png)

[![Build status](https://github.com/hdisysteme/MongoBumblebee/actions/workflows/maven.yml/badge.svg)](https://github.com/hdisysteme/MongoBumblebee/actions/workflows/maven.yml) [![CodeQL](https://github.com/hdisysteme/MongoBumblebee/actions/workflows/codeql.yml/badge.svg)](https://github.com/hdisysteme/MongoBumblebee/actions/workflows/codeql.yml) [![Maven Central](https://maven-badges.herokuapp.com/maven-central/de.hdi/mongobumblebee/badge.svg)](https://maven-badges.herokuapp.com/maven-central/de.hdi/mongobumblebee) [![Licence](https://img.shields.io/hexpm/l/plug.svg)](https://github.com/hdisysteme/MongoBumblebee/blob/main/LICENSE)
---


**MongoBumblebee** is a Java tool which helps you to *manage changes* in your MongoDB and *synchronize* them with your application.
The concept is very similar to other db migration tools such as [Liquibase](http://www.liquibase.org) or [Flyway](http://flywaydb.org) but *without using XML/JSON/YML files*.

The goal is to keep this tool simple and comfortable to use.

**MongoBumblebee** provides new approach for adding changes (change sets) based on Java classes and methods with appropriate annotations.

**MongoBumblebee** is a fork of [mongobee](https://github.com/mongobee/mongobee). It works now with Spring Boot 3.x and uses an embedded MongoDB for unit tests.

## Getting started

### Add a dependency

With Maven
```xml
<dependency>
  <groupId>de.hdi</groupId>
  <artifactId>mongobumblebee</artifactId>
  <version>1.0</version>
</dependency>
```
With Gradle
```groovy
compile 'de.hdi:mongobumblebee:1.0'
```

### Usage with Spring

You need to instantiate MongoBumblebee object and provide some configuration.
If you use Spring can be instantiated as a singleton bean in the Spring context. 
In this case the migration process will be executed automatically on startup.

```java
@Bean
public MongoBumblebee mongobumblebee(){
  MongoBumblebee runner = new MongoBumblebee("mongodb://YOUR_DB_HOST:27017/", "DB_NAME");
  runner.setChangeLogsScanPackage(
       "com.example.yourapp.changelogs"); // the package to be scanned for change sets
  
  return runner;
}
```


### Usage without Spring
Using MongoBumblebee without a spring context has similar configuration but you have to remember to run `execute()` to start a migration process.

```java
MongoBumblebee runner = new MongoBumblebee("mongodb://YOUR_DB_HOST:27017/", "DB_NAME");
runner.setChangeLogsScanPackage(
     "com.example.yourapp.changelogs"); // package to scan for change sets

runner.execute();         //  ------> starts migration change sets
```

Above examples provide minimal configuration. `MongoBumblebee` object provides some other possibilities (setters) to make the tool more flexible:

```java
runner.setChangelogCollectionName(logColName);   // default is changelog, collection with applied change sets
runner.setLockCollectionName(lockColName);       // default is lock, collection used during migration process
runner.setEnabled(shouldBeEnabled);              // default is true, migration won't start if set to false
runner.setSpringEnvironment(enviroment);         // Mandantory if `MongoBumblebee` should work with profiles
```

MongoDB URI format:
```
mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[database[.collection]][?options]]
```
[More about URI](http://mongodb.github.io/mongo-java-driver/3.5/javadoc/)


### Creating change logs

`ChangeLog` contains bunch of `ChangeSet`s. `ChangeSet` is a single task (set of instructions made on a database). In other words `ChangeLog` is a class annotated with `@ChangeLog` and containing methods annotated with `@ChangeSet`.

```java 
package com.example.yourapp.changelogs;

@ChangeLog
public class DatabaseChangelog {
  
  @ChangeSet(order = "001", id = "someChangeId", author = "testAuthor")
  public void importantWorkToDo(MongoDatabase db){
     // task implementation
  }


}
```
#### @ChangeLog

Class with change sets must be annotated by `@ChangeLog`. There can be more than one change log class but in that case `order` argument should be provided:

```java
@ChangeLog(order = "001")
public class DatabaseChangelog {
  //...
}
```
Change logs are sorted alphabetically by `order` argument and change sets are applied due to this order.

By default the no args constructor is invoked. If a constructor is defined acception an `ApplicationContext` as argument this constructor will get invoked. This allows accessing all beans in the Spring context.

#### @ChangeSet

Method annotated by @ChangeSet is taken and applied to the database. History of applied change sets is stored in a collection called `mbchangelog` (by default) in your MongoDB

##### Annotation parameters:

`order` - string for sorting change sets in one change log. Sorting in alphabetical order, ascending. It can be a number, a date etc.

`id` - name of a change set, **must be unique** for all change logs in a database

`author` - author of a change set

`runAlways` - _[optional, default: false]_ change set will always be executed but only first execution event will be stored in mbchangelog collection

##### Defining change set methods
Method annotated by `@ChangeSet` can have one of the following definition:

```java
@ChangeSet(order = "001", id = "someChangeWithoutArgs", author = "testAuthor")
public void someChange1() {
   // method without arguments can do some non-db changes
}

@ChangeSet(order = "002", id = "someChangeWithMongoDatabase", author = "testAuthor")
public void someChange2(MongoDatabase db) {
  // type: com.mongodb.client.MongoDatabase : original MongoDB driver v. 3.x, operations allowed by driver are possible
  // example: 
  MongoCollection<Document> mycollection = db.getCollection("mycollection");
  Document doc = new Document("testName", "example").append("test", "1");
  mycollection.insertOne(doc);
}

@ChangeSet(order = "003", id = "someChangeWithSpringDataTemplate", author = "testAuthor")
public void someChange3(MongoTemplate mongoTemplate) {
  // type: org.springframework.data.mongodb.core.MongoTemplate
  // Spring Data integration allows using MongoTemplate in the ChangeSet
  // example:
  mongoTemplate.save(myEntity);
}

@ChangeSet(order = "004", id = "someChangeWithSpringDataTemplate", author = "testAuthor")
public void someChange4(MongoTemplate mongoTemplate, Environment environment) {
  // type: org.springframework.data.mongodb.core.MongoTemplate
  // type: org.springframework.core.env.Environment
  // Spring Data integration allows using MongoTemplate and Environment in the ChangeSet
}
```

##### Return values

The return value of the change set method is written to the database, too. As an example the method could return the number of created or changed objects.

### Using Spring profiles
     
**MongoBumblebee** accepts Spring's `org.springframework.context.annotation.Profile` annotation. If a change log or change set class is annotated  with `@Profile` then it is activated for current application profiles.

_Example 1_: annotated change set will be invoked for a `dev` profile
```java
@Profile("dev")
@ChangeSet(author = "testuser", id = "myDevChangest", order = "01")
public void devEnvOnly(MongoDatabase db) {
  // ...
}
```
_Example 2_: all change sets in a change log will be invoked for a `test` profile
```java
@ChangeLog(order = "1")
@Profile("test")
public class ChangelogForTestEnv{
  @ChangeSet(author = "testuser", id = "myTestChangest", order = "01")
  public void testingEnvOnly(MongoDatabase db) {
    // ...
  } 
}
```

#### Enabling @Profile annotation (option)
      
To enable the `@Profile` integration, please inject `org.springframework.core.env.Environment` to you runner.

```java      
@Bean 
public MongoBumblebee mongobumblebee(@Autowired Environment environment) {
  MongoBumblebee runner = new MongoBumblebee(uri, dbName);
  runner.setSpringEnvironment(environment);
  //... etc
}
```

## Known issues

