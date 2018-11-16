/**
 * Copyright (c) 2012 - 2015 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

/*
 * MongoDB client binding for YCSB.
 *
 * Submitted by Yen Pai on 5/11/2010.
 *
 * https://gist.github.com/000a66b8db2caf42467b#file_mongo_database.java
 */
package com.yahoo.ycsb.db;

import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.client.AggregateIterable;
import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.mongodb.util.JSON;


import com.yahoo.ycsb.generator.soe.Generator;
import org.bson.Document;
import org.bson.types.Binary;


import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.text.SimpleDateFormat;

/**
 * MongoDB binding for YCSB framework using the MongoDB Inc. <a
 * href="http://docs.mongodb.org/ecosystem/drivers/java/">driver</a>
 * <p>
 * See the <code>README.md</code> for configuration information.
 * </p>
 * 
 * @author ypai
 * @see <a href="http://docs.mongodb.org/ecosystem/drivers/java/">MongoDB Inc.
 *      driver</a>
 */
public class MongoDbClient extends DB {

  /** Used to include a field in a response. */
  private static final Integer INCLUDE = Integer.valueOf(1);

  /** The options to use for inserting many documents. */
  private static final InsertManyOptions INSERT_UNORDERED =
      new InsertManyOptions().ordered(false);

  /** The options to use for inserting a single document. */
  private static final UpdateOptions UPDATE_WITH_UPSERT = new UpdateOptions()
      .upsert(true);

  /**
   * The database name to access.
   */
  private static String databaseName;

  /** The database name to access. */
  private static MongoDatabase database;

  /**
   * Count the number of times initialized to teardown on the last
   * {@link #cleanup()}.
   */
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

  /** A singleton Mongo instance. */
  private static MongoClient mongoClient;

  /** The default read preference for the test. */
  private static ReadPreference readPreference;

  /** The default write concern for the test. */
  private static WriteConcern writeConcern;

  /** The batch size to use for inserts. */
  private static int batchSize;

  /** If true then use updates with the upsert option for inserts. */
  private static boolean useUpsert;

  /** The bulk inserts pending for the thread. */
  private final List<Document> bulkInserts = new ArrayList<Document>();

  /**  */
  private static boolean flat;
  private static boolean nested;
  private static int nestingDepth;
  private static String NESTED_KEY = "yscb_key";
  private static String COLUMN_PREFIX = "field";

  private static boolean select_all_fields;
  private static boolean select_one_field;
  private static String[] select_field_path;

  private static boolean update_one_field;
  private static boolean update_all_fields;
  private static String update_field;

  private static String inc_field;

  private static int document_depth;
  private static int document_width;
  private static int element_values;
  private static int element_obj;

  /**
   * Cleanup any state for this DB. Called once per DB instance; there is one DB
   * instance per client thread.
   */
  @Override
  public void cleanup() throws DBException {
    if (INIT_COUNT.decrementAndGet() == 0) {
      try {
        mongoClient.close();
      } catch (Exception e1) {
        System.err.println("Could not close MongoDB connection pool: "
            + e1.toString());
        e1.printStackTrace();
        return;
      } finally {
        database = null;
        mongoClient = null;
      }
    }
  }




  /*
       ================    SOE operations  ======================
   */

  @Override
  public Status soeLoad(String table, Generator generator) {

    try {
      String key = generator.getCustomerIdRandom();
      MongoCollection<Document> collection = database.getCollection(table);
      Document query = new Document("_id", key);
      FindIterable<Document> findIterable = collection.find(query);
      Document queryResult = findIterable.first();
      if (queryResult == null) {
        System.out.println("Empty return");
        return Status.OK;
      }

      SimpleDateFormat sdfr = new SimpleDateFormat("yyyy-MM-dd");
      String dobStr = sdfr.format(queryResult.get(Generator.SOE_FIELD_CUSTOMER_DOB));
      queryResult.put(Generator.SOE_FIELD_CUSTOMER_DOB, dobStr);

      generator.putCustomerDocument(key, queryResult.toJson());
      List<String> orders = (List<String>) queryResult.get(Generator.SOE_FIELD_CUSTOMER_ORDER_LIST);
      for (String order:orders) {
        query = new Document("_id", order);
        findIterable = collection.find(query);
        queryResult = findIterable.first();
        if (queryResult == null) {
          return Status.ERROR;
        }
        generator.putOrderDocument(order, queryResult.toJson());
      }
      return Status.OK;
    } catch (Exception e) {
      System.err.println(e.toString());
    }
    return Status.ERROR;
  }


  // *********************  SOE Insert ********************************

  @Override
  public Status soeInsert(String table, HashMap<String, ByteIterator> result, Generator gen)  {

    try {
      MongoCollection<Document> collection = database.getCollection(table);
      String key = gen.getPredicate().getDocid();
      String value = gen.getPredicate().getValueA();
      Document toInsert = new Document("_id", key);
      DBObject body = (DBObject) JSON.parse(value);
      toInsert.put(key, body);
      collection.insertOne(toInsert);

      return Status.OK;
    } catch (Exception e) {
      System.err.println("Exception while trying bulk insert with "
          + bulkInserts.size());
      e.printStackTrace();
      return Status.ERROR;
    }

  }



  // *********************  SOE Update ********************************

  @Override
  public Status soeUpdate(String table, HashMap<String, ByteIterator> result, Generator gen) {
    try {
      MongoCollection<Document> collection = database.getCollection(table);
      String key = gen.getCustomerIdWithDistribution();
      String updateFieldName = gen.getPredicate().getNestedPredicateA().getName();
      String updateFieldValue = gen.getPredicate().getNestedPredicateA().getValueA();

      Document query = new Document("_id", key);
      Document fieldsToSet = new Document();

      fieldsToSet.put(updateFieldName, updateFieldValue);
      Document update = new Document("$set", fieldsToSet);

      UpdateResult res = collection.updateOne(query, update);
      if (res.wasAcknowledged() && res.getMatchedCount() == 0) {
        System.err.println("Nothing updated for key " + key);
        return Status.NOT_FOUND;
      }
      return Status.OK;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    }
  }

// *********************  SOE Read ********************************

  @Override
  public Status soeRead(String table, HashMap<String, ByteIterator> result, Generator gen) {
    try {
      MongoCollection<Document> collection = database.getCollection(table);
      String key = gen.getCustomerIdWithDistribution();
      Document query = new Document("_id", key);
      FindIterable<Document> findIterable = collection.find(query);

      Document projection = new Document();
      for (String field : gen.getAllFields()) {
        projection.put(field, INCLUDE);
      }
      findIterable.projection(projection);

      Document queryResult = findIterable.first();

      if (queryResult != null) {
        soeFillMap(result, queryResult);
      }
      return queryResult != null ? Status.OK : Status.NOT_FOUND;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    }
  }


  // *********************  SOE Scan ********************************
  @Override
  public Status soeScan(String table, final Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    String startkey = gen.getCustomerIdWithDistribution();
    int recordcount = gen.getRandomLimit();
    MongoCursor<Document> cursor = null;
    try {
      MongoCollection<Document> collection = database.getCollection(table);

      Document scanRange = new Document("$gte", startkey);
      Document query = new Document("_id", scanRange);
      Document sort = new Document("_id", INCLUDE);

      FindIterable<Document> findIterable =
          collection.find(query).sort(sort).limit(recordcount);

      Document projection = new Document();
      for (String field : gen.getAllFields()) {
        projection.put(field, INCLUDE);
      }
      findIterable.projection(projection);

      cursor = findIterable.iterator();

      if (!cursor.hasNext()) {
        System.err.println("Nothing found in scan for key " + startkey);
        return Status.ERROR;
      }

      result.ensureCapacity(recordcount);

      while (cursor.hasNext()) {
        HashMap<String, ByteIterator> resultMap =
            new HashMap<String, ByteIterator>();

        Document obj = cursor.next();
        soeFillMap(resultMap, obj);
        result.add(resultMap);
      }
      return Status.OK;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    } finally {
      if (cursor != null) {
        cursor.close();
      }
    }
  }

  // *********************  SOE Page ********************************

  @Override
  public Status soePage(String table, final Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    int recordcount = gen.getRandomLimit();
    int offset = gen.getRandomOffset();
    String addrzipName = gen.getPredicate().getName() + "." + gen.getPredicate().getNestedPredicateA().getName();
    String addzipValue = gen.getPredicate().getNestedPredicateA().getValueA();

    MongoCursor<Document> cursor = null;
    try {
      MongoCollection<Document> collection = database.getCollection(table);

      Document query = new Document(addrzipName, addzipValue);

      FindIterable<Document> findIterable =
          collection.find(query).limit(recordcount).skip(offset);

      Document projection = new Document();
      for (String field : gen.getAllFields()) {
        projection.put(field, INCLUDE);
      }
      findIterable.projection(projection);

      cursor = findIterable.iterator();

      if (!cursor.hasNext()) {
        //System.err.println("Nothing found for " + addrzipName + " = " + addzipValue);
        return Status.NOT_FOUND;
      }

      result.ensureCapacity(recordcount);

      while (cursor.hasNext()) {
        HashMap<String, ByteIterator> resultMap =
            new HashMap<String, ByteIterator>();

        Document obj = cursor.next();
        soeFillMap(resultMap, obj);
        result.add(resultMap);

      }
      return Status.OK;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    } finally {
      if (cursor != null) {
        cursor.close();
      }
    }
  }


  // *********************  SOE search ********************************

  @Override
  public Status soeSearch(String table, final Vector<HashMap<String, ByteIterator>> result, Generator gen) {

    MongoCursor<Document> cursor = null;
    try {
      int recordcount = gen.getRandomLimit();
      int offset = gen.getRandomOffset();

      String addrcountryName = gen.getPredicatesSequence().get(0).getName() + "." +
          gen.getPredicatesSequence().get(0).getNestedPredicateA().getName();
      String agegroupName = gen.getPredicatesSequence().get(1).getName();
      String dobyearName = gen.getPredicatesSequence().get(2).getName();

      String addrcountryValue = gen.getPredicatesSequence().get(0).getNestedPredicateA().getValueA();
      String agegroupValue = gen.getPredicatesSequence().get(1).getValueA();
      String dobyearValue = gen.getPredicatesSequence().get(2).getValueA();
      MongoCollection<Document> collection = database.getCollection(table);

      DBObject clause1 = new BasicDBObject(addrcountryName, addrcountryValue);
      DBObject clause2 = new BasicDBObject(agegroupName, agegroupValue);
      DBObject clause3Range =
          new BasicDBObject("$gte", new SimpleDateFormat("yyyy-MM-dd").parse(dobyearValue + "-1-1"));
      DBObject clause3 = new BasicDBObject(dobyearName, clause3Range);
      DBObject clause4Range =
          new BasicDBObject("$lte", new SimpleDateFormat("yyyy-MM-dd").parse(dobyearValue+ "-12-31"));
      DBObject clause4 = new BasicDBObject(dobyearName, clause4Range);

      BasicDBList and = new BasicDBList();
      and.add(clause1);
      and.add(clause2);
      and.add(clause3);
      and.add(clause4);

      Document query = new Document("$and", and);

      FindIterable<Document> findIterable =
          collection.find(query).sort(new BasicDBObject(addrcountryName, 1).
              append(agegroupName, 1).append(dobyearName, 1)).limit(recordcount).skip(offset);

      Document projection = new Document();
      for (String field : gen.getAllFields()) {
        projection.put(field, INCLUDE);
      }
      findIterable.projection(projection);

      cursor = findIterable.iterator();

      if (!cursor.hasNext()) {
        return Status.NOT_FOUND;
      }
      result.ensureCapacity(recordcount);

      while (cursor.hasNext()) {
        HashMap<String, ByteIterator> resultMap =
            new HashMap<String, ByteIterator>();

        Document obj = cursor.next();
        soeFillMap(resultMap, obj);
        result.add(resultMap);
      }
      return Status.OK;

    } catch (Exception e) {
      System.out.println(e.getMessage().toString());
      return Status.ERROR;
    } finally {
      if (cursor != null) {
        cursor.close();
      }
    }
  }


  // *********************  SOE NestScan ********************************

  @Override
  public Status soeNestScan(String table, final Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    int recordcount = gen.getRandomLimit();
    String nestedZipName =  gen.getPredicate().getName() + "." + gen.getPredicate().getNestedPredicateA().getName() +
        "." + gen.getPredicate().getNestedPredicateA().getNestedPredicateA().getName();
    String nestedZipValue = gen.getPredicate().getNestedPredicateA().getNestedPredicateA().getValueA();

    MongoCursor<Document> cursor = null;
    try {
      MongoCollection<Document> collection = database.getCollection(table);

      Document query = new Document(nestedZipName, nestedZipValue);

      FindIterable<Document> findIterable =
          collection.find(query).limit(recordcount);

      Document projection = new Document();
      for (String field : gen.getAllFields()) {
        projection.put(field, INCLUDE);
      }
      findIterable.projection(projection);

      cursor = findIterable.iterator();

      if (!cursor.hasNext()) {
        return Status.NOT_FOUND;
      }

      result.ensureCapacity(recordcount);

      while (cursor.hasNext()) {
        HashMap<String, ByteIterator> resultMap =
            new HashMap<String, ByteIterator>();

        Document obj = cursor.next();
        soeFillMap(resultMap, obj);
        result.add(resultMap);
      }
      return Status.OK;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    } finally {
      if (cursor != null) {
        cursor.close();
      }
    }
  }

  // *********************  SOE ArrayScan ********************************

  @Override
  public Status soeArrayScan(String table, final Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    int recordcount = gen.getRandomLimit();
    String arrName =  gen.getPredicate().getName();
    String arrValue = gen.getPredicate().getValueA();
    Document sort = new Document("_id", INCLUDE);

    MongoCursor<Document> cursor = null;
    try {
      MongoCollection<Document> collection = database.getCollection(table);

      BasicDBObject   query = new BasicDBObject();
      query.put(arrName, arrValue);

      FindIterable<Document> findIterable = collection.find(query).sort(sort).limit(recordcount);
      Document projection = new Document();
      for (String field : gen.getAllFields()) {
        projection.put(field, INCLUDE);
      }
      findIterable.projection(projection);
      cursor = findIterable.iterator();
      if (!cursor.hasNext()) {
        return Status.NOT_FOUND;
      }
      result.ensureCapacity(recordcount);

      while (cursor.hasNext()) {
        HashMap<String, ByteIterator> resultMap =
            new HashMap<String, ByteIterator>();

        Document obj = cursor.next();
        soeFillMap(resultMap, obj);
        result.add(resultMap);
      }
      return Status.OK;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    } finally {
      if (cursor != null) {
        cursor.close();
      }
    }
  }

  // *********************  SOE ArrayDeepScan ********************************

  @Override
  public Status soeArrayDeepScan(String table, final Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    int recordcount = gen.getRandomLimit();
    String fieldName =  gen.getPredicate().getName();
    String fieldCountryName = gen.getPredicate().getNestedPredicateA().getName();
    String fieldCitiesName = gen.getPredicate().getNestedPredicateB().getName();
    String fieldCountryValue = gen.getPredicate().getNestedPredicateA().getValueA();
    String fieldCitiesValue = gen.getPredicate().getNestedPredicateB().getValueA();

    Document sort = new Document("_id", INCLUDE);

    MongoCursor<Document> cursor = null;
    try {
      MongoCollection<Document> collection = database.getCollection(table);

      BasicDBObject   query = new BasicDBObject();
      query.put(fieldName + "." + fieldCountryName, fieldCountryValue);
      query.put(fieldName + "." + fieldCitiesName, fieldCitiesValue);

      FindIterable<Document> findIterable = collection.find(query).sort(sort);
      Document projection = new Document();
      for (String field : gen.getAllFields()) {
        projection.put(field, INCLUDE);
      }
      findIterable.projection(projection);
      cursor = findIterable.iterator();
      if (!cursor.hasNext()) {
        return Status.NOT_FOUND;
      }
      result.ensureCapacity(recordcount);

      while (cursor.hasNext()) {
        HashMap<String, ByteIterator> resultMap =
            new HashMap<String, ByteIterator>();

        Document obj = cursor.next();
        soeFillMap(resultMap, obj);
        result.add(resultMap);
      }
      return Status.OK;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    } finally {
      if (cursor != null) {
        cursor.close();
      }
    }
  }



  // *********************  SOE Report ********************************

  @Override
  public Status soeReport(String table, final Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    int recordcount = gen.getRandomLimit();
    String orderListName = gen.getPredicatesSequence().get(0).getName();
    String addrName = gen.getPredicatesSequence().get(1).getName();
    String addrZipName = addrName + "." + gen.getPredicatesSequence().get(1).getNestedPredicateA().getName();
    String addrZipValue = gen.getPredicatesSequence().get(1).getNestedPredicateA().getValueA();

    MongoCursor<Document> cursor = null;
    try {

      MongoCollection<Document> collection = database.getCollection(table);
      Document query = new Document(addrZipName, addrZipValue);
      FindIterable<Document> findIterable = collection.find(query);
      Document projection = new Document();
      for (String field : gen.getAllFields()) {
        projection.put(field, INCLUDE);
      }
      findIterable.projection(projection);
      cursor = findIterable.iterator();
      if (!cursor.hasNext()) {
        return Status.NOT_FOUND;
      }
      result.ensureCapacity(recordcount);

      while (cursor.hasNext()) {
        HashMap<String, ByteIterator> resultMap = new HashMap<String, ByteIterator>();
        Document obj = cursor.next();
        if (obj.get(orderListName) != null) {
          BasicDBObject subq  = new BasicDBObject();
          subq.put("_id", new BasicDBObject("$in", obj.get(orderListName)));
          FindIterable<Document> findSubIterable = collection.find(subq);
          Document orderDoc = findSubIterable.first();
          obj.put(orderListName, orderDoc);
        }
        soeFillMap(resultMap, obj);
        result.add(resultMap);
      }
      return Status.OK;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    } finally {
      if (cursor != null) {
        cursor.close();
      }
    }
  }


  // *********************  SOE Report2 ********************************

  @Override
  public Status soeReport2(String table, final Vector<HashMap<String, ByteIterator>> result, Generator gen) {

    String nameOrderMonth = gen.getPredicatesSequence().get(0).getName();
    String nameOrderSaleprice = gen.getPredicatesSequence().get(1).getName();
    String nameAddress =  gen.getPredicatesSequence().get(2).getName();
    String nameAddressZip =  nameAddress + "." + gen.getPredicatesSequence().get(2).getNestedPredicateA().getName();
    String nameOrderlist = gen.getPredicatesSequence().get(3).getName();
    String valueOrderMonth = gen.getPredicatesSequence().get(0).getValueA();
    String valueAddressZip =  gen.getPredicatesSequence().get(2).getNestedPredicateA().getValueA();

    MongoCursor<Document> cursor = null;
    try {

      MongoCollection<Document> collection = database.getCollection(table);
      Document query = new Document(nameAddressZip, valueAddressZip);
      FindIterable<Document> findIterable = collection.find(query);
      Document projection = new Document();
      projection.put(nameOrderlist, INCLUDE);
      projection.put(nameAddressZip, INCLUDE);

      findIterable.projection(projection);
      cursor = findIterable.iterator();
      if (!cursor.hasNext()) {
        return Status.NOT_FOUND;
      }
      float totalsum = 0;
      HashMap<String, ByteIterator> resultMap = new HashMap<String, ByteIterator>();
      while (cursor.hasNext()) {
        Document obj = cursor.next();
        if (obj.get(nameOrderlist) != null) {
          BasicDBObject subq  = new BasicDBObject();
          subq.put("_id", new BasicDBObject("$in", obj.get(nameOrderlist)));
          subq.put(nameOrderMonth, valueOrderMonth);

          Document g1 = new Document("_id", new BasicDBObject("$in", obj.get(nameOrderlist)));
          Document g2 = new Document(nameOrderMonth, valueOrderMonth);

          AggregateIterable<Document> output = collection.aggregate(Arrays.asList(
              new Document("$match", new Document("$and", Arrays.asList(g1, g2))),
              new Document("$group", new Document("_id", null).
                  append("SUM", new BasicDBObject("$sum", "$sale_price")))
          ));

          for (Document dbObject : output) {
            if (dbObject.get("SUM") != null) {
              totalsum += Float.parseFloat(dbObject.get("SUM").toString());
            }
          }
        }
      }
      Document res = new Document();
      res.put("Total", String.valueOf(totalsum));
      res.put(nameOrderMonth, valueOrderMonth);
      res.put(nameAddressZip, valueAddressZip);
      soeFillMap(resultMap, res);
      result.add(resultMap);

      return Status.OK;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    } finally {
      if (cursor != null) {
        cursor.close();
      }
    }
  }

  //DBObject query = QueryBuilder.start("_id").in(new String[] {"foo", "bar"}).get();
  //collection.find(query);

  /**
   * Delete a record from the database.
   * 
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error. See the {@link DB}
   *         class's description for a discussion of error codes.
   */
  @Override
  public Status delete(String table, String key) {
    try {
      MongoCollection<Document> collection = database.getCollection(table);

      Document query = new Document("_id", key);
      DeleteResult result =
          collection.withWriteConcern(writeConcern).deleteOne(query);
      if (result.wasAcknowledged() && result.getDeletedCount() == 0) {
        System.err.println("Nothing deleted for key " + key);
        return Status.NOT_FOUND;
      }
      return Status.OK;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    }
  }

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one
   * DB instance per client thread.
   */
  @Override
  public void init() throws DBException {
    INIT_COUNT.incrementAndGet();
    synchronized (INCLUDE) {
      if (mongoClient != null) {
        return;
      }

      Properties props = getProperties();

      flat = Boolean.parseBoolean(props.getProperty("flat", "true"));
      nested = Boolean.parseBoolean(props.getProperty("nested", "false"));
      nestingDepth = Integer.parseInt(props.getProperty("depth", "10"));

	  select_all_fields = Boolean.parseBoolean(props.getProperty("select_all_fields", "true"));
	  select_one_field = Boolean.parseBoolean(props.getProperty("select_one_field", "false"));
	  select_field_path = props.getProperty("select_field_path", "").split(",");

	  update_all_fields = Boolean.parseBoolean(props.getProperty("update_all_fields", "true"));
	  update_one_field = Boolean.parseBoolean(props.getProperty("update_one_field", "false"));
	  update_field = props.getProperty("update_field", "");

          inc_field = props.getProperty("inc_field", null);

      document_depth = Integer.parseInt(props.getProperty("document_depth", "3"));
      document_width = Integer.parseInt(props.getProperty("document_width", "4"));
      element_values = Integer.parseInt(props.getProperty("element_values", "2"));
      element_obj = Integer.parseInt(props.getProperty("element_obj", "2"));

      // Set insert batchsize, default 1 - to be YCSB-original equivalent
      batchSize = Integer.parseInt(props.getProperty("batchsize", "1"));

      // Set is inserts are done as upserts. Defaults to false.
      useUpsert = Boolean.parseBoolean(
          props.getProperty("mongodb.upsert", "false"));

      // Just use the standard connection format URL
      // http://docs.mongodb.org/manual/reference/connection-string/
      // to configure the client.
      String url = props.getProperty("mongodb.url", null);
      boolean defaultedUrl = false;
      if (url == null) {
        defaultedUrl = true;
        url = "mongodb://localhost:27017/ycsb?w=1";
      }

      url = OptionsSupport.updateUrl(url, props);

      if (!url.startsWith("mongodb://")) {
        System.err.println("ERROR: Invalid URL: '" + url
            + "'. Must be of the form "
            + "'mongodb://<host1>:<port1>,<host2>:<port2>/database?options'. "
            + "http://docs.mongodb.org/manual/reference/connection-string/");
        System.exit(1);
      }

      try {
        MongoClientURI uri = new MongoClientURI(url);

        String uriDb = uri.getDatabase();
        if (!defaultedUrl && (uriDb != null) && !uriDb.isEmpty()
            && !"admin".equals(uriDb)) {
          databaseName = uriDb;
        } else {
          // If no database is specified in URI, use "ycsb"
          databaseName = "ycsb";

        }

        readPreference = uri.getOptions().getReadPreference();
        writeConcern = uri.getOptions().getWriteConcern();

        mongoClient = new MongoClient(uri);
        database =
            mongoClient.getDatabase(databaseName)
                .withReadPreference(readPreference)
                .withWriteConcern(writeConcern);

        System.out.println("mongo client connection created with " + url);
      } catch (Exception e1) {
        System.err
            .println("Could not initialize MongoDB connection pool for Loader: "
                + e1.toString());
        e1.printStackTrace();
        return;
      }
    }
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key.
   * 
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to insert.
   * @param values
   *          A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error. See the {@link DB}
   *         class's description for a discussion of error codes.
   */
  @Override
  public Status insert(String table, String key,
      HashMap<String, ByteIterator> values) {
    try {
      MongoCollection<Document> collection = database.getCollection(table);
      Document toInsert;

      if (nested) {
        toInsert = new Document("ycsb_key", key);
        for (int i = 1; i < nestingDepth; i++) {
          toInsert = new Document("ycsb_key" + i, toInsert);
        }
      }
      else {
        toInsert = new Document("_id", key);
      }

      int depth = 0;
      int index = 2;

      if (document_depth == 0) {
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
          toInsert.put(entry.getKey(), entry.getValue().toArray());
        }
      }
      else {
        ArrayList<Document> obj_keys = new ArrayList<Document>();
        ArrayList<Document> top_keys = obj_keys;
        ArrayList<Document> current_keys;
        LinkedList<ByteIterator> val_list = new LinkedList<ByteIterator>(values.values());
        for(int i = 0; i < document_width; i++) {
          obj_keys.add(new Document());
        }

        while (depth < document_depth) {
          current_keys = new ArrayList<Document>();
          for(Document obj : obj_keys) {

            // put values
            for(int i = 0; i < element_values; i++) {
              obj.put(String.format("%s%d", COLUMN_PREFIX, index), val_list.pop().toArray());
              index++;
            }

            if (depth < document_depth - 1) {
              // put objects
              for(int i = 0; i < element_obj; i++) {
                Document child = new Document();
                obj.put(String.format("%s%d", COLUMN_PREFIX, index), child);
                current_keys.add(child);
                index++;
              }
            }
            else {
              // put values
              for(int i = 0; i < element_obj; i++) {
                obj.put(String.format("%s%d", COLUMN_PREFIX, index), val_list.pop().toArray());
                index++;
              }
            }
          }

          obj_keys = current_keys;
          depth++;
        }

        for(Document obj : top_keys) {
          toInsert.put(String.format("%s%d", COLUMN_PREFIX, index++), obj);
        }
      }

      if (batchSize == 1) {
        if (useUpsert) {
          // this is effectively an insert, but using an upsert instead due
          // to current inability of the framework to clean up after itself
          // between test runs.
          collection.replaceOne(new Document("_id", toInsert.get("_id")),
              toInsert, UPDATE_WITH_UPSERT);
        } else {
          collection.insertOne(toInsert);
        }
      } else {
        bulkInserts.add(toInsert);
        if (bulkInserts.size() == batchSize) {
          if (useUpsert) {
            List<UpdateOneModel<Document>> updates = 
                new ArrayList<UpdateOneModel<Document>>(bulkInserts.size());
            for (Document doc : bulkInserts) {
              updates.add(new UpdateOneModel<Document>(
                  new Document("_id", doc.get("_id")),
                  doc, UPDATE_WITH_UPSERT));
            }
            collection.bulkWrite(updates);
          } else {
            collection.insertMany(bulkInserts, INSERT_UNORDERED);
          }
          bulkInserts.clear();
        } else {
          return Status.BATCHED_OK;
        }
      }
      return Status.OK;
    } catch (Exception e) {
      System.err.println("Exception while trying bulk insert with "
          + bulkInserts.size());
      e.printStackTrace();
      return Status.ERROR;
    }

  }

  /**
   * Read a record from the database. Each field/value pair from the result will
   * be stored in a HashMap.
   * 
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to read.
   * @param fields
   *          The list of fields to read, or null for all of them
   * @param result
   *          A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error or "not found".
   */
  @Override
  public Status read(String table, String key, Set<String> fields,
      HashMap<String, ByteIterator> result) {
    try {
      MongoCollection<Document> collection = database.getCollection(table);
      Document query;

      if (nested) {
        StringBuilder path = new StringBuilder("");
        for (int i = nestingDepth; i > 0; i++) {
            path.append(String.format("ycsb_key%d.", i));
        }
        path.append("ycsb_key");

        query = new Document(path.toString(), key);
      }
      else {
        query = new Document("_id", key);
      }

      FindIterable<Document> findIterable = collection.find(query);
      Document projection = new Document();

      if (fields != null && select_all_fields) {
        for (String field : fields) {
          projection.put(field, INCLUDE);
        }
        findIterable.projection(projection);
      }

      if (select_one_field) {
	    for(String field : select_field_path) {
          projection.put(field, INCLUDE);
	    }
        findIterable.projection(projection);
      }

      Document queryResult = findIterable.first();


      if (queryResult != null) {
        //fillMap(result, queryResult);
      }
      //ystem.out.println(result.toString());S
      return queryResult != null ? Status.OK : Status.NOT_FOUND;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    }
  }

  /**
   * Perform a range scan for a set of records in the database. Each field/value
   * pair from the result will be stored in a HashMap.
   * 
   * @param table
   *          The name of the table
   * @param startkey
   *          The record key of the first record to read.
   * @param recordcount
   *          The number of records to read
   * @param fields
   *          The list of fields to read, or null for all of them
   * @param result
   *          A Vector of HashMaps, where each HashMap is a set field/value
   *          pairs for one record
   * @return Zero on success, a non-zero error code on error. See the {@link DB}
   *         class's description for a discussion of error codes.
   */
  @Override
  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    MongoCursor<Document> cursor = null;
    try {
      MongoCollection<Document> collection = database.getCollection(table);

      Document scanRange = new Document("$gte", startkey);
      Document query = new Document("_id", scanRange);
      Document sort = new Document("_id", INCLUDE);

      FindIterable<Document> findIterable =
          collection.find(query).sort(sort).limit(recordcount);

      if (fields != null) {
        Document projection = new Document();
        for (String fieldName : fields) {
          projection.put(fieldName, INCLUDE);
        }
        findIterable.projection(projection);
      }

      cursor = findIterable.iterator();

      if (!cursor.hasNext()) {
        System.err.println("Nothing found in scan for key " + startkey);
        return Status.ERROR;
      }

      result.ensureCapacity(recordcount);

      while (cursor.hasNext()) {
        HashMap<String, ByteIterator> resultMap =
            new HashMap<String, ByteIterator>();

        Document obj = cursor.next();
        fillMap(resultMap, obj);

        result.add(resultMap);
      }

      return Status.OK;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    } finally {
      if (cursor != null) {
        cursor.close();
      }
    }
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key, overwriting any existing values with the same field name.
   * 
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to write.
   * @param values
   *          A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error. See this class's
   *         description for a discussion of error codes.
   */
  @Override
  public Status update(String table, String key,
      HashMap<String, ByteIterator> values) {
    try {
      MongoCollection<Document> collection = database.getCollection(table);

      Document query = new Document("_id", key);
      Document update;
      
      if (inc_field != null)
      {
        Document fieldsToSet = new Document(inc_field, new Integer(1));
        update = new Document("$inc", fieldsToSet);
      }
      else
      {
        Document fieldsToSet = new Document();
        if (update_all_fields) {
          for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
            fieldsToSet.put(entry.getKey(), entry.getValue().toArray());
          }
        }
  
        if (update_one_field) {
          fieldsToSet.put(update_field, values.entrySet().iterator().next().getValue().toArray());
        }
  
        update = new Document("$set", fieldsToSet);
      }

      UpdateResult result = collection.updateOne(query, update);
      if (result.wasAcknowledged() && result.getMatchedCount() == 0) {
        System.err.println("Nothing updated for key " + key);
        return Status.NOT_FOUND;
      }
      return Status.OK;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    }
  }

  /**
   * Fills the map with the values from the DBObject.
   * 
   * @param resultMap
   *          The map to fill/
   * @param obj
   *          The object to copy values from.
   */
  protected void fillMap(Map<String, ByteIterator> resultMap, Document obj) {
    for (Map.Entry<String, Object> entry : obj.entrySet()) {
      if (entry.getValue() instanceof Binary) {
        resultMap.put(entry.getKey(),
            new ByteArrayByteIterator(((Binary) entry.getValue()).getData()));
      }
    }
  }

  protected void soeFillMap(Map<String, ByteIterator> resultMap, Document obj) {
    for (Map.Entry<String, Object> entry : obj.entrySet()) {
      String value = "null";
      if (entry.getValue() != null) {
        value = entry.getValue().toString();
      }
      resultMap.put(entry.getKey(), new StringByteIterator(value));
    }
  }
}
