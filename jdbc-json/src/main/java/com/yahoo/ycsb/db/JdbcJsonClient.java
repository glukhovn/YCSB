/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
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

package com.yahoo.ycsb.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang.StringEscapeUtils;
import org.json.*;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.generator.soe.Generator;
import com.yahoo.ycsb.workloads.soe.SoeQueryPredicate;

/**
 * A class that wraps a JDBC compliant database to allow it to be interfaced with YCSB.
 * This class extends {@link DB} and implements the database interface used by YCSB client.
 *
 * <br> Each client will have its own instance of this class. This client is
 * not thread safe.
 *
 * <br> This interface expects a schema <key> <field1> <field2> <field3> ...
 * All attributes are of type VARCHAR. All accesses are through the primary key. Therefore,
 * only one index on the primary key is needed.
 *
 * <p> The following options must be passed when using this database client.
 *
 * <ul>
 * <li><b>db.driver</b> The JDBC driver class to use.</li>
 * <li><b>db.url</b> The Database connection URL.</li>
 * <li><b>db.user</b> User name for the connection.</li>
 * <li><b>db.passwd</b> Password for the connection.</li>
 * </ul>
 *
 * @author sudipto
 *
 */
public abstract class JdbcJsonClient extends DB implements JdbcDBClientConstants {

  private ArrayList<Connection> conns;
  private boolean initialized = false;
  private Properties props;

  protected static boolean pk_column;
  protected static boolean flat_key;
  protected static boolean nested_key;

  protected static boolean select_all_fields;
  protected static boolean select_one_field;
  protected static String select_field_path;

  protected static boolean update_one_field;
  protected static boolean update_all_fields;
  protected static String update_field;

  protected static int nesting_key_depth;
  protected static int document_depth;
  protected static int document_width;
  protected static int element_values;
  protected static int element_obj;

  private int jdbcFetchSize;
  private int batchSize;
  private boolean autoCommit;
  private boolean batchUpdates;
  private boolean advisoryLocks;
  private static final String DEFAULT_PROP = "";
  private ConcurrentMap<StatementType, PreparedStatement> cachedStatements;
  private long numRowsInBatch = 0;

  protected static class Predicate {
    enum Type {
      ID_EQ(1),
      ID_GT(2),
      PATH_EQ(3),
      PATH_GT(4),
      PATH_GE(5),
      PATH_LT(6),
      PATH_LE(7),
      PATH_ARR_EQ(8),
      AND(9),
      OR(10)
      ;

      int internalType;
      private Type(int type) {
        internalType = type;
      }

      int getHashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + internalType;
        return result;
      }
    }

    public Type type;
    public Predicate[] children;
    public Object[] keys;
    public String val;

    public String[] getPath() {
      return (String[]) keys;
    }

    public Predicate(Type type, String[] path, String val) {
      this.type = type;
      this.keys = path;
      this.val = val;
      this.children = null;
    }

    public Predicate(Type type, Predicate[] children) {
      this.type = type;
      this.keys = null;
      this.val = null;
      this.children = children;
    }

    public int typeHashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + (type == null ? 0 : type.getHashCode());
      result = prime * result + (keys == null ? 0 : keys.hashCode());
      if (children != null)
        for (Predicate child : children)
          result = prime * result + child.typeHashCode();
      return result;
    }

    public boolean typeEquals(Predicate other) {
      if (this == other)
        return true;

      if (other == null)
        return false;

      if (type != other.type)
        return false;

      if (keys == null ? other.keys != null : !keys.equals(other.keys))
        return false;

      if (children != null) {
        if (other.children == null || children.length != other.children.length)
          return false;
        for (int i = 0; i < children.length; i++)
          if (!children[i].typeEquals(other.children[i]))
            return false;
      } else if (other.children != null)
        return false;

      return true;
    }
  }

  protected static class Sort {
    String[] path;
    boolean asc;

    public Sort(String[] path, boolean asc) {
      this.path = path;
      this.asc = asc;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      Sort other = (Sort) obj;
      if (path == null ? other.path != null : !path.equals(other.path))
        return false;
      if (asc != other.asc)
        return false;
      return true;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + (asc ? 0 : 1) * 100;
      result = prime * result + (path == null ? 0 : path.hashCode());
      return result;
    }
  }

  /**
   * The statement type for the prepared statements.
   */
  protected static class StatementType {

    enum Type {
      INSERT(1),
      DELETE(2),
      READ(3),
      UPDATE(4),
      SCAN(5),
      LOCK_SHARED(6),
      LOCK_EXCLUSIVE(7),
      UNLOCK_SHARED(8),
      UNLOCK_EXCLUSIVE(9)
      ;
      int internalType;
      private Type(int type) {
        internalType = type;
      }

      int getHashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + internalType;
        return result;
      }
    }

    Type type;
    int shardIndex;
    int numFields;
    String tableName;
    List<String> fields;
    Predicate pred;
    Sort[] sort;

    StatementType(Type type, String tableName, int _shardIndex, Predicate pred, Sort[] sort, Collection<String> fields) {
      this.type = type;
      this.tableName = tableName;
      this.pred = pred;
      this.sort = sort;
      this.numFields = fields == null ? 0 : fields.size();
      this.shardIndex = _shardIndex;
      this.fields = fields == null ? null : new ArrayList<String>(fields);
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + numFields + 100 * shardIndex;
      result = prime * result + (tableName == null ? 0 : tableName.hashCode());
      result = prime * result + (type == null ? 0 : type.getHashCode());
      result = prime * result + (pred == null ? 0 : pred.typeHashCode());
      result = prime * result + (sort == null ? 0 : sort.hashCode());
      result = prime * result + (fields == null ? 0 : fields.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      StatementType other = (StatementType) obj;
      if (type != other.type)
        return false;
      if (numFields != other.numFields)
        return false;
      if (shardIndex != other.shardIndex)
        return false;
      if (tableName == null ? other.tableName != null : !tableName.equals(other.tableName))
        return false;
      if (pred == null ? other.pred != null : !pred.typeEquals(other.pred))
        return false;
      if (sort == null ? other.sort != null : !sort.equals(other.sort))
        return false;
      if (fields == null ? other.fields != null : !fields.equals(other.fields))
        return false;
      return true;
    }
  }

  /**
   * For the given key, returns what shard contains data for this key
   *
   * @param key Data key to do operation on
   * @return Shard index
   */
  private int getShardIndexByKey(String key) {
    if (key == null)
      return 0;
    int ret = Math.abs(key.hashCode()) % conns.size();
    return ret;
  }

  /**
   * For the given key, returns Connection object that holds connection to the
   * shard that contains this key
   *
   * @param key Data key to get information for
   * @return Connection object
   */
  private Connection getShardConnectionByKey(String key) {
    return conns.get(getShardIndexByKey(key));
  }

  private void cleanupAllConnections() throws SQLException {
    for (Connection conn : conns) {
      if (!autoCommit) {
        conn.commit();
      }
      conn.close();
    }
  }

  /** Returns parsed int value from the properties if set, otherwise returns -1. */
  private static int getIntProperty(Properties props, String key) throws DBException {
    String valueStr = props.getProperty(key);
    if (valueStr != null) {
      try {
        return Integer.parseInt(valueStr);
      } catch (NumberFormatException nfe) {
        System.err.println("Invalid " + key + " specified: " + valueStr);
        throw new DBException(nfe);
      }
    }
    return -1;
  }

  /**
   * Returns parsed boolean value from the properties if set, otherwise returns
   * defaultVal.
   */
  private static boolean getBoolProperty(Properties props, String key, boolean defaultVal) {
    String valueStr = props.getProperty(key);
    if (valueStr != null) {
      return Boolean.parseBoolean(valueStr);
    }
    return defaultVal;
  }

  /**
   * Initialize the database connection and set it up for sending requests to the database.
   * This must be called once per client.
   * @throws
   */
  @Override
  public void init() throws DBException {
    if (initialized) {
      System.err.println("Client connection already initialized.");
      return;
    }
    props = getProperties();
    String urls = props.getProperty(CONNECTION_URL, DEFAULT_PROP);
    String user = props.getProperty(CONNECTION_USER, DEFAULT_PROP);
    String passwd = props.getProperty(CONNECTION_PASSWD, DEFAULT_PROP);
    String driver = props.getProperty(DRIVER_CLASS);

    pk_column = Boolean.parseBoolean(props.getProperty(PK_COLUMN, "false"));
    flat_key = Boolean.parseBoolean(props.getProperty(FLAT_KEY, "true"));
    nested_key = Boolean.parseBoolean(props.getProperty(NESTED_KEY, "false"));

    select_all_fields = Boolean.parseBoolean(props.getProperty("select_all_fields", "true"));
    select_one_field = Boolean.parseBoolean(props.getProperty("select_one_field", "false"));
    select_field_path = props.getProperty("select_field_path");

    update_all_fields = Boolean.parseBoolean(props.getProperty("update_all_fields", "true"));
    update_one_field = Boolean.parseBoolean(props.getProperty("update_one_field", "false"));
    update_field = props.getProperty("update_field");

    nesting_key_depth = nested_key ? Integer.parseInt(props.getProperty("depth", "1")) : 1;
    document_depth = Integer.parseInt(props.getProperty("document_depth", "0"));
    document_width = Integer.parseInt(props.getProperty("document_width", "0"));
    element_values = Integer.parseInt(props.getProperty("element_values", "0"));
    element_obj = Integer.parseInt(props.getProperty("element_obj", "2"));

    jdbcFetchSize = getIntProperty(props, JDBC_FETCH_SIZE);
    batchSize = getIntProperty(props, DB_BATCH_SIZE);

    autoCommit = getBoolProperty(props, JDBC_AUTO_COMMIT, true);
    batchUpdates = getBoolProperty(props, JDBC_BATCH_UPDATES, false);
    advisoryLocks = getBoolProperty(props, ADVISORY_LOCKS, false);

    try {
      if (driver != null) {
        Class.forName(driver);
      }
      int shardCount = 0;
      conns = new ArrayList<Connection>(3);
      final String[] urlArr = urls.split(",");
      for (String url : urlArr) {
        System.out.println("Adding shard node URL: " + url);
        Connection conn = DriverManager.getConnection(url, user, passwd);

        // Since there is no explicit commit method in the DB interface, all
        // operations should auto commit, except when explicitly told not to
        // (this is necessary in cases such as for PostgreSQL when running a
        // scan workload with fetchSize)
        conn.setAutoCommit(autoCommit);

        shardCount++;
        conns.add(conn);
      }

      System.out.println("Using shards: " + shardCount + ", batchSize:" + batchSize + ", fetchSize: " + jdbcFetchSize);

      cachedStatements = new ConcurrentHashMap<StatementType, PreparedStatement>();
    } catch (ClassNotFoundException e) {
      System.err.println("Error in initializing the JDBS driver: " + e);
      throw new DBException(e);
    } catch (SQLException e) {
      System.err.println("Error in database operation: " + e);
      throw new DBException(e);
    } catch (NumberFormatException e) {
      System.err.println("Invalid value for fieldcount property. " + e);
      throw new DBException(e);
    }

    initialized = true;
  }

  @Override
  public void cleanup() throws DBException {
    if (batchSize > 0) {
      try {
        // commit un-finished batches
        for (PreparedStatement st : cachedStatements.values()) {
          if (!st.getConnection().isClosed() && !st.isClosed() && (numRowsInBatch % batchSize != 0)) {
            st.executeBatch();
          }
        }
      } catch (SQLException e) {
        System.err.println("Error in cleanup execution. " + e);
        throw new DBException(e);
      }
    }

    try {
      cleanupAllConnections();
    } catch (SQLException e) {
      System.err.println("Error in closing the connection. " + e);
      throw new DBException(e);
    }
  }

  protected PreparedStatement createAndCacheStatement(StatementType statementType, String key, String sql, int fetchSize)
      throws SQLException {
    PreparedStatement statement = getShardConnectionByKey(key).prepareStatement(sql);
    if (fetchSize > 0)
      statement.setFetchSize(fetchSize);
    PreparedStatement stmt = cachedStatements.putIfAbsent(statementType, statement);
    return stmt == null ? statement: stmt;
  }

  protected PreparedStatement createAndCacheStatement(StatementType statementType, String key, String sql)
      throws SQLException {
    return createAndCacheStatement(statementType, key, sql, -1);
  }

  protected abstract String createInsertStatement(StatementType insertType);

  protected abstract String createReadStatement(StatementType readType);

  protected abstract String createDeleteStatement(StatementType deleteType);

  protected abstract String createUpdateStatement(StatementType updateType);

  protected abstract String createScanStatement(StatementType scanType);

  protected abstract String createLockStatement(StatementType lockType);

  protected static void appendJsonKey(StringBuilder json, String key) {
    for (int i = 1; i < nesting_key_depth; i++)
      json.append(String.format("\"%s%d\": {", PRIMARY_KEY, i));

    json.append(String.format("\"%s\" : \"%s\"", PRIMARY_KEY, key));
    json.append(new String(new char[nesting_key_depth - 1]).replace("\0", "}"));
  }

  protected String buildConditionKey(String key) {
    return key;
  }

  protected String buildScanKey(String key) {
    return key;
  }

  protected String buildLockKey(String key) {
    return key;
  }

  protected abstract List<String> buildUpdateValues(HashMap<String, ByteIterator> values);

  protected String buildInsertValue(String key, HashMap<String, ByteIterator> values) {
    StringBuilder json = new StringBuilder("{");

    appendJsonKey(json, key);

    if (document_depth == 0) {
      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        String value = entry.getValue().toString();
        json.append(String.format(", \"%s\": \"%s\"", entry.getKey(), StringEscapeUtils.escapeJava(value)));
      }
    } else {
      ArrayList<JSONObject> obj_keys = new ArrayList<JSONObject>();
      ArrayList<JSONObject> top_keys = obj_keys;
      ArrayList<JSONObject> current_keys;
      LinkedList<ByteIterator> val_list = new LinkedList<ByteIterator>(values.values());
      int depth = 0;
      int index = 2;

      for (int i = 0; i < document_width; i++)
        obj_keys.add(new JSONObject());

      while (depth < document_depth) {
        current_keys = new ArrayList<JSONObject>();
        for (JSONObject obj : obj_keys) {
          // put values
          for (int i = 0; i < element_values; i++) {
            obj.put(String.format("%s%d", COLUMN_PREFIX, index), val_list.pop());
            index++;
          }

          if (depth < document_depth - 1) {
            // put objects
            for (int i = 0; i < element_obj; i++) {
              JSONObject child = new JSONObject();
              obj.put(String.format("%s%d", COLUMN_PREFIX, index), child);
              current_keys.add(child);
              index++;
            }
          } else {
            // put values
            for (int i = 0; i < element_obj; i++) {
              obj.put(String.format("%s%d", COLUMN_PREFIX, index), val_list.pop());
              index++;
            }
          }
        }

        obj_keys = current_keys;
        depth++;
      }

      for (JSONObject obj : top_keys)
        json.append(String.format(", \"%s%d\": %s", COLUMN_PREFIX, index++, obj.toString()));
    }

    json.append("}");

    return json.toString();
  }

  protected String buildUpdateValue(HashMap<String, ByteIterator> values) {
    StringBuilder json = new StringBuilder("{");

    int index = 0;

    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      if (index++ > 0)
        json.append(", ");

      json.append("\"")
          .append(update_one_field ? update_field : entry.getKey())
          .append("\": \"")
          .append(StringEscapeUtils.escapeJava(entry.getValue().toString()))
          .append("\"");

      if (update_one_field)
        break;
    }

    return json.append("}").toString();
  }

  private PreparedStatement getLockStatement(String tableName, String key, StatementType.Type lockType) throws SQLException {
      if (!this.advisoryLocks)
        return null;
      Predicate pred = new Predicate(Predicate.Type.ID_EQ, null, key);
      StatementType type = new StatementType(lockType, tableName, getShardIndexByKey(key), pred, null, null);
      PreparedStatement lockStatement = cachedStatements.get(type);

      if (lockStatement == null)
        lockStatement = createAndCacheStatement(type, key, createLockStatement(type));

      lockStatement.setString(1, buildLockKey(key));

      return lockStatement;
  }

  @Override
  public Status read(String tableName, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
    try {
      Predicate pred = new Predicate(Predicate.Type.ID_EQ, null, key);
      StatementType type = new StatementType(StatementType.Type.READ, tableName, getShardIndexByKey(key), pred, null, fields);
      PreparedStatement readStatement = cachedStatements.get(type);
      if (readStatement == null)
        readStatement = createAndCacheStatement(type, key, createReadStatement(type));
      
      PreparedStatement lockStatement =
        getLockStatement(tableName, key, StatementType.Type.LOCK_SHARED);
      PreparedStatement unlockStatement =
        getLockStatement(tableName, key, StatementType.Type.UNLOCK_SHARED);

      if (lockStatement != null)
        lockStatement.execute();

      readStatement.setString(1, buildConditionKey(key));

      ResultSet resultSet = readStatement.executeQuery();

      Status status = Status.NOT_FOUND;

      if (resultSet.next()) {
        status = Status.OK;

        if (result != null && fields != null) {
          for (String field : fields) {
            String col = field == null ? "_data_" : field;
            String value = resultSet.getString(col);
            result.put(field, new StringByteIterator(value));
          }
        }
      }

      resultSet.close();

      if (unlockStatement != null)
        unlockStatement.execute();

      return status;
    } catch (SQLException e) {
        System.err.println("Error in processing read of table " + tableName + ": "+e);
      return Status.ERROR;
    }
  }

  protected abstract StringBuilder appendPrimaryKey(StringBuilder builder);
  protected abstract StringBuilder appendPath(StringBuilder builder, String alias, String[] path);

  protected StringBuilder appendPathCond(StringBuilder builder, String[] path, String op) {
    return appendPath(builder, "", path).append(" ").append(op).append(" ?");
  }

  protected StringBuilder appendPred(StringBuilder builder, Predicate pred) {
    switch (pred.type) {
      case AND:
      case OR:
        String op = pred.type == Predicate.Type.AND ? " AND " : " OR ";
        for (Predicate child : pred.children) {
          appendPred(builder, child).append(op);
        }
        return builder.append(pred.type == Predicate.Type.AND ? "TRUE" : "FALSE");
      case ID_EQ:
        return appendPrimaryKey(builder).append(" = ?");
      case ID_GT:
        return appendPrimaryKey(builder).append(" >= ?");
      case PATH_EQ:
        return appendPathCond(builder, pred.getPath(), "=");
      case PATH_GT:
        return appendPathCond(builder, pred.getPath(), ">");
      case PATH_GE:
        return appendPathCond(builder, pred.getPath(), ">=");
      case PATH_LT:
        return appendPathCond(builder, pred.getPath(), "<");
      case PATH_LE:
        return appendPathCond(builder, pred.getPath(), "<=");
      case PATH_ARR_EQ:
        throw new UnsupportedOperationException();
    }
    throw new UnsupportedOperationException();
  }

  protected List<String> buildPredValues(Predicate pred) {
    switch (pred.type) {
      case AND:
        List<String> vals = new ArrayList<String>(pred.children.length);
        for (Predicate child : pred.children)
          vals.addAll(buildPredValues(child));
        return vals;
      default:
        return Collections.singletonList(buildConditionKey(pred.val));
    }
  }

  private void executeScan(PreparedStatement scanStatement, Set<String> fields, Vector<HashMap<String, ByteIterator>> result, int recordcount)
      throws SQLException {
    ResultSet resultSet = scanStatement.executeQuery();

    if (result != null && fields != null)
      result.ensureCapacity(recordcount);

    for (int i = 0; i < recordcount && resultSet.next(); i++) {
      if (result != null && fields != null) {
        HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
        for (String field : fields) {
          String value = resultSet.getString(field);
          values.put(field, new StringByteIterator(value));
        }
        result.add(values);
      }
    }

    resultSet.close();
  }

  protected Status scan(String tableName, String shardKey,
                        Predicate pred, Sort[] sort,
                        int recordcount, int offset, Set<String> fields,
                        Vector<HashMap<String, ByteIterator>> result) {
    try {
      StatementType type = new StatementType(StatementType.Type.SCAN, tableName, getShardIndexByKey(shardKey), pred, sort, fields);
      PreparedStatement scanStatement = cachedStatements.get(type);
      if (scanStatement == null)
        scanStatement = createAndCacheStatement(type, shardKey, createScanStatement(type), this.jdbcFetchSize);

      int param = 1;

      for (String val : buildPredValues(pred))
        scanStatement.setString(param++, val);

      scanStatement.setInt(param++, recordcount);
      scanStatement.setInt(param++, offset);

      executeScan(scanStatement, fields, result, recordcount);

      return Status.OK;
    } catch (SQLException e) {
      System.err.println("Error in processing scan of table: " + tableName + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String tableName, String startKey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    Predicate pred = new Predicate(Predicate.Type.ID_GT, null, startKey);
    Sort[] sort = new Sort[] { new Sort(null, true) };

    return scan(tableName, startKey, pred, sort, recordcount, 0, fields, result);
  }

  @Override
  public Status update(String tableName, String key, HashMap<String, ByteIterator> values) {
    try {
      Predicate pred = new Predicate(Predicate.Type.ID_EQ, null, key);
      StatementType type = new StatementType(StatementType.Type.UPDATE, tableName, getShardIndexByKey(key), pred, null, values.keySet());
      PreparedStatement updateStatement = cachedStatements.get(type);
      if (updateStatement == null)
        updateStatement = createAndCacheStatement(type, key, createUpdateStatement(type));

      PreparedStatement lockStatement =
        getLockStatement(tableName, key, StatementType.Type.LOCK_EXCLUSIVE);
      PreparedStatement unlockStatement =
        getLockStatement(tableName, key, StatementType.Type.UNLOCK_EXCLUSIVE);

      if (lockStatement != null)
        lockStatement.execute();

      int index = 1;

      for (String val : buildUpdateValues(values))
        updateStatement.setString(index++, val);

      updateStatement.setString(index, buildConditionKey(key));

      int result = updateStatement.executeUpdate();

      if (unlockStatement != null)
        unlockStatement.execute();

      return result == 1 || result == 2 ? Status.OK : Status.UNEXPECTED_STATE;
    } catch (SQLException e) {
      System.err.println("Error in processing update to table: " + tableName + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String tableName, String key, HashMap<String, ByteIterator> values) {
    try {
      StatementType type = new StatementType(StatementType.Type.INSERT, tableName, getShardIndexByKey(key), null, null, values.keySet());
      PreparedStatement insertStatement = cachedStatements.get(type);
      if (insertStatement == null)
        insertStatement = createAndCacheStatement(type, key, createInsertStatement(type));

      PreparedStatement lockStatement =
        getLockStatement(tableName, key, StatementType.Type.LOCK_EXCLUSIVE);
      PreparedStatement unlockStatement =
        getLockStatement(tableName, key, StatementType.Type.UNLOCK_EXCLUSIVE);

      insertStatement.setString(1, buildInsertValue(key, values));
      if (pk_column)
        insertStatement.setString(2, key);

      // Using the batch insert API
      if (batchUpdates) {
        insertStatement.addBatch();
        // Check for a sane batch size
        if (batchSize > 0) {
          // Commit the batch after it grows beyond the configured size
          if (++numRowsInBatch % batchSize == 0) {
            int[] results = insertStatement.executeBatch();
            for (int r : results) {
              if (r != 1 && r != Statement.SUCCESS_NO_INFO) {
                System.err.println("executeBatch() returned " + r);
                return Status.ERROR;
              }
            }
            // If autoCommit is off, make sure we commit the batch
            if (!autoCommit) {
              getShardConnectionByKey(key).commit();
            }
            return Status.OK;
          } // else, the default value of -1 or a nonsense. Treat it as an infinitely large batch.
        } // else, we let the batch accumulate
        // Added element to the batch, potentially committing the batch too.
        return Status.BATCHED_OK;
      } else {
        // Normal update
        int result = insertStatement.executeUpdate();
        // If we are not autoCommit, we might have to commit now
        if (!autoCommit) {
          // Let updates be batcher locally
          if (batchSize > 0) {
            if (++numRowsInBatch % batchSize == 0) {
              // Send the batch of updates
              getShardConnectionByKey(key).commit();
            }
            // uhh
            return Status.OK;
          } else {
            // Commit each update
            getShardConnectionByKey(key).commit();
          }
        }
        if (result == 1) {
          return Status.OK;
        }
      }
      return Status.UNEXPECTED_STATE;
    } catch (SQLException e) {
      System.err.println("Error in processing insert to table: " + tableName + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String tableName, String key) {
    try {
      Predicate pred = new Predicate(Predicate.Type.ID_EQ, null, key);
      StatementType type = new StatementType(StatementType.Type.DELETE, tableName, getShardIndexByKey(key), pred, null, null);
      PreparedStatement deleteStatement = cachedStatements.get(type);
      if (deleteStatement == null)
        deleteStatement = createAndCacheStatement(type, key, createDeleteStatement(type));

      PreparedStatement lockStatement =
        getLockStatement(tableName, key, StatementType.Type.LOCK_EXCLUSIVE);
      PreparedStatement unlockStatement =
        getLockStatement(tableName, key, StatementType.Type.UNLOCK_EXCLUSIVE);

      if (lockStatement != null)
        lockStatement.execute();

      deleteStatement.setString(1, buildConditionKey(key));

      int result = deleteStatement.executeUpdate();

      if (unlockStatement != null)
        unlockStatement.execute();

      return result == 1 ? Status.OK : Status.UNEXPECTED_STATE;
    } catch (SQLException e) {
      System.err.println("Error in processing delete to table: " + tableName + e);
      return Status.ERROR;
    }
  }

  private static String mapToJson(HashMap<String, ByteIterator> obj) {
    JSONWriter writer = new JSONStringer();

    writer.object();

    for (Map.Entry<String, ByteIterator> entry : obj.entrySet())
      writer.key(entry.getKey()).value(entry.getValue().toString());

    return writer.endObject().toString();
  }

  /*
   *  ================    SOE operations  ======================
   */

  @Override
  public Status soeLoad(String table, Generator generator) {
    String key = generator.getCustomerIdRandom();
    Set<String> fields = new HashSet<String>();
    HashMap<String, ByteIterator> result = new HashMap<>();

    fields.add(null);
    fields.add(Generator.SOE_FIELD_CUSTOMER_ORDER_LIST);

    Status res = read(table, key, fields, result);

    if (res == Status.ERROR)
      return res;

    if (res == Status.NOT_FOUND || result.size() <= 0) {
      System.out.println("Empty return");
      return Status.OK;
    }

    //for (Map.Entry<String, ByteIterator> e : result.entrySet())
    //  System.out.println(e.getKey() + " : " + e.getValue());

    try {
      /*
      ByteIterator dob = result.get(Generator.SOE_FIELD_CUSTOMER_DOB);
      if (dob != null) {
        SimpleDateFormat sdfr = new SimpleDateFormat("yyyy-MM-dd");
        String dobStr = sdfr.format(dob.toString());
        result.put(Generator.SOE_FIELD_CUSTOMER_DOB, new StringByteIterator(dobStr));
      }
      */

      generator.putCustomerDocument(key, result.get(null).toString()); //mapToJson(result));

      ByteIterator ordersIt = result.get(Generator.SOE_FIELD_CUSTOMER_ORDER_LIST);
/*
      if (ordersIt != null && !ordersIt.toString().equals("null")) {
        JSONArray orders = new JSONArray(ordersIt.toString());

        for (int i = 0; i < orders.length(); i++) {
          String order = orders.getString(i);

          result.clear();
          res = read(table, order, fields, result);

          if (res == Status.ERROR || res == Status.NOT_FOUND || result.size() <= 0)
            return Status.ERROR;

          generator.putOrderDocument(order, mapToJson(result));
        }
      }
*/
      return Status.OK;
    } catch (Exception e) {
      System.err.println(e.toString());
      e.printStackTrace(System.err);
    }
    return Status.ERROR;
  }

  // *********************  SOE Insert ********************************

  @Override
  public Status soeInsert(String table, HashMap<String, ByteIterator> result, Generator gen)  {

    try {
      String key = gen.getPredicate().getDocid();
      String value = gen.getPredicate().getValueA();
      Set<String> fields = null; // FIXME
      StatementType type = new StatementType(StatementType.Type./*SOE_*/INSERT, table, getShardIndexByKey(key), null, null, fields);
      PreparedStatement insertStatement = cachedStatements.get(type);
      if (insertStatement == null)
        insertStatement = createAndCacheStatement(type, key, createInsertStatement(type));

      PreparedStatement lockStatement =
        getLockStatement(table, key, StatementType.Type.LOCK_EXCLUSIVE);
      PreparedStatement unlockStatement =
        getLockStatement(table, key, StatementType.Type.UNLOCK_EXCLUSIVE);

      StringBuilder json = new StringBuilder("{");
      appendJsonKey(json, key);
      json.append(", ");
      json.append("\"value\": ").append(value);
      json.append("}");

      insertStatement.setString(1, json.toString());
      if (pk_column)
        insertStatement.setString(2, key);

        int res = insertStatement.executeUpdate();

        // If we are not autoCommit, we might have to commit now
        if (!autoCommit) {
          // Let updates be batcher locally
          if (batchSize > 0) {
            if (++numRowsInBatch % batchSize == 0) {
              // Send the batch of updates
              getShardConnectionByKey(key).commit();
            }
            // uhh
            return Status.OK;
          } else {
            // Commit each update
            getShardConnectionByKey(key).commit();
          }
        }

      if (res != 1)
        return Status.UNEXPECTED_STATE;

      return Status.OK;
    } catch (Exception e) {
      System.err.println("Exception while trying bulk insert with " + batchSize);
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  // *********************  SOE Update ********************************

  @Override
  public Status soeUpdate(String table, HashMap<String, ByteIterator> result, Generator gen) {
    String key = gen.getCustomerIdWithDistribution();
    String updateFieldName = gen.getPredicate().getNestedPredicateA().getName();
    String updateFieldValue = gen.getPredicate().getNestedPredicateA().getValueA();

    HashMap<String, ByteIterator> values = new HashMap<>();
    values.put(updateFieldName, new StringByteIterator(updateFieldValue));

    return update(table, key, values);
  }

  // *********************  SOE Read ********************************

  @Override
  public Status soeRead(String table, HashMap<String, ByteIterator> result, Generator gen) {
    String key = gen.getCustomerIdWithDistribution();
    Set<String> fields = gen.getAllFields();
    //Set<String> fields = new HashSet<String>(); fields.add("_id");

    return read(table, key, fields, result);
  }


  // *********************  SOE Scan ********************************

  @Override
  public Status soeScan(String table, final Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    String startkey = gen.getCustomerIdWithDistribution();
    Set<String> fields = gen.getAllFields();
    int recordcount = gen.getRandomLimit();

    Status status = scan(table, startkey, recordcount, fields, result);

    if (result.isEmpty()) {
      System.err.println("Nothing found in scan for key " + startkey);
      return Status.ERROR;
    }

    return status;
  }

  // *********************  SOE Page ********************************

  @Override
  public Status soePage(String table, final Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    int recordcount = gen.getRandomLimit();
    int offset = gen.getRandomOffset();
    Set<String> fields = gen.getAllFields();
    String addrzipName[] = new String[] { gen.getPredicate().getName(), gen.getPredicate().getNestedPredicateA().getName() };
    String addzipValue = gen.getPredicate().getNestedPredicateA().getValueA();
    Predicate pred = new Predicate(Predicate.Type.PATH_EQ, addrzipName, addzipValue);

    Status res = scan(table, null, pred, null, recordcount, offset, fields, result);

    if (result.size() <= 0) {
      //System.err.println("Nothing found for " + addrzipName + " = " + addzipValue);
      return Status.NOT_FOUND;
    }

    return res;
  }


  // *********************  SOE search ********************************

  @Override
  public Status soeSearch(String table, final Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    int recordcount = gen.getRandomLimit();
    int offset = gen.getRandomOffset();

    String[] addrcountryName = new String[] {
        gen.getPredicatesSequence().get(0).getName(),
        gen.getPredicatesSequence().get(0).getNestedPredicateA().getName()
      };
    String[] agegroupName = new String [] { gen.getPredicatesSequence().get(1).getName() };
    String[] dobyearName = new String [] { gen.getPredicatesSequence().get(2).getName() };

    String addrcountryValue = gen.getPredicatesSequence().get(0).getNestedPredicateA().getValueA();
    String agegroupValue = gen.getPredicatesSequence().get(1).getValueA();
    String dobyearValue = gen.getPredicatesSequence().get(2).getValueA();

    Predicate pred1 = new Predicate(Predicate.Type.PATH_EQ, addrcountryName, addrcountryValue);
    Predicate pred2 = new Predicate(Predicate.Type.PATH_EQ, agegroupName, agegroupValue);
    Predicate pred3;
    Predicate pred4;
    try {
      pred3 = new Predicate(Predicate.Type.PATH_GE, dobyearName, (new SimpleDateFormat("yyyy-MM-dd").parse(dobyearValue + "-1-1")).toString());
      pred4 = new Predicate(Predicate.Type.PATH_LE, dobyearName, (new SimpleDateFormat("yyyy-MM-dd").parse(dobyearValue+ "-12-31")).toString());
    } catch (java.text.ParseException ex) {
       return Status.ERROR;
    }

    Predicate pred = new Predicate(Predicate.Type.AND, new Predicate[] { pred1, pred2, pred3, pred4 });

    Sort[] sort = new Sort[] {
      new Sort(addrcountryName, true),
      new Sort(agegroupName, true),
      new Sort(dobyearName, true)
    };

    Status res = scan(table, null, pred, sort, recordcount, offset, gen.getAllFields(), result);

    if (result.size() <= 0)
      return Status.NOT_FOUND;

    return res;
  }


  // *********************  SOE NestScan ********************************

  @Override
  public Status soeNestScan(String table, final Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    int recordcount = gen.getRandomLimit();
    String nestedZipName[] = new String[] {
        gen.getPredicate().getName(),
        gen.getPredicate().getNestedPredicateA().getName(),
        gen.getPredicate().getNestedPredicateA().getNestedPredicateA().getName()
      };
    String nestedZipValue = gen.getPredicate().getNestedPredicateA().getNestedPredicateA().getValueA();
    Predicate pred = new Predicate(Predicate.Type.PATH_EQ, nestedZipName, nestedZipValue);

    Status res = scan(table, null, pred, null, recordcount, 0, gen.getAllFields(), result);

    if (result.size() <= 0)
      return Status.NOT_FOUND;

    return res;
  }

  // *********************  SOE ArrayScan ********************************

  @Override
  public Status soeArrayScan(String table, final Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    int recordcount = gen.getRandomLimit();
    String[] arrName = new String[] { gen.getPredicate().getName() };
    String arrValue = gen.getPredicate().getValueA();

    Sort[] sort = new Sort[] { new Sort(null, true) };

    Predicate pred = new Predicate(Predicate.Type.PATH_ARR_EQ, arrName, arrValue);

    Status res = scan(table, null, pred, sort, recordcount, 0, gen.getAllFields(), result);

    if (result.size() <= 0)
      return Status.NOT_FOUND;

    return res;
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

    Sort[] sort = new Sort[] { new Sort(null, true) };

    Predicate pred1 = new Predicate(Predicate.Type.PATH_ARR_EQ, new String[] { fieldName, fieldCountryName }, fieldCountryValue);
    Predicate pred2 = new Predicate(Predicate.Type.PATH_ARR_EQ, new String[] { fieldName, fieldCitiesName }, fieldCitiesValue);
    Predicate pred = new Predicate(Predicate.Type.AND, new Predicate[] { pred1, pred2 });

    Status res = scan(table, null, pred, sort, -1, 0, gen.getAllFields(), result);

    if (result.size() <= 0)
      return Status.NOT_FOUND;

    return res;
  }

  // *********************  SOE Report ********************************

  @Override
  public Status soeReport(String table, final Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    int recordcount = gen.getRandomLimit();
    String[] orderListName = new String[] {
        gen.getPredicatesSequence().get(0).getName()
      };
    String[] addrZipName = new String[] {
        gen.getPredicatesSequence().get(1).getName(),
        gen.getPredicatesSequence().get(1).getNestedPredicateA().getName()
      };
    String addrZipValue = gen.getPredicatesSequence().get(1).getNestedPredicateA().getValueA();
    Predicate addrZipPred = new Predicate(Predicate.Type.PATH_EQ, addrZipName, addrZipValue);
    Set<String> fields = gen.getAllFields();

    Predicate idInPred =  null;// FIXME new Predicate(Predicate.Type.ID_IN, null, new Subquery(addrZipPred, orderListName));

    return scan(table, null, idInPred, null, -1, 0, fields, result);
/*
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

      StringBuilder query = new StringBuilder();
      query.append("SELECT FROM");

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
    */
  }


  // *********************  SOE Report2 ********************************

  @Override
  public Status soeReport2(String table, final Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    List<SoeQueryPredicate> preds = gen.getPredicatesSequence();
    String[] nameOrderMonth = new String[] { preds.get(0).getName() };
    String[] nameOrderSaleprice = new String[] { preds.get(1).getName() };
    String nameAddress = preds.get(2).getName();
    String[] nameAddressZip = new String[] { nameAddress, preds.get(2).getNestedPredicateA().getName() };
    String[] nameOrderlist = new String[] { preds.get(3).getName() };
    String valueOrderMonth = preds.get(0).getValueA();
    String valueAddressZip = preds.get(2).getNestedPredicateA().getValueA();

    //Predicate addrZipPred = new Predicate(Predicate.Type.PATH_EQ, nameAddressZip, valueAddressZip);

    Set<String> fields = new HashSet<String>(3);
    fields.add(nameOrderlist[0]);
    fields.add(nameAddressZip[1]);
    fields.add("Total");

    StringBuilder builder = new StringBuilder ("SELECT ");
    appendPath(builder, "o.", nameOrderMonth).append(", ");
    appendPath(builder, "c.", nameAddressZip).append(", ");
    appendPath(builder.append("SUM("), "o.", nameOrderSaleprice).append(") AS \"Total\"");
    builder.append(" FROM ").append(table).append(" c INNER JOIN ").append(table)
           .append(" o ON ("); // o._id IN c.order_list)
    appendPath(builder, "o.", null).append(" IN ");
    appendPath(builder, "c.", nameOrderlist).append(") WHERE ");
    appendPath(builder, "c.", nameAddressZip).append(" = ? AND ");
    appendPath(builder, "o.", nameOrderMonth).append(" = ? GROUP BY 1, 2 ORDER BY 3");

    try {
      StatementType type = new StatementType(StatementType.Type.SCAN, table + "_SOE_Report2", -1, null, null, fields);
      PreparedStatement scanStatement = cachedStatements.get(type);
      if (scanStatement == null)
        scanStatement = createAndCacheStatement(type, null, builder.toString(), this.jdbcFetchSize);

      scanStatement.setString(1, valueOrderMonth);
      scanStatement.setString(2, valueAddressZip);

      executeScan(scanStatement, fields, result, -1);

      if (result.size() <= 0)
        return Status.NOT_FOUND;

      return Status.OK;
    } catch (SQLException e) {
      System.err.println("Error in processing scan of table: " + table + e);
      return Status.ERROR;
    }
/*
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
  */
  }
}
