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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang.StringEscapeUtils;
import org.json.JSONObject;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;

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
  private static final String DEFAULT_PROP = "";
  private ConcurrentMap<StatementType, PreparedStatement> cachedStatements;
  private long numRowsInBatch = 0;

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

    StatementType(Type type, String tableName, int numFields, int _shardIndex, Collection<String> fields) {
      this.type = type;
      this.tableName = tableName;
      this.numFields = numFields;
      this.shardIndex = _shardIndex;
      this.fields = fields == null ? null : new ArrayList<String>(fields);
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + numFields + 100 * shardIndex;
      result = prime * result
          + ((tableName == null) ? 0 : tableName.hashCode());
      result = prime * result + ((type == null) ? 0 : type.getHashCode());
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
      if (numFields != other.numFields)
        return false;
      if (shardIndex != other.shardIndex)
        return false;
      if (tableName == null) {
        if (other.tableName != null)
          return false;
      } else if (!tableName.equals(other.tableName))
        return false;
      if (type != other.type)
        return false;
      if (this.fields == null ? other.fields == null
                              : other.fields != null && this.fields.equals(other.fields));
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

  @Override
  public Status read(String tableName, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
    try {
      StatementType type = new StatementType(StatementType.Type.READ, tableName, 1, getShardIndexByKey(key), fields);
      PreparedStatement readStatement = cachedStatements.get(type);
      if (readStatement == null)
        readStatement = createAndCacheStatement(type, key, createReadStatement(type));

      readStatement.setString(1, buildConditionKey(key));

      ResultSet resultSet = readStatement.executeQuery();

      if (!resultSet.next()) {
        resultSet.close();
        return Status.NOT_FOUND;
      }

      if (result != null && fields != null) {
        for (String field : fields) {
          String value = resultSet.getString(field);
          result.put(field, new StringByteIterator(value));
        }
      }

      resultSet.close();

      return Status.OK;
    } catch (SQLException e) {
        System.err.println("Error in processing read of table " + tableName + ": "+e);
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String tableName, String startKey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    try {
      StatementType type = new StatementType(StatementType.Type.SCAN, tableName, 1, getShardIndexByKey(startKey), fields);
      PreparedStatement scanStatement = cachedStatements.get(type);
      if (scanStatement == null)
        scanStatement = createAndCacheStatement(type, startKey, createScanStatement(type), this.jdbcFetchSize);

      scanStatement.setString(1, buildScanKey(startKey));
      scanStatement.setInt(2, recordcount);

      ResultSet resultSet = scanStatement.executeQuery();

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

      return Status.OK;
    } catch (SQLException e) {
      System.err.println("Error in processing scan of table: " + tableName + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status update(String tableName, String key, HashMap<String, ByteIterator> values) {
    try {
      int numFields = values.size();
      StatementType type = new StatementType(StatementType.Type.UPDATE, tableName, numFields, getShardIndexByKey(key), values.keySet());
      PreparedStatement updateStatement = cachedStatements.get(type);
      if (updateStatement == null)
        updateStatement = createAndCacheStatement(type, key, createUpdateStatement(type));

      int index = 1;

      for (String val : buildUpdateValues(values))
        updateStatement.setString(index++, val);

      updateStatement.setString(index, buildConditionKey(key));

      int result = updateStatement.executeUpdate();
      return result == 1 || result == 2 ? Status.OK : Status.UNEXPECTED_STATE;
    } catch (SQLException e) {
      System.err.println("Error in processing update to table: " + tableName + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String tableName, String key, HashMap<String, ByteIterator> values) {
    try {
      int numFields = values.size();
      StatementType type = new StatementType(StatementType.Type.INSERT, tableName, numFields, getShardIndexByKey(key), values.keySet());
      PreparedStatement insertStatement = cachedStatements.get(type);
      if (insertStatement == null)
        insertStatement = createAndCacheStatement(type, key, createInsertStatement(type));

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
      StatementType type = new StatementType(StatementType.Type.DELETE, tableName, 1, getShardIndexByKey(key), null);
      PreparedStatement deleteStatement = cachedStatements.get(type);
      if (deleteStatement == null)
        deleteStatement = createAndCacheStatement(type, key, createDeleteStatement(type));

      deleteStatement.setString(1, buildConditionKey(key));

      int result = deleteStatement.executeUpdate();
      return result == 1 ? Status.OK : Status.UNEXPECTED_STATE;
    } catch (SQLException e) {
      System.err.println("Error in processing delete to table: " + tableName + e);
      return Status.ERROR;
    }
  }
}
