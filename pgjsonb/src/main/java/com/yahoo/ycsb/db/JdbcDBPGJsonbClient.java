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

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;

import org.apache.commons.lang.StringEscapeUtils;
import org.json.JSONObject;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.lang.System;

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
public class JdbcDBPGJsonbClient extends DB implements JdbcDBClientConstants {
	
  private ArrayList<Connection> conns;
  private boolean initialized = false;
  private Properties props;

  private boolean flat_key;
  private boolean nested_key;
  private int nesting_key_depth;
  private int document_depth;
  private int document_width;
  private int element_values;
  private int element_obj;

  private Integer jdbcFetchSize;
  private static final String DEFAULT_PROP = "";
  private ConcurrentMap<StatementType, PreparedStatement> cachedStatements;
  
  /**
   * The statement type for the prepared statements.
   */
  private static class StatementType {
    
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
    
    StatementType(Type type, String tableName, int numFields, int _shardIndex) {
      this.type = type;
      this.tableName = tableName;
      this.numFields = numFields;
      this.shardIndex = _shardIndex;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + numFields + 100 * shardIndex;
      result = prime * result
          + ((tableName == null) ? 0 : tableName.hashCode());
      result = prime * result + ((type == null) ? 0 : type.getHashCode());
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
       //System.out.println(conns.size() + ": Shard instance for "+ key + " (hash  " + key.hashCode()+ " ) " + " is " + ret);
       return ret;
    }

    /**
     * For the given key, returns Connection object that holds connection
     * to the shard that contains this key
     *
     * @param key Data key to get information for
     * @return Connection object
     */
    private Connection getShardConnectionByKey(String key) {
        return conns.get(getShardIndexByKey(key));
    }

    private void cleanupAllConnections() throws SQLException {
       for(Connection conn: conns) {
           conn.close();
       }
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

		flat_key = Boolean.parseBoolean(props.getProperty(FLAT_KEY, "true"));
		nested_key = Boolean.parseBoolean(props.getProperty(NESTED_KEY, "false"));
		nesting_key_depth = Integer.parseInt(props.getProperty("depth", "10"));
 		document_depth = Integer.parseInt(props.getProperty("document_depth", "3"));
 		document_width = Integer.parseInt(props.getProperty("document_width", "4"));
 		element_values = Integer.parseInt(props.getProperty("element_values", "2"));
 		element_obj = Integer.parseInt(props.getProperty("element_obj", "2"));

      String jdbcFetchSizeStr = props.getProperty(JDBC_FETCH_SIZE);
          if (jdbcFetchSizeStr != null) {
          try {
              this.jdbcFetchSize = Integer.parseInt(jdbcFetchSizeStr);
          } catch (NumberFormatException nfe) {
              System.err.println("Invalid JDBC fetch size specified: " + jdbcFetchSizeStr);
              throw new DBException(nfe);
          }
      }

      String autoCommitStr = props.getProperty(JDBC_AUTO_COMMIT, Boolean.TRUE.toString());
      Boolean autoCommit = Boolean.parseBoolean(autoCommitStr);

      try {
		  if (driver != null) {
	      Class.forName(driver);
	    }
          int shardCount = 0;
          conns = new ArrayList<Connection>(3);
          for (String url: urls.split(",")) {
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

          System.out.println("Using " + shardCount + " shards");

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
	  try {
      cleanupAllConnections();
    } catch (SQLException e) {
      System.err.println("Error in closing the connection. " + e);
      throw new DBException(e);
    }
	}
	
	private PreparedStatement createAndCacheInsertStatement(StatementType insertType, String key)
	throws SQLException {
	  StringBuilder insert = new StringBuilder("INSERT INTO ");
	  insert.append(insertType.tableName);
	  insert.append("(data) VALUES(?::jsonb)");
    PreparedStatement insertStatement = getShardConnectionByKey(key).prepareStatement(insert.toString());
    PreparedStatement stmt = cachedStatements.putIfAbsent(insertType, insertStatement);
    if (stmt == null) return insertStatement;
    else return stmt;
	}
	
	private PreparedStatement createAndCacheReadStatement(StatementType readType, String key)
	throws SQLException {
    StringBuilder read = new StringBuilder("SELECT data FROM ");
    read.append(readType.tableName);

    if (jsonb_path_ops) {
      read.append(" WHERE data @> ?::jsonb");
    }

    if (field_index && flat_key) {
      read.append(" WHERE data->>\"");
      read.append(PRIMARY_KEY);
      read.append("\" = ?");
    }

    if (field_index && nested_key) {
      read.append(" WHERE data->>\"");
      for (int i = 1; i < nesting_key_depth - 1; i++) {
        read.append("\"" + PRIMARY_KEY + i + "\"->>");
      }
      read.append("\"" + PRIMARY_KEY + "\"");
      read.append(" = ?");
    }


    PreparedStatement readStatement = getShardConnectionByKey(key).prepareStatement(read.toString());
    PreparedStatement stmt = cachedStatements.putIfAbsent(readType, readStatement);
    if (stmt == null) return readStatement;
    else return stmt;
  }
	
	private PreparedStatement createAndCacheDeleteStatement(StatementType deleteType, String key)
	throws SQLException {
    StringBuilder delete = new StringBuilder("DELETE FROM ");
    delete.append(deleteType.tableName);
    delete.append(" WHERE data @> ?::jsonb");
    PreparedStatement deleteStatement = getShardConnectionByKey(key).prepareStatement(delete.toString());
    PreparedStatement stmt = cachedStatements.putIfAbsent(deleteType, deleteStatement);
    if (stmt == null) return deleteStatement;
    else return stmt;
  }
	
	private PreparedStatement createAndCacheUpdateStatement(StatementType updateType, String key)
	throws SQLException {
    StringBuilder update = new StringBuilder("UPDATE ");
    update.append(updateType.tableName);
    update.append(" SET data = data || ?::jsonb");
    update.append(" WHERE data @> ?::jsonb");
    PreparedStatement insertStatement = getShardConnectionByKey(key).prepareStatement(update.toString());
    PreparedStatement stmt = cachedStatements.putIfAbsent(updateType, insertStatement);
    if (stmt == null) return insertStatement;
    else return stmt;
  }
	
	private PreparedStatement createAndCacheScanStatement(StatementType scanType, String key)
	throws SQLException {
	  StringBuilder select = new StringBuilder("SELECT * FROM ");
    select.append(scanType.tableName);
    select.append(" WHERE data->>'");
    select.append(PRIMARY_KEY);
    select.append("' >= ?");
    select.append(" ORDER BY ");
    select.append(PRIMARY_KEY);
    select.append(" LIMIT ?");
    PreparedStatement scanStatement = getShardConnectionByKey(key).prepareStatement(select.toString());
    if (this.jdbcFetchSize != null) scanStatement.setFetchSize(this.jdbcFetchSize);
    PreparedStatement stmt = cachedStatements.putIfAbsent(scanType, scanStatement);
    if (stmt == null) return scanStatement;
    else return stmt;
  }

	@Override
	public Status read(String tableName, String key, Set<String> fields,
			HashMap<String, ByteIterator> result) {
    try {
      StatementType type = new StatementType(StatementType.Type.READ, tableName, 1, getShardIndexByKey(key));
      PreparedStatement readStatement = cachedStatements.get(type);
      if (readStatement == null) {
        readStatement = createAndCacheReadStatement(type, key);
      }

      StringBuilder readCondition = new StringBuilder("{");

      if (flat_key && !field_index) {
          readCondition.append("\"");
          readCondition.append(PRIMARY_KEY);
          readCondition.append("\": \"");
          readCondition.append(key);
          readCondition.append("\"}");

          readStatement.setString(1, readCondition.toString());
      }

      if (nested_key && !field_index) {
        for (int i = 1; i < nesting_key_depth - 1; i++) {
            readCondition.append(String.format("\"%s%d\": {", PRIMARY_KEY, i));
        }
        readCondition.append(String.format("\"%s\": \"%s\"", PRIMARY_KEY, key));
        readCondition.append(new String(new char[nesting_key_depth - 1]).replace("\0", "}"));

        readStatement.setString(1, readCondition.toString());
      }

      if (field_index) {
        readStatement.setString(1, key);
      }

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
      StatementType type = new StatementType(StatementType.Type.SCAN, tableName, 1, getShardIndexByKey(startKey));
      PreparedStatement scanStatement = cachedStatements.get(type);
      if (scanStatement == null) {
        scanStatement = createAndCacheScanStatement(type, startKey);
      }
      scanStatement.setString(1, startKey);
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
      StatementType type = new StatementType(StatementType.Type.UPDATE, tableName, numFields, getShardIndexByKey(key));
      PreparedStatement updateStatement = cachedStatements.get(type);
      if (updateStatement == null) {
        updateStatement = createAndCacheUpdateStatement(type, key);
      }
	  StringBuilder update_jsonb = new StringBuilder("{");
      int index = 1;
      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        if (index > 1)
        {
            update_jsonb.append(", ");
        }
        update_jsonb.append("\"");
        update_jsonb.append(COLUMN_PREFIX);
        update_jsonb.append(index++);
        update_jsonb.append("\": \"");
        update_jsonb.append(StringEscapeUtils.escapeJava(entry.getValue().toString()));
        update_jsonb.append("\"");
      }
      update_jsonb.append("}");

      StringBuilder updateCondition = new StringBuilder("{\"");
      updateCondition.append(PRIMARY_KEY);
      updateCondition.append("\": \"");
      updateCondition.append(key);
      updateCondition.append("\"}");

      updateStatement.setString(1, update_jsonb.toString());
      updateStatement.setString(2, updateCondition.toString());
      int result = updateStatement.executeUpdate();
      if (result == 1 || result == 2) return Status.OK;
      else return Status.UNEXPECTED_STATE;
    } catch (SQLException e) {
      System.err.println("Error in processing update to table: " + tableName + e);
      return Status.ERROR;
    }
	}

	@Override
	public Status insert(String tableName, String key, HashMap<String, ByteIterator> values) {
	  try {
	    int numFields = values.size();
	    StatementType type = new StatementType(StatementType.Type.INSERT, tableName, numFields, getShardIndexByKey(key));
	    PreparedStatement insertStatement = cachedStatements.get(type);
	    if (insertStatement == null) {
	      insertStatement = createAndCacheInsertStatement(type, key);
	    }
	  StringBuilder insert_jsonb = new StringBuilder("{");

      if (flat_key) {
        insert_jsonb.append(String.format("\"%s\": \"%s\"", PRIMARY_KEY, key));
      }

      if (nested_key) {
        for (int i = 1; i < nesting_key_depth - 1; i++) {
            insert_jsonb.append(String.format("\"%s%d\": {", PRIMARY_KEY, i));
        }
        insert_jsonb.append(String.format("\"%s\" : \"%s\"", PRIMARY_KEY, key));
        insert_jsonb.append(new String(new char[nesting_key_depth - 2]).replace("\0", "}"));
      }

      int depth = 0;
      int index = 2;

      if (document_depth == 0) {
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
          String field = entry.getValue().toString();
          insert_jsonb.append(String.format(", \"%s%d\": \"%s\"", COLUMN_PREFIX, index++, StringEscapeUtils.escapeJava(field)));
        }
      }
      else {
        ArrayList<JSONObject> obj_keys = new ArrayList<JSONObject>();
        ArrayList<JSONObject> top_keys = obj_keys;
        ArrayList<JSONObject> current_keys;
        LinkedList<ByteIterator> val_list = new LinkedList<ByteIterator>(values.values());
        for(int i = 0; i < document_width; i++) {
          obj_keys.add(new JSONObject());
        }

        while (depth < document_depth) {
          current_keys = new ArrayList<JSONObject>();
          for(JSONObject obj : obj_keys) {

            // put values
            for(int i = 0; i < element_values; i++) {
              obj.put(String.format("%s%d", COLUMN_PREFIX, index), val_list.pop());
              index++;
            }

            if (depth < document_depth - 1) {
              // put objects
              for(int i = 0; i < element_obj; i++) {
                JSONObject child = new JSONObject();
                obj.put(String.format("%s%d", COLUMN_PREFIX, index), child);
                current_keys.add(child);
                index++;
              }
            }
            else {
              // put values
              for(int i = 0; i < element_obj; i++) {
                obj.put(String.format("%s%d", COLUMN_PREFIX, index), val_list.pop());
                index++;
              }
            }
          }

          obj_keys = current_keys;
          depth++;
        }

        for(JSONObject obj : top_keys) {
          insert_jsonb.append(String.format(", \"%s%d\": %s", COLUMN_PREFIX, index++, obj.toString()));
        }
      }

      insert_jsonb.append("}");
      insertStatement.setString(1, insert_jsonb.toString());
      int result = insertStatement.executeUpdate();
      if (result == 1) return Status.OK;
      else return Status.UNEXPECTED_STATE;
    } catch (SQLException e) {
      System.err.println("Error in processing insert to table: " + tableName + e);
      return Status.ERROR;
    }
	}

	@Override
	public Status delete(String tableName, String key) {
    try {
      StatementType type = new StatementType(StatementType.Type.DELETE, tableName, 1, getShardIndexByKey(key));
      PreparedStatement deleteStatement = cachedStatements.get(type);
      if (deleteStatement == null) {
        deleteStatement = createAndCacheDeleteStatement(type, key);
      }
      StringBuilder deleteCondition = new StringBuilder("{\"");
      deleteCondition.append(PRIMARY_KEY);
      deleteCondition.append("\": \"");
      deleteCondition.append(key);
      deleteCondition.append("\"}");

      deleteStatement.setString(1, deleteCondition.toString());
      int result = deleteStatement.executeUpdate();
      if (result == 1) return Status.OK;
      else return Status.UNEXPECTED_STATE;
    } catch (SQLException e) {
      System.err.println("Error in processing delete to table: " + tableName + e);
      return Status.ERROR;
    }
	}
}
