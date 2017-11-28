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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

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
public class JdbcDBPGJsonbClient extends JdbcJsonClient {

  private boolean initialized = false;

  private static boolean jsonb_path_ops;
  private static boolean jsonb_path_ops_no_parse;
  private static boolean field_index;
  private static boolean sql_json;
  private static boolean select_for_update;
   
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

    Properties props = getProperties();

    jsonb_path_ops = Boolean.parseBoolean(props.getProperty("jsonb_path_ops", "false"));
    jsonb_path_ops_no_parse = Boolean.parseBoolean(props.getProperty("jsonb_path_ops_no_parse", "true"));
    field_index = Boolean.parseBoolean(props.getProperty("field_index", "true"));
    sql_json = Boolean.parseBoolean(props.getProperty("sql_json", "false"));
    select_for_update = Boolean.parseBoolean(props.getProperty("select_for_update", "false"));

    super.init();

    initialized = true;
  }

  @Override
  protected String buildConditionKey(String key) {
    if (pk_column || field_index || (jsonb_path_ops && jsonb_path_ops_no_parse))
      return key;

    StringBuilder condition = new StringBuilder("{");
    appendJsonKey(condition, key);
    condition.append('}');

    return condition.toString();
  }

  private StringBuilder appendWhereClause(StringBuilder builder, String alias, String param) {
    if (jsonb_path_ops) {
      builder.append(" WHERE ").append(alias).append("data @> ");
      if (jsonb_path_ops_no_parse)
        builder.append("jsonb_build_object('").append(PRIMARY_KEY).append("', ").append(param).append(")");
      else
        builder.append(param).append("::jsonb");
    } else if (field_index) {
      if (pk_column)
        builder.append(" WHERE ").append(alias).append(PRIMARY_KEY).append(" = ").append(param);
      else if (sql_json) {
        builder.append(" WHERE JSON_VALUE(").append(alias).append("data, '$");

        for (int i = 1; i < nesting_key_depth; i++)
          builder.append(".").append(PRIMARY_KEY).append(i);

        builder.append(".").append(PRIMARY_KEY).append("' RETURNING text) = ").append(param);
      } else {
        builder.append(" WHERE ").append(alias).append("data->>");

        for (int i = 1; i < nesting_key_depth; i++)
          builder.append("'").append(PRIMARY_KEY).append(i).append("'->>");

        builder.append("'" + PRIMARY_KEY + "'").append(" = ").append(param);
      }
    }

    return builder;
  }

  private StringBuilder appendWhereClause(StringBuilder builder) {
    return appendWhereClause(builder, "", "?");
  }


  @Override
  protected String createInsertStatement(StatementType insertType) {
    return new StringBuilder("INSERT INTO ")
      .append(insertType.tableName)
      .append("(data").append(pk_column ? ", " + PRIMARY_KEY : "").append(")")
      .append(" VALUES (?::jsonb").append(pk_column ? ", ?::text" : "").append(")")
      .toString();
  }

  private StringBuilder createSelectStatement(StatementType type) {
    StringBuilder read = new StringBuilder("SELECT ");

    if (select_all_fields) {
      if (type.fields == null)
        read.append("data");
      else {
        int index = 0;
        for (String field : type.fields) {
          if (index++ > 0)
            read.append(", ");
          appendKeyField(read, field).append(" AS \"").append(field).append('"');
        }
      }
    }

    if (select_one_field)
      if (sql_json)
        read.append("JSON_VALUE(data, '$.").append(select_field_path).append("')");
      else
        read.append("data->>").append(select_field_path);

    read.append(" FROM ").append(type.tableName);

    return read;
  }

  @Override
  protected String createReadStatement(StatementType readType) {
    return appendWhereClause(createSelectStatement(readType)).toString();
  }

  @Override
  protected String createDeleteStatement(StatementType deleteType) {
    return appendWhereClause(new StringBuilder("DELETE FROM ").append(deleteType.tableName)).toString();
  }

  @Override
  protected List<String> buildUpdateValues(HashMap<String, ByteIterator> values) {
    return Collections.singletonList(buildUpdateValue(values));
  }

  @Override
  protected String createUpdateStatement(StatementType updateType) {
    if (select_for_update)
    {
      String sql = appendWhereClause(
       appendWhereClause(
       appendKeyField(
        new StringBuilder("UPDATE ").append(updateType.tableName)
          .append(" SET data = data || ?::jsonb FROM (SELECT "), PRIMARY_KEY).append(" id FROM ").append(updateType.tableName)).append(" FOR NO KEY UPDATE) q "), "", "q.id").toString();
      System.err.println(sql);
      return sql;
    }

    return appendWhereClause(
        new StringBuilder("UPDATE ").append(updateType.tableName)
          .append(" SET data = data || ?::jsonb")).toString();
  }

  private StringBuilder appendKeyField(StringBuilder builder, String key) {
    if (sql_json)
      return builder.append("JSON_VALUE(data, '$.").append(key).append("' RETURNING text)");
    else
      return builder.append("data->>'").append(key).append("'");
  }

  @Override
  protected String createScanStatement(StatementType scanType) {
    String key = pk_column ? PRIMARY_KEY
                           : appendKeyField(new StringBuilder(), PRIMARY_KEY).toString();

    return createSelectStatement(scanType)
          .append(" WHERE ").append(key).append(" >= ?")
          .append(" ORDER BY ").append(key)
          .append(" LIMIT ?")
          .toString();
  }

  @Override
  protected String buildLockKey(String key) {
    return new Integer(key.hashCode()).toString();
  }

  @Override
  protected String createLockStatement(StatementType lockType) {
    String fn;
    switch (lockType.type) {
      case LOCK_SHARED:
        fn = "pg_advisory_lock_shared";
        break;
      case UNLOCK_SHARED:
        fn = "pg_advisory_unlock_shared";
        break;
      case LOCK_EXCLUSIVE:
        fn = "pg_advisory_lock";
        break;
      case UNLOCK_EXCLUSIVE:
        fn = "pg_advisory_unlock";
        break;
      default:
        throw new IllegalArgumentException("invalid advisory lock type");
    }
    return (new StringBuilder()).append("SELECT ").append(fn).append("(?::bigint)").toString();
  }
}
