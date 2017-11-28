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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;

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
public class JdbcDBMysqlJsonClient extends JdbcJsonClient {

  @Override
  protected String buildConditionKey(String key) {
    return key; //String.format("\"%s\"", key);
  }

  @Override
  public String createLockStatement(StatementType lockType, String key) {
    throw new RuntimeException("advisory locks are not supported");
  }

  @Override
  protected String createInsertStatement(StatementType insertType) {
    return new StringBuilder("INSERT INTO ").append(insertType.tableName)
      .append("(data").append(pk_column ? ", " + PRIMARY_KEY : "").append(")")
      .append(" VALUES (?").append(pk_column ? ", ?" : "").append(")")
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
          read.append("data->>'$.").append(field).append("' AS ").append(field);
        }
      }
    }

    if (select_one_field)
      read.append("data->>'$.").append(select_field_path).append("')");

    read.append(" FROM ").append(type.tableName);

    return read;
  }

  private StringBuilder appendWhereClause(StringBuilder builder) {
    builder.append(" WHERE ").append(PRIMARY_KEY).append(" = ?");
    /*
    builder.append(" WHERE data->>'$");

    for (int i = 1; i < nesting_key_depth; i++)
      builder.append('.').append(PRIMARY_KEY).append(i);

    builder.append('.').append(PRIMARY_KEY).append("' = ?");
*/
    return builder;
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
    if (update_one_field)
      return Collections.singletonList(values.values().iterator().next().toString());

    assert(update_all_fields);

    if (values.size() == -1) // TODO full row update
      return Collections.singletonList(buildUpdateValue(values));

    ArrayList<String> vals = new ArrayList<>(values.size());

    for (ByteIterator val : values.values())
      vals.add(val.toString());

    return vals;
  }

  @Override
  protected String createUpdateStatement(StatementType updateType) {
    StringBuilder update = new StringBuilder("UPDATE ")
        .append(updateType.tableName).append(" SET data = ");

    if (update_one_field)
      update.append("json_set(data, '$.").append(update_field).append("', ?)");
    else {
      assert(update_all_fields);

      if (updateType.fields.size() == -1) // TODO full row update
        update.append("?");
      else {
        update.append("json_set(data");
        for (String field : updateType.fields)
          update.append(", '$.").append(field).append("', ?");
        update.append(")");
      }
    }

    return appendWhereClause(update).toString();
  }

  @Override
  protected String createScanStatement(StatementType scanType) {
    return createSelectStatement(scanType)
        //.append(" WHERE data->>'$.").append(PRIMARY_KEY).append("' >= ?")
        //.append(" ORDER BY data->>'$.").append(PRIMARY_KEY).append("' LIMIT ?")
        .append(" WHERE ").append(PRIMARY_KEY).append(" >= ?")
        .append(" ORDER BY ").append(PRIMARY_KEY).append(" LIMIT ?")
        .toString();
  }
}
