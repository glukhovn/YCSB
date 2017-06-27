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

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Enumeration;
import java.util.Properties;

/**
 * Utility class to create the table to be used by the benchmark.
 * 
 * @author sudipto
 *
 */
public class JdbcDBCreateTable implements JdbcDBClientConstants {

  private static void usageMessage() {
    System.out.println("Create Table Client. Options:");
    System.out.println("  -p   key=value properties defined.");
    System.out.println("  -P   location of the properties file to load.");
    System.out.println("  -n   name of the table.");
    System.out.println("  -f   number of fields (default 10).");
  }
  
  private static void createTable(Properties props, String tablename)
  throws SQLException {
    String driver = props.getProperty(DRIVER_CLASS);
    String username = props.getProperty(CONNECTION_USER);
    String password = props.getProperty(CONNECTION_PASSWD, "");
    String url = props.getProperty(CONNECTION_URL);
    int fieldcount = Integer.parseInt(props.getProperty(FIELD_COUNT_PROPERTY, 
        FIELD_COUNT_PROPERTY_DEFAULT));
    boolean pk_column = Boolean.parseBoolean(props.getProperty(PK_COLUMN, "false"));
    boolean jsonbPathOps = Boolean.parseBoolean(props.getProperty("jsonb_path_ops", "false"));
    boolean sqlJson = Boolean.parseBoolean(props.getProperty("sql_json", "false"));
    boolean ginFastUpdate = Boolean.parseBoolean(props.getProperty("gin_fast_update", "false"));
    boolean jsonbc = Boolean.parseBoolean(props.getProperty("jsonbc", "false"));
    boolean pglz = Boolean.parseBoolean(props.getProperty("pglz", "true"));
    int partitions = Integer.parseInt(props.getProperty(PARTITION_COUNT_PROPERTY, "0"));

    if (driver == null || username == null || url == null) {
      throw new SQLException("Missing connection information.");
    }
    
    Connection conn = null;
    
    try {
      Class.forName(driver);
      
      conn = DriverManager.getConnection(url, username, password);
      Statement stmt = conn.createStatement();
      
      StringBuilder sql = new StringBuilder("DROP TABLE IF EXISTS ");
      sql.append(tablename);
      sql.append(" CASCADE;");
      
      stmt.execute(sql.toString());

      String pk_expr =
        pk_column ? PRIMARY_KEY :
        jsonbPathOps ? null :
        sqlJson ? "(JSON_VALUE(data, '$.'" + PRIMARY_KEY + "' RETURNING text))"
                : "(DATA->>'" + PRIMARY_KEY + "')";

      sql = new StringBuilder("CREATE TABLE ");
      sql.append(tablename);
      sql.append(" (");
      if (pk_column)
        sql.append(PRIMARY_KEY).append(" text").append(partitions > 0 ? ", ": " PRIMARY KEY, ");
      sql.append("DATA jsonb" + (jsonbc ? " COMPRESSED jsonbc" : "") + ")");
      if (partitions > 0 && pk_expr != null)
        sql.append(" PARTITION BY HASH (").append(pk_expr).append(")");
      sql.append(";");

      stmt.execute(sql.toString());

      if (!pglz)
        stmt.execute("ALTER TABLE " + tablename + " ALTER data SET STORAGE EXTERNAL;");

      if (partitions > 0 && pk_expr != null) {
        for (int i = 0; i < partitions; i++) {
          String partname = tablename + "_" + i;

          stmt.execute(
            "CREATE TABLE " + partname +
            " PARTITION OF " + tablename +
            " FOR VALUES WITH (modulus " + partitions + ", remainder " + i + ");"
          );

          stmt.execute(
            "CREATE UNIQUE INDEX ON " + partname + "(" + pk_expr + ");"
          );
        }
      }

      if (!pk_column && partitions <= 0)
        stmt.execute(jsonbPathOps ? "CREATE INDEX ON " + tablename +
                                    " USING gin(DATA jsonb_path_ops)" +
                                     "WITH (fastupdate =" + (ginFastUpdate ? "ON" : "OFF") + ")"
                                  : "CREATE UNIQUE INDEX ON " + tablename + "(" + pk_expr + ")");

      System.out.println("Table " + tablename + " created.");
    } catch (ClassNotFoundException e) {
      throw new SQLException("JDBC Driver class not found.");
    } finally {
      if (conn != null) {
        System.out.println("Closing database connection.");
        conn.close();
      }
    }
  }
  
  /**
   * @param args
   */
  public static void main(String[] args) {
    
    if (args.length == 0) {
      usageMessage();
      System.exit(0);
    }
    
    String tablename = null;
    int fieldcount = -1;
    Properties props = new Properties();
    Properties fileprops = new Properties();

    // parse arguments
    int argindex = 0;
    while (args[argindex].startsWith("-")) {
      if (args[argindex].compareTo("-P") == 0) {
        argindex++;
        if (argindex >= args.length) {
          usageMessage();
          System.exit(0);
        }
        String propfile = args[argindex];
        argindex++;

        Properties myfileprops = new Properties();
        try {
          myfileprops.load(new FileInputStream(propfile));
        } catch (IOException e) {
          System.out.println(e.getMessage());
          System.exit(0);
        }

        // Issue #5 - remove call to stringPropertyNames to make compilable
        // under Java 1.5
        for (Enumeration<?> e = myfileprops.propertyNames(); e
            .hasMoreElements();) {
          String prop = (String) e.nextElement();

          fileprops.setProperty(prop, myfileprops.getProperty(prop));
        }

      } else if (args[argindex].compareTo("-p") == 0) {
        argindex++;
        if (argindex >= args.length) {
          usageMessage();
          System.exit(0);
        }
        int eq = args[argindex].indexOf('=');
        if (eq < 0) {
          usageMessage();
          System.exit(0);
        }

        String name = args[argindex].substring(0, eq);
        String value = args[argindex].substring(eq + 1);
        props.put(name, value);
        argindex++;
      } else if (args[argindex].compareTo("-n") == 0) {
        argindex++;
        if (argindex >= args.length) {
          usageMessage();
          System.exit(0);
        }
        tablename = args[argindex++];
      } else if (args[argindex].compareTo("-f") == 0) {
        argindex++;
        if (argindex >= args.length) {
          usageMessage();
          System.exit(0);
        }
        try {
          fieldcount = Integer.parseInt(args[argindex++]);
        } catch (NumberFormatException e) {
          System.err.println("Invalid number for field count");
          usageMessage();
          System.exit(1);
        }
      } else {
        System.out.println("Unknown option " + args[argindex]);
        usageMessage();
        System.exit(0);
      }

      if (argindex >= args.length) {
        break;
      }
    }

    if (argindex != args.length) {
      usageMessage();
      System.exit(0);
    }

    // overwrite file properties with properties from the command line

    // Issue #5 - remove call to stringPropertyNames to make compilable under
    // Java 1.5
    for (Enumeration<?> e = props.propertyNames(); e.hasMoreElements();) {
      String prop = (String) e.nextElement();

      fileprops.setProperty(prop, props.getProperty(prop));
    }

    props = fileprops;
    
    if (tablename == null)
      tablename = props.getProperty("table");

    if (tablename == null) {
      System.err.println("table name missing.");
      usageMessage();
      System.exit(1);
    }

    if (fieldcount < 0) {
      String prop = props.getProperty("fieldcount");
      if (prop != null)
        try {
          fieldcount = Integer.parseInt(prop);
        } catch (NumberFormatException e) { }
	}
    
    if (fieldcount > 0) {
      props.setProperty(FIELD_COUNT_PROPERTY, String.valueOf(fieldcount));
    }
    
    try {
      createTable(props, tablename);
    } catch (SQLException e) {
      System.err.println("Error in creating table. " + e);
      System.exit(1);
    }
  }
}
