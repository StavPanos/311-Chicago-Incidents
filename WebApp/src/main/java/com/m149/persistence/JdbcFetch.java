package com.m149.persistence;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author stavropoulosp
 */
public class JdbcFetch {

  private static final String URL = "jdbc:postgresql://localhost:5432/postgres";
  private static final String USERNAME = "postgres";
  private static final String PASSWORD = "postgres";

  private static Map<Class, Map> classesMethods = null;

  public JdbcFetch() {
  }

  public Connection getConnection() {
    try {
      Connection connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);

      if (connection != null) {
        System.out.println("Connected to the database!");
      } else {
        System.out.println("Failed to make connection!");
      }

      connection.setAutoCommit(false);

      return connection;

    } catch (RuntimeException ex) {
      Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, ex.getLocalizedMessage(), ex);
      throw ex;
    } catch (Exception ex) {
      Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, ex.getLocalizedMessage(), ex);
      throw new RuntimeException(ex);
    }
  }

  public void closeConnection(Connection connection) {
    try {
      connection.close();
      connection = null;
    } catch (Exception ex) {
      Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, ex.getLocalizedMessage(), ex);
    }
  }

  public List<Map> select(String sql) {
    return select(null, sql, null);
  }

  public List<Map> select(String sql, Object[] parametersValues) {
    return select(null, sql, parametersValues);
  }

  public List<Map> select(String sql, Object[] parametersValues, int limitStart, int limitRows) {
    return select(null, sql, parametersValues, limitStart, limitRows);
  }

  public List<Map> select(Connection connection, String sql) {
    return select(connection, sql, null, -1, -1);
  }

  public List<Map> select(Connection connection, String sql, Object[] parametersValues) {
    return select(connection, sql, parametersValues, -1, -1);
  }

  public List<Map> select(Connection connection, String sql, Object[] parametersValues, int limitStart, int limitRows) {
    return select(connection, sql, parametersValues, limitStart, limitRows, false, null, null);
  }

  public Object[] selectWithMetadata(String sql, Object[] parametersValues) {
    return selectWithMetadata(null, sql, parametersValues);
  }

  public Object[] selectWithMetadata(Connection connection, String sql, Object[] parametersValues) {
    Map metadataMap = new HashMap();
    List metadataList = new ArrayList();

    List list = select(connection, sql, parametersValues, -1, -1, true, metadataMap, metadataList);

    Object[] result = new Object[3];
    result[0] = list;
    result[1] = metadataMap;
    result[2] = metadataList;

    return result;
  }

  private List<Map> select(Connection connection, String sql, Object[] parametersValues, int limitStart, int limitRows, boolean extractMetadata, Map metadataMap, List metadataList) {
    try {
      boolean newConnection = connection == null;
      if (newConnection) {
        connection = getConnection();
      }
      try {
        if (limitStart > 0 || limitRows > 0) {
          String _sql;
          if (limitStart > 0) {
            _sql = "SELECT * FROM ( SELECT * FROM ( SELECT ROW_.*, ROW_NUMBER() OVER () ROWNUM_ FROM ( ";
          } else {
            _sql = "SELECT * FROM ( SELECT ROW_.*, ROW_NUMBER() OVER () ROWNUM_ FROM ( ";
          }

          _sql += sql;

          if (limitStart > 0) {
            _sql += " ) ROW_ ) a WHERE ROWNUM_ <= " + (limitStart + (limitRows > 0 ? limitRows : 1)) + " ORDER BY ROWNUM_ ) b WHERE ROWNUM_ > " + limitStart + " ORDER BY ROWNUM_";
          } else {
            _sql += " ) ROW_ ) a WHERE ROWNUM_ <= " + limitRows + " ORDER BY ROWNUM_";
          }
          sql = _sql;
        }

        PreparedStatement preparedStatement = connection.prepareStatement(sql);

        try {
          if (parametersValues != null) {
            for (int i = 0; i < parametersValues.length; i++) {
              Object parameter = parametersValues[i];
              if (parameter == null) {
                preparedStatement.setNull(i + 1, 12);
              } else if (parameter instanceof String) {
                preparedStatement.setString(i + 1, (String) parameter);
              } else if (parameter instanceof Number) {
                preparedStatement.setBigDecimal(i + 1, new BigDecimal(((Number) parameter).doubleValue()));
              } else if (parameter instanceof Boolean) {
                preparedStatement.setBoolean(i + 1, ((Boolean) parameter).booleanValue());
              } else if (parameter instanceof Date) {
                preparedStatement.setTimestamp(i + 1, new Timestamp(((Date) parameter).getTime()));
              } else {
                preparedStatement.setObject(i + 1, parameter);
              }
            }
          }

          ResultSet resultSet = preparedStatement.executeQuery();
          try {
            if (extractMetadata) {
              _metadataResultSet(resultSet, metadataMap, metadataList, false);
            }

            ResultSetMetaData metaData = resultSet.getMetaData();
            ArrayList result = null;
            while (resultSet.next()) {
              Map row = new HashMap();
              for (int j = 1; j <= metaData.getColumnCount(); j++) {
                String columnName = metaData.getColumnName(j).toUpperCase();
                if (!columnName.equalsIgnoreCase("ROWNUM_")) {
                  Object _value = resultSet.getObject(j);
                  Object value = null;
                  if (_value != null) {
                    if (_value instanceof String) {
                      value = _value;
                    } else if (_value instanceof BigDecimal) {
                      value = _value;
                    } else if (_value instanceof Long) {
                      value = _value;
                    } else if (_value instanceof Integer) {
                      value = _value;
                    } else if (_value instanceof Date) {
                      value = new Date(((Date) _value).getTime());
                    } 
                  }
                  row.put(columnName, value);
                }
              }
              if (result == null) {
                result = new ArrayList();
              }
              result.add(row);
            }
            return result;
          } finally {
            resultSet.close();
          }
        } finally {
          preparedStatement.close();
        }
      } finally {
        if (newConnection) {
          closeConnection(connection);
        }
      }
    } catch (RuntimeException ex) {
      Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, sql);
      if (parametersValues != null) {
        Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, "" + Arrays.asList(parametersValues));
      }
      Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, ex.getLocalizedMessage(), ex);
      throw ex;
    } catch (Exception ex) {
      Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, sql);
      if (parametersValues != null) {
        Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, "" + Arrays.asList(parametersValues));
      }
      Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, ex.getLocalizedMessage(), ex);
      throw new RuntimeException(ex);
    }
  }

  protected static void _metadataResultSet(ResultSet resultSet, Map<String, Object> metadataMap, List<Map> metadataList, boolean silent) {
    try {
      ResultSetMetaData metaData = resultSet.getMetaData();

      int columnsCount = metaData.getColumnCount();
      metadataMap.put("#COLUMNS", new BigDecimal(columnsCount));
      for (int j = 1; j <= columnsCount; j++) {
        String columnName = metaData.getColumnName(j).toUpperCase();
        String columnType = metaData.getColumnTypeName(j);

        Map<String, Object> metadataColumn = new HashMap();
        metadataColumn.put("NAME", columnName);
        metadataColumn.put("TYPE", columnType);

        metadataMap.put("#COLUMN_" + j, columnName);
        metadataMap.put(columnName + "#TYPE", columnType);
        metadataMap.put(columnName + "#NOTNULL", new Boolean(metaData.isNullable(j) == ResultSetMetaData.columnNoNulls));
        metadataColumn.put("NOTNULL", new Boolean(metaData.isNullable(j) == ResultSetMetaData.columnNoNulls));

        if (columnType.equals("VARCHAR2") || columnType.equals("CHAR")) {
          metadataMap.put(columnName, "");
          metadataMap.put(columnName + "#LENGTH", new Integer(metaData.getPrecision(j)));
          metadataColumn.put("LENGTH", new Integer(metaData.getPrecision(j)));
        } else if (columnType.equals("NUMBER")) {
          metadataMap.put(columnName, new BigDecimal(0));
          metadataMap.put(columnName + "#PRECISION", new Integer(metaData.getPrecision(j)));
          metadataColumn.put("PRECISION", new Integer(metaData.getPrecision(j)));
          metadataMap.put(columnName + "#SCALE", new Integer(metaData.getScale(j)));
          metadataColumn.put("SCALE", new Integer(metaData.getScale(j)));
        } else if (columnType.equals("DATE") || columnType.equals("TIME") || columnType.equals("TIMESTAMP")) {
          metadataMap.put(columnName, new Date());
        }

        metadataList.add(metadataColumn);
      }
    } catch (RuntimeException ex) {
      if (!silent) {
        Logger.getLogger(JdbcFetch.class.getName()).log(Level.SEVERE, ex.getLocalizedMessage(), ex);
      }
      throw ex;
    } catch (Exception ex) {
      if (!silent) {
        Logger.getLogger(JdbcFetch.class.getName()).log(Level.SEVERE, ex.getLocalizedMessage(), ex);
      }
      throw new RuntimeException(ex);
    }
  }

  public static Map retrieveMethods(Class clazz) {
    Map<String, Method> classMethods = null;
    if (classesMethods == null) {
      classesMethods = new HashMap();
    } else {
      classMethods = (Map) classesMethods.get(clazz);
    }
    if (classMethods == null) {
      classMethods = new HashMap();
      Method[] methods = clazz.getMethods();
      for (Method method : methods) {
        String methodName = method.getName().toLowerCase();
        classMethods.put(methodName, method);
      }
      classesMethods.put(clazz, classMethods);
    }
    return classMethods;
  }

  public static Method getMethod(Class clazz, String name) {
    return (Method) retrieveMethods(clazz).get(name);
  }

  public Map<String, Object> selectSingle(String sql) {
    return selectSingle(null, sql, null);
  }

  public Map<String, Object> selectSingle(String sql, Object[] parametersValues) {
    return selectSingle(null, sql, parametersValues);
  }

  public Map<String, Object> selectSingle(Connection connection, String sql) {
    return selectSingle(connection, sql, null);
  }

  public Map<String, Object> selectSingle(Connection connection, String sql, Object[] parametersValues) {
    List<Map> list = select(connection, sql, parametersValues);
    if (list == null || list.isEmpty()) {
      return null;
    }
    return list.get(0);
  }

  public Map<String, Object> selectFirst(String sql) {
    return selectFirst(null, sql, null);
  }

  public Map<String, Object> selectFirst(String sql, Object[] parametersValues) {
    return selectFirst(null, sql, parametersValues);
  }

  public Map<String, Object> selectFirst(Connection connection, String sql) {
    return selectFirst(connection, sql, null);
  }

  public Map<String, Object> selectFirst(Connection connection, String sql, Object[] parametersValues) {
    List<Map> list = select(connection, sql, parametersValues, 0, 1);
    if (list == null || list.isEmpty()) {
      return null;
    }
    return list.get(0);
  }

  public int selectCount(String sql) {
    return selectCount(null, sql, null);
  }

  public int selectCount(String sql, Object[] sqlParameters) {
    return selectCount(null, sql, sqlParameters);
  }

  public int selectCount(Connection connection, String sql) {
    return selectCount(connection, sql, null);
  }

  public int selectCount1(Connection connection, String sql, Object[] sqlParameters) {
    Map count = selectSingle(connection, "SELECT COUNT(*) ROWS_COUNT FROM (" + sql + ") a", sqlParameters);
    System.out.println(count.get("ROWS_COUNT"));
    return count != null && count.get("ROWS_COUNT") != null ? ((BigDecimal) count.get("ROWS_COUNT")).intValue() : 0;
  }

  public int selectCount(Connection connection, String sql, Object[] sqlParameters) {
    return ((Long) selectSingle(connection, "SELECT COUNT(*) ROWS_COUNT FROM (" + sql + ") a", sqlParameters).get("ROWS_COUNT")).intValue();
  }

  public BigDecimal selectSeq(String seqName) {
    return selectSeq(null, seqName);
  }

  public BigDecimal selectSeq(Connection connection, String seqName) {
    return (BigDecimal) selectSingle(connection, "SELECT " + seqName + ".NEXTVAL NEXT_VAL FROM DUAL").get("NEXT_VAL");
  }

}
