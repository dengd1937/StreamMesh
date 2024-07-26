package org.codehub.dp.util;

import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JdbcUtil {

    private static final Logger log = LoggerFactory.getLogger(JdbcUtil.class);

    public static String getDorisTableColumns(String database, String table, String host, int port, String user, String password) throws Exception {
        String sql = String.format("select group_concat(COLUMN_NAME) as COLUMNS" +
                " from information_schema.columns" +
                " where TABLE_SCHEMA = '%s' and TABLE_NAME = '%s'", database, table);
        List<JSONObject> resultObj = JdbcUtil.executeQuery(DBType.MYSQL, host, port, user, password, sql);
        if (resultObj == null || resultObj.size() == 0) {
            throw new Exception("Unable to find the fields in the " + table);
        }
        return resultObj.get(0).getString("COLUMNS");
    }

    public static List<String> getDorisTables(String database, String host, int port, String user, String password) throws Exception {
        String sql = String.format("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = 'internal' AND TABLE_SCHEMA = '%s'", database);
        List<JSONObject> resultObj = executeQuery(DBType.MYSQL, host, port, user, password,sql);
        List<String> schemaTables = new ArrayList<>();
        resultObj.forEach(r -> schemaTables.add(r.getString("TABLE_NAME")));
        return schemaTables;
    }

    public static List<JSONObject> executeQuery(DBType dbType, String hostUrl, int port, String user, String password, String sql) throws Exception {
        registerDriver(dbType);

        List<JSONObject> beJson = new ArrayList<>();
        String connectionUrl = String.format("jdbc:%s://%s:%s", dbType.getName(), hostUrl, port);
        Connection con = null;
        try {
            con = DriverManager.getConnection(connectionUrl, user, password);
            PreparedStatement ps = con.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            beJson = resultSetToJson(rs);
        } catch (Exception e) {
            log.error("报错堆栈信息: ", e);
        } finally {
            if (con != null) {
                try {
                    con.close();
                } catch (SQLException e) {
                    log.error("报错堆栈信息: ", e);
                }
            }
        }
        return beJson;
    }

    private static void registerDriver(DBType dbType) throws Exception {
        String driverClass = null;
        switch (dbType) {
            case MYSQL:
                driverClass = "com.mysql.cj.jdbc.Driver";
                break;
            case SQLSERVER:
                driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
                break;
        }
        if (driverClass == null) {
            throw new Exception("The database type is currently not supported: " + dbType);
        }

        try {
            Class.forName(driverClass);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private static List<JSONObject> resultSetToJson(ResultSet rs) throws SQLException {
        List<JSONObject> list = new ArrayList<>();
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        while (rs.next()) {
            JSONObject jsonObj = new JSONObject();
            for (int i = 1; i <= columnCount; i++) {
                String columnName = metaData.getColumnLabel(i);
                String value = rs.getString(columnName);
                jsonObj.put(columnName, value);
            }
            list.add(jsonObj);
        }
        return list;
    }


}
