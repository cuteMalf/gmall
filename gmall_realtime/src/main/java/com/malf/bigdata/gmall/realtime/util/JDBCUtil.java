package com.malf.bigdata.gmall.realtime.util;

import com.google.common.base.CaseFormat;
import com.malf.bigdata.gmall.realtime.bean.TableProcess;
import com.malf.bigdata.gmall.realtime.common.GmallConfig;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JDBCUtil {
    public static Connection getPhoenixConnection()  {

        return getJDBCConnection(GmallConfig.PHOENIX_DRIVER, GmallConfig.PHOENIX_URL,null,null);
    }

    public static Connection getMysqlConnection()  {

        return getJDBCConnection(GmallConfig.MYSQL_DRIVER, GmallConfig.MYSQL_URL,"root","aaaaaa");
    }

    public static Connection getClickHouseConnection(){
        return getJDBCConnection(GmallConfig.CLICKHOUSE_DRIVER,GmallConfig.CLICKHOUSE_URL,"ck","123456");

    }

    private static Connection getJDBCConnection(String driver, String url, String user, String password) {

        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            new RuntimeException("你提供的驱动类不存在-->"+driver);
        }
        Connection connection =null;


        try {
            connection = DriverManager.getConnection(url, user, password);
        } catch (SQLException e) {
            e.printStackTrace();
            new RuntimeException("url,user,password.这三者可能有错误!");
        }


        return connection;
    }

    public static void close(Connection connection) throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();

        }
    }

    public static <T> List<T> queryList(Connection connection,String sql,Object[] args,Class<T> tClass,boolean ... isUnderLine){
        boolean needToChangeCamel=false;
        if (isUnderLine.length==1&&isUnderLine[0]==true){
            needToChangeCamel=true;
        }
        List<T> list = new ArrayList<>();

        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            //给占位符赋值
            for (int i = 0; args!=null&&i < args.length; i++) {
                preparedStatement.setObject(i+1,args[i]);
            }

            //preparedStatement.execute(); 用于增删改和DML 的执行
            ResultSet resultSet = preparedStatement.executeQuery(); //用于查询
            ResultSetMetaData metaData = resultSet.getMetaData();
            while (resultSet.next()){
                //将查询出来的数据封装为T
                // 如何new 一个泛型对象
                T t = tClass.newInstance();
                for (int i = 0; i < metaData.getColumnCount(); i++) {
                    String columnName;
                    //1.列名：元数据
                    columnName = metaData.getColumnName(i + 1);
                    if (needToChangeCamel) {
                        columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                    }
                    //2.值：数据集
                    Object object = resultSet.getObject(i + 1);
                    //bean 工具类
                    BeanUtils.setProperty(t,columnName,object);

                }
               list.add(t);
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

return list;
    }

    public static void main(String[] args) {
        List<TableProcess> list = JDBCUtil.queryList(getMysqlConnection(), "select * from table_process", null, TableProcess.class,true);
        for (TableProcess tableProcess : list) {
            System.out.println("tableProcess = " + tableProcess);
        }
    }
}

