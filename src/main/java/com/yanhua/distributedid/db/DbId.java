package com.yanhua.distributedid.db;

import java.sql.*;
import java.util.concurrent.*;

/**
 * 数据id生成方案
 *
 * @author xuyanhua
 * @description:
 * @date 2019/3/29 上午9:53
 */
public class DbId {
    private Connection connection;

    public DbId() {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            String url = "jdbc:mysql://127.0.0.1:3306/did?characterEncoding=utf8&useSSL=true&serverTimezone=Asia/Shanghai";
            String user = "root";
            String password = "admin";
            connection = DriverManager.getConnection(url, user, password);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public Long id() {
        String sql = "INSERT INTO did (`c`) VALUES (0)";
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            pstmt = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            pstmt.executeUpdate();
            rs = pstmt.getGeneratedKeys();
            if (rs.next()) {
                long id = rs.getLong(1);
                return new Long(id);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (pstmt != null) {
                try {
                    pstmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return 0L;
    }

    public static void main(String[] args) throws InterruptedException {

        final int taskSize = 10;
        final int taskPority = 1000;
        final CountDownLatch cd = new CountDownLatch(taskPority * taskSize);
        final DbId dbId = new DbId();
        ExecutorService executorService = new ThreadPoolExecutor(10, 10, 10, TimeUnit.MICROSECONDS, new ArrayBlockingQueue<>(200));
        for (int i = 0; i < taskSize; i++) {
            executorService.execute(new Runnable() {
                public void run() {
                    for (int i = 0; i < taskPority; i++) {
                        System.out.println(dbId.id());
                        cd.countDown();
                    }
                }
            });
        }
        long time1 = System.currentTimeMillis();
        cd.await();
        long time2 = System.currentTimeMillis();
        //1w个id需要19457ms，平均每秒生成512个
        System.out.println("运行时间：" + (time2 - time1));


    }
}
