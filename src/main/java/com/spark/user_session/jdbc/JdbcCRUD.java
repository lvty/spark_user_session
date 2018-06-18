package com.spark.user_session.jdbc;


import java.sql.*;


public class JdbcCRUD {
    public static void main(String[] args) {
        //insert();
        //update();
        //delete();
        select();
    }

    private static void select() {
        Connection connection = null;
        PreparedStatement psmt = null;
        ResultSet rs = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "root");

            String sql = "select Id,Score from scores where Id > 3";
            psmt = connection.prepareStatement(sql);

            rs = psmt.executeQuery();
            while (rs.next()) {
                System.out.println("Id: " + rs.getInt(1) + " Score: " + rs.getDouble(2));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (psmt != null) {
                try {
                    psmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static void delete() {
        Connection connection = null;
        PreparedStatement psmt = null;

        try {
            Class.forName("com.mysql.jdbc.Driver");
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "root");

            String sql = "delete from scores where Id = ?";
            psmt = connection.prepareStatement(sql);
            psmt.setInt(1, 7);

            psmt.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (psmt != null) {
                try {
                    psmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static void update() {
        Connection connection = null;
        PreparedStatement psmt = null;

        try {
            Class.forName("com.mysql.jdbc.Driver");
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "root");

            String sql = "UPDATE scores set Score = ? where Id = ?";
            psmt = connection.prepareStatement(sql);
            psmt.setDouble(1, 4.0);
            psmt.setInt(2, 7);

            psmt.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (psmt != null) {
                try {
                    psmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    private static void insert() {
        Connection connection = null;
        PreparedStatement psmt = null;

        try {
            Class.forName("com.mysql.jdbc.Driver");
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "root");

            String sql = "insert into scores(Id,Score) values(?,?)";
            psmt = connection.prepareStatement(sql);

            psmt.setInt(1, 7);
            psmt.setDouble(2, 3.5);

            psmt.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (psmt != null) {
                try {
                    psmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

    }
}
