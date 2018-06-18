package com.spark.user_session.jdbc;


import java.util.ArrayList;
import java.util.List;

public class JDBCHelperTest {
    public static void main(String[] args) {
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();

        /*jdbcHelper.executeUpdate("insert into scores(Id,Score) values(?,?)",
                        new Object[]{7,2.5});
        final Map<String,Object> testUser = new HashMap<String,Object>();
        jdbcHelper.executeQuery(
                "select * from scores where Id = ?",
                new Object[]{7},
                new JDBCHelper.QueryCallBack() {
                    @Override
                    public void process(ResultSet rs) throws Exception {
                        if(rs.next()){
                            testUser.put(
                                    String.valueOf(rs.getInt(1)),
                                    rs.getDouble(2)
                            );
                        }
                    }
                }
        );

        System.out.println(testUser);*/

        String sql = "insert into scores(Id,Score) values(?,?)";
        List<Object[]> paramList = new ArrayList<>();
        paramList.add(new Object[]{8,1.0});
        paramList.add(new Object[]{9,3.5});
        jdbcHelper.executeBatch(sql,paramList);
    }
}
