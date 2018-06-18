package com.spark.user_session.dao.impl;

import com.spark.user_session.dao.ITaskDao;
import com.spark.user_session.domain.Task;
import com.spark.user_session.jdbc.JDBCHelper;

import java.sql.ResultSet;

public class TaskDaoImpl implements ITaskDao {

    /**
     * 实现根据主键查询任务
     * @param taskId
     * @return
     */
    @Override
    public Task findById(long taskId) {
        final Task task = new Task();

        String sql = "select * from task where task_id = ?";
        Object[] params = new Object[]{taskId};
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();

        jdbcHelper.executeQuery(sql, params, new JDBCHelper.QueryCallBack() {
            @Override
            public void process(ResultSet rs) throws Exception {
                if(rs.next()){
                    long task_id = rs.getLong(1);
                    String task_name = rs.getString(2);
                    String create_time = rs.getString(3);
                    String start_time = rs.getString(4);
                    String finish_time = rs.getString(5);
                    String task_type = rs.getString(6);
                    String task_status = rs.getString(7);
                    String task_param = rs.getString(8);

                    task.setTask_id(task_id);
                    task.setTask_name(task_name);
                    task.setCreate_time(create_time);
                    task.setStart_time(start_time);
                    task.setFinish_time(finish_time);
                    task.setTask_type(task_type);
                    task.setTask_status(task_status);
                    task.setTask_param(task_param);

                }
            }
        });

        return task;
    }
}
