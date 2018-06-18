package com.spark.user_session.domain;

public class SessionRandomExtract {
    private long task_id;
    private String session_id;
    private String start_time;
    private String search_keywords;
    private String click_category_ids;


    public long getTask_id() {
        return task_id;
    }

    public void setTask_id(long task_id) {
        this.task_id = task_id;
    }

    public String getSession_id() {
        return session_id;
    }

    public void setSession_id(String session_id) {
        this.session_id = session_id;
    }

    public String getStart_time() {
        return start_time;
    }

    public void setStart_time(String start_time) {
        this.start_time = start_time;
    }

    public String getSearch_keywords() {
        return search_keywords;
    }

    public void setSearch_keywords(String search_keywords) {
        this.search_keywords = search_keywords;
    }

    public String getClick_category_ids() {
        return click_category_ids;
    }

    public void setClick_category_ids(String click_category_ids) {
        this.click_category_ids = click_category_ids;
    }
}
