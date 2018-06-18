package com.spark.user_session.domain;

public class SessionDetail {
    private long               task_id;
    private long               user_id;
    private String          session_id;
    private long               page_id;
    private String         action_time;
    private String      search_keyword;
    private long     click_category_id;
    private long      click_product_id;
    private String  order_category_ids;
    private String   order_product_ids;
    private String    pay_category_ids;
    private String     pay_product_ids;

    public long getTask_id() {
        return task_id;
    }

    public void setTask_id(long task_id) {
        this.task_id = task_id;
    }

    public long getUser_id() {
        return user_id;
    }

    public void setUser_id(long user_id) {
        this.user_id = user_id;
    }

    public String getSession_id() {
        return session_id;
    }

    public void setSession_id(String session_id) {
        this.session_id = session_id;
    }

    public long getPage_id() {
        return page_id;
    }

    public void setPage_id(long page_id) {
        this.page_id = page_id;
    }

    public String getAction_time() {
        return action_time;
    }

    public void setAction_time(String action_time) {
        this.action_time = action_time;
    }

    public String getSearch_keyword() {
        return search_keyword;
    }

    public void setSearch_keyword(String search_keyword) {
        this.search_keyword = search_keyword;
    }

    public long getClick_category_id() {
        return click_category_id;
    }

    public void setClick_category_id(long click_category_id) {
        this.click_category_id = click_category_id;
    }

    public long getClick_product_id() {
        return click_product_id;
    }

    public void setClick_product_id(long click_product_id) {
        this.click_product_id = click_product_id;
    }

    public String getOrder_category_ids() {
        return order_category_ids;
    }

    public void setOrder_category_ids(String order_category_ids) {
        this.order_category_ids = order_category_ids;
    }

    public String getOrder_product_ids() {
        return order_product_ids;
    }

    public void setOrder_product_ids(String order_product_ids) {
        this.order_product_ids = order_product_ids;
    }

    public String getPay_category_ids() {
        return pay_category_ids;
    }

    public void setPay_category_ids(String pay_category_ids) {
        this.pay_category_ids = pay_category_ids;
    }

    public String getPay_product_ids() {
        return pay_product_ids;
    }

    public void setPay_product_ids(String pay_product_ids) {
        this.pay_product_ids = pay_product_ids;
    }
}
