package com.spark.user_session.domain;

//session 聚合统计domain
public class SessionAggr {

    private long   taskid;
    private long   sessioncount;
    private double visitlength_1s_3s_ratio;
    private double visitlength_4s_6s_ratio;
    private double visitlength_7s_9s_ratio;
    private double visitlength_10s_30s_ratio;
    private double visitlength_30s_60s_ratio;
    private double visitlength_1m_3m_ratio;
    private double visitlength_3m_10m_ratio;
    private double visitlength_10m_30m_ratio;
    private double visitlength_30m_ratio;
    private double steplength_1_3_ratio;
    private double steplength_4_6_ratio;
    private double steplength_7_9_ratio;
    private double steplength_10_30_ratio;
    private double steplength_30_60_ratio;
    private double steplength_60_ratio;



    @Override
    public String toString() {
        return "SessionAggr{" +
                "taskid=" + taskid +
                ", sessioncount=" + sessioncount +
                ", visitlength_1s_3s_ratio=" + visitlength_1s_3s_ratio +
                ", visitlength_4s_6s_ratio=" + visitlength_4s_6s_ratio +
                ", visitlength_7s_9s_ratio=" + visitlength_7s_9s_ratio +
                ", visitlength_10s_30s_ratio=" + visitlength_10s_30s_ratio +
                ", visitlength_30s_60s_ratio=" + visitlength_30s_60s_ratio +
                ", visitlength_1m_3m_ratio=" + visitlength_1m_3m_ratio +
                ", visitlength_3m_10m_ratio=" + visitlength_3m_10m_ratio +
                ", visitlength_10m_30m_ratio=" + visitlength_10m_30m_ratio +
                ", visitlength_30m_ratio=" + visitlength_30m_ratio +
                ", steplength_1_3_ratio=" + steplength_1_3_ratio +
                ", steplength_4_6_ratio=" + steplength_4_6_ratio +
                ", steplength_7_9_ratio=" + steplength_7_9_ratio +
                ", steplength_10_30_ratio=" + steplength_10_30_ratio +
                ", steplength_30_60_ratio=" + steplength_30_60_ratio +
                ", steplength_60_ratio=" + steplength_60_ratio +
                '}';
    }

    public long getTaskid() {
        return taskid;
    }

    public void setTaskid(long taskid) {
        this.taskid = taskid;
    }

    public long getSessioncount() {
        return sessioncount;
    }

    public void setSessioncount(long sessioncount) {
        this.sessioncount = sessioncount;
    }

    public double getVisitlength_1s_3s_ratio() {
        return visitlength_1s_3s_ratio;
    }

    public void setVisitlength_1s_3s_ratio(double visitlength_1s_3s_ratio) {
        this.visitlength_1s_3s_ratio = visitlength_1s_3s_ratio;
    }

    public double getVisitlength_4s_6s_ratio() {
        return visitlength_4s_6s_ratio;
    }

    public void setVisitlength_4s_6s_ratio(double visitlength_4s_6s_ratio) {
        this.visitlength_4s_6s_ratio = visitlength_4s_6s_ratio;
    }

    public double getVisitlength_7s_9s_ratio() {
        return visitlength_7s_9s_ratio;
    }

    public void setVisitlength_7s_9s_ratio(double visitlength_7s_9s_ratio) {
        this.visitlength_7s_9s_ratio = visitlength_7s_9s_ratio;
    }

    public double getVisitlength_10s_30s_ratio() {
        return visitlength_10s_30s_ratio;
    }

    public void setVisitlength_10s_30s_ratio(double visitlength_10s_30s_ratio) {
        this.visitlength_10s_30s_ratio = visitlength_10s_30s_ratio;
    }

    public double getVisitlength_30s_60s_ratio() {
        return visitlength_30s_60s_ratio;
    }

    public void setVisitlength_30s_60s_ratio(double visitlength_30s_60s_ratio) {
        this.visitlength_30s_60s_ratio = visitlength_30s_60s_ratio;
    }

    public double getVisitlength_1m_3m_ratio() {
        return visitlength_1m_3m_ratio;
    }

    public void setVisitlength_1m_3m_ratio(double visitlength_1m_3m_ratio) {
        this.visitlength_1m_3m_ratio = visitlength_1m_3m_ratio;
    }

    public double getVisitlength_3m_10m_ratio() {
        return visitlength_3m_10m_ratio;
    }

    public void setVisitlength_3m_10m_ratio(double visitlength_3m_10m_ratio) {
        this.visitlength_3m_10m_ratio = visitlength_3m_10m_ratio;
    }

    public double getVisitlength_10m_30m_ratio() {
        return visitlength_10m_30m_ratio;
    }

    public void setVisitlength_10m_30m_ratio(double visitlength_10m_30m_ratio) {
        this.visitlength_10m_30m_ratio = visitlength_10m_30m_ratio;
    }

    public double getVisitlength_30m_ratio() {
        return visitlength_30m_ratio;
    }

    public void setVisitlength_30m_ratio(double visitlength_30m_ratio) {
        this.visitlength_30m_ratio = visitlength_30m_ratio;
    }

    public double getSteplength_1_3_ratio() {
        return steplength_1_3_ratio;
    }

    public void setSteplength_1_3_ratio(double steplength_1_3_ratio) {
        this.steplength_1_3_ratio = steplength_1_3_ratio;
    }

    public double getSteplength_4_6_ratio() {
        return steplength_4_6_ratio;
    }

    public void setSteplength_4_6_ratio(double steplength_4_6_ratio) {
        this.steplength_4_6_ratio = steplength_4_6_ratio;
    }

    public double getSteplength_7_9_ratio() {
        return steplength_7_9_ratio;
    }

    public void setSteplength_7_9_ratio(double steplength_7_9_ratio) {
        this.steplength_7_9_ratio = steplength_7_9_ratio;
    }

    public double getSteplength_10_30_ratio() {
        return steplength_10_30_ratio;
    }

    public void setSteplength_10_30_ratio(double steplength_10_30_ratio) {
        this.steplength_10_30_ratio = steplength_10_30_ratio;
    }

    public double getSteplength_30_60_ratio() {
        return steplength_30_60_ratio;
    }

    public void setSteplength_30_60_ratio(double steplength_30_60_ratio) {
        this.steplength_30_60_ratio = steplength_30_60_ratio;
    }

    public double getSteplength_60_ratio() {
        return steplength_60_ratio;
    }

    public void setSteplength_60_ratio(double steplength_60_ratio) {
        this.steplength_60_ratio = steplength_60_ratio;
    }

    public SessionAggr(long taskid, long sessioncount, double visitlength_1s_3s_ratio, double visitlength_4s_6s_ratio, double visitlength_7s_9s_ratio, double visitlength_10s_30s_ratio, double visitlength_30s_60s_ratio, double visitlength_1m_3m_ratio, double visitlength_3m_10m_ratio, double visitlength_10m_30m_ratio, double visitlength_30m_ratio, double steplength_1_3_ratio, double steplength_4_6_ratio, double steplength_7_9_ratio, double steplength_10_30_ratio, double steplength_30_60_ratio, double steplength_60_ratio) {
        this.taskid = taskid;
        this.sessioncount = sessioncount;
        this.visitlength_1s_3s_ratio = visitlength_1s_3s_ratio;
        this.visitlength_4s_6s_ratio = visitlength_4s_6s_ratio;
        this.visitlength_7s_9s_ratio = visitlength_7s_9s_ratio;
        this.visitlength_10s_30s_ratio = visitlength_10s_30s_ratio;
        this.visitlength_30s_60s_ratio = visitlength_30s_60s_ratio;
        this.visitlength_1m_3m_ratio = visitlength_1m_3m_ratio;
        this.visitlength_3m_10m_ratio = visitlength_3m_10m_ratio;
        this.visitlength_10m_30m_ratio = visitlength_10m_30m_ratio;
        this.visitlength_30m_ratio = visitlength_30m_ratio;
        this.steplength_1_3_ratio = steplength_1_3_ratio;
        this.steplength_4_6_ratio = steplength_4_6_ratio;
        this.steplength_7_9_ratio = steplength_7_9_ratio;
        this.steplength_10_30_ratio = steplength_10_30_ratio;
        this.steplength_30_60_ratio = steplength_30_60_ratio;
        this.steplength_60_ratio = steplength_60_ratio;
    }

    public SessionAggr(){}
}
