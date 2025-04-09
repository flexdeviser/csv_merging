package org.e4s.dto;

public class Read {

    private String device;

    private String timeKey;

    private Float v;

    private Float p;

    private Float q;


    public String getDevice() {
        return device;
    }

    public void setDevice(String device) {
        this.device = device;
    }

    public String getTimeKey() {
        return timeKey;
    }

    public void setTimeKey(String timeKey) {
        this.timeKey = timeKey;
    }

    public Float getV() {
        return v;
    }

    public void setV(Float v) {
        this.v = v;
    }

    public Float getP() {
        return p;
    }

    public void setP(Float p) {
        this.p = p;
    }

    public Float getQ() {
        return q;
    }

    public void setQ(Float q) {
        this.q = q;
    }
}
