package top.theonly.spark.jav.sql.over;

import java.io.Serializable;

public class UserAccInfo implements Serializable {

    private Long id;
    private String date;
    private Long duration;

    public UserAccInfo(Long id, String date, Long duration) {
        this.id = id;
        this.date = date;
        this.duration = duration;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public Long getDuration() {
        return duration;
    }

    public void setDuration(Long duration) {
        this.duration = duration;
    }
}
