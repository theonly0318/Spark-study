package top.theonly.spark.jav.core.accumulator;

import java.io.Serializable;

public class PersonInfo implements Serializable {

    private Integer personCount;
    private Integer ageCount;
    private Integer maleCount;
    private Integer femaleCount;

    public PersonInfo() {
    }

    public PersonInfo(Integer personCount, Integer ageCount, Integer maleCount, Integer femaleCount) {
        this.personCount = personCount;
        this.ageCount = ageCount;
        this.maleCount = maleCount;
        this.femaleCount = femaleCount;
    }

    public Integer getPersonCount() {
        return personCount;
    }

    public void setPersonCount(Integer personCount) {
        this.personCount = personCount;
    }

    public Integer getAgeCount() {
        return ageCount;
    }

    public void setAgeCount(Integer ageCount) {
        this.ageCount = ageCount;
    }

    public Integer getMaleCount() {
        return maleCount;
    }

    public void setMaleCount(Integer maleCount) {
        this.maleCount = maleCount;
    }

    public Integer getFemaleCount() {
        return femaleCount;
    }

    public void setFemaleCount(Integer femaleCount) {
        this.femaleCount = femaleCount;
    }

    @Override
    public String toString() {
        return "PersonInfo{" +
                "personCount=" + personCount +
                ", ageCount=" + ageCount +
                ", maleCount=" + maleCount +
                ", femaleCount=" + femaleCount +
                '}';
    }
}
