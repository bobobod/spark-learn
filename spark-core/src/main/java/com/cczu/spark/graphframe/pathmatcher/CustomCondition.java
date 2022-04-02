package com.cczu.spark.graphframe.pathmatcher;

import java.io.Serializable;
import java.util.List;

/**
 * @author yjz
 * @date 2022/4/2
 */
public class CustomCondition implements Serializable {
    private String key;
    private String operator;
    private String value;
    private String logic;
    private List<CustomCondition> group;
    private boolean ignoreCase;

    public CustomCondition() {
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getLogic() {
        return logic;
    }

    public void setLogic(String logic) {
        this.logic = logic;
    }

    public List<CustomCondition> getGroup() {
        return group;
    }

    public void setGroup(List<CustomCondition> group) {
        this.group = group;
    }

    public boolean isIgnoreCase() {
        return ignoreCase;
    }

    public void setIgnoreCase(boolean ignoreCase) {
        this.ignoreCase = ignoreCase;
    }

    @Override
    public String toString() {
        return "CustomCondition{" +
                "key='" + key + '\'' +
                ", operator='" + operator + '\'' +
                ", value='" + value + '\'' +
                ", logic='" + logic + '\'' +
                ", group=" + group +
                ", ignoreCase=" + ignoreCase +
                '}';
    }
}
