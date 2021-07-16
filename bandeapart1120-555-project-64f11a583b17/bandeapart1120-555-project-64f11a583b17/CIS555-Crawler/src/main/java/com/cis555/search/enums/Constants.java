package com.cis555.search.enums;

public enum Constants {

    CHAR_SET("0123456789ABCDEF"),
    ROBOT_SPLIT("\\r?\\n|\\r"),
    INTHREAD_QUEUE_SIZE("20000"),
    BLOCK_SIZE(String.valueOf(20 * 1024 * 1024));

    private String value;

    Constants(String value){
        this.value = value;
    }

    public String value(){
        return this.value;
    }

}
