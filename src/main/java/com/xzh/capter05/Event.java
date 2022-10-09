package com.xzh.capter05;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 1、属性public
 * 2、提供无参构造方法
 * 3、属性序列化
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Event {
    public String user;
    public String url;
    public Long timestamp;
}
