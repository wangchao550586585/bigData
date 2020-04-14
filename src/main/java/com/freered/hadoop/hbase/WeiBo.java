package com.freered.hadoop.hbase;

/**
 * 1:发布微博内容
 * 2：查看关注人，取消关注
 * 3：查看我的粉丝
 * 4：查看关注人的微博列表，按照时间降序排序的前1000个微博
 * 5：我发布的微博列表
 * 
 * 表1：微博表
 * rowkey：UID_WID(用户id_微博id)
 * cf1:微博内容
 * 
 * 表2：用户关注表
 * rowkey:UID_U1(用户id_关注的用户id)
 * cf1:
 * 
 * 表2-2：用户关注表
 * rowkey:UID(用户id)
 * cf1:u1=u1(关注者id=关注者id)
 * 
 * 表3：粉丝表
 * rowkey:UID_U1
 * cf1:
 * 
 * 表4：微博收件箱
 * rowkey:UID
 * cf1:微博表的rowkey(UID_WID)，保存的最大版本为1000
 */
public class WeiBo {

}
