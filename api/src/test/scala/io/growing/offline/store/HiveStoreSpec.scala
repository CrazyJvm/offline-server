package io.growing.offline.store

import io.growing.offline.SparkTestBase

/**
  * Created by king on 12/29/16.
  * 测试HiveStoreLoader的存储功能
  * 通过读取写入的数据验证存储成功
  */
class HiveStoreSpec extends SparkTestBase with HiveStore {

  test("load2hive save") {
    // 创建hive表
    session.sql(
      """
        |create table load2hive(
        |   id string,
        |   name string,
        |   age int)
        |partitioned by (city string)
        |stored as orc
      """.stripMargin)
    val df = session.createDataFrame(Seq(Person("id1", "name1", 23, "shanghai"),
      Person("id2", "name2", 20, "hangzhou")))

    session.sql("describe load2hive")

    store2hive(df, "load2hive", Seq(("city", None)), isOverWrite = true)

    // 读取数据
    val result = session.sql("select id, name, age, city from load2hive where city = 'shanghai'")
    checkAnswer(df.filter("city = 'shanghai'"), result)
  }

}

case class Person(id: String, name: String, age: Int, city: String)