# 背景

收集和开发一些常用的Hive UDF函数,也方便自己后续使用。
会慢慢将目前所用到的UDF都维护上来。

可以分为3类

- UDF
- UDAF
- UDTF  

  
  
  
## 使用和案例

进入命令行,添加包(以我自己路径为例)

```sql
add jar file:///home/hadoop/hive-udf-1.0-SNAPSHOT.jar; 
```


  
  
#### UDF

###### 返回输入参数列表的最大值

```sql
-- 注册函数
CREATE TEMPORARY FUNCTION udf_args_max AS 'udf.UDFArgsMax';

-- 案例1
select udf_args_max(1,2,null,5,10) as res;
-- 结果
+------+
| res  |
+------+
| 10   |
+------+


-- 案例2
select udf_args_max(null,'2021-12-01','2021-12-02',null,'2021-12-05','2021-12-06') as res;
-- 结果
+-------------+
|     res     |
+-------------+
| 2021-12-06  |
+-------------+
```

