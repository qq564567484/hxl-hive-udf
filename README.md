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


  
  
### UDF


###### udf_args_max : 返回输入参数列表的最大值

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

###### udf_args_max_index : 返回输入参数列表的最大值对应的下标(从0开始)
```sql
-- 注册函数
CREATE TEMPORARY FUNCTION udf_args_max_index AS 'udf.UDFArgsMaxIndex';  

-- 案例1
select udf_args_max_index(1,2,null,5,10) as res;  
-- 结果
+------+
| res  |
+------+
| 4    |
+------+

```
  
###### udf_args_min : 返回输入参数列表的最小值对应的下标(从0开始)
```sql
-- 注册函数
CREATE TEMPORARY FUNCTION udf_args_min_index AS 'udf.UDFArgsMinIndex';  

-- 案例1
select udf_args_min_index(1,2,null,5,10);
-- 结果
+------+
| _c0  |
+------+
| 1    |
+------+

```
  

###### udf_args_min_index : 返回输入参数列表的最小值
```sql
-- 注册函数
CREATE TEMPORARY FUNCTION udf_args_min AS 'udf.UDFArgsMin';  

-- 案例1
select udf_args_min_index(1,2,null,5,10) as res;  
-- 结果
+------+
| res  |
+------+
| 0    |
+------+

```


###### udf_array_concat : 数组拼接(不去重)
```sql
-- 注册函数
CREATE TEMPORARY FUNCTION udf_array_concat AS 'udf.UDFArrayConcat';

-- 案例1
select udf_array_concat(array(1,2,3),array(2,3,4)) as res;
-- 结果
+----------------+
|      res       |
+----------------+
| [1,2,3,2,3,4]  |
+----------------+

```

###### udf_array_distinct_concat : 数组拼接(去重)
```sql
-- 注册函数
CREATE TEMPORARY FUNCTION udf_array_distinct_concat AS 'udf.UDFArrayDistinctConcat';

-- 案例1
select udf_array_distinct_concat(array(1,2,3),array(2,3,4)) as res;

-- 结果
+------------+
|    res     |
+------------+
| [1,2,3,4]  |
+------------+

```



###### udf_array_exclude_index : 数组排除数据(从第一个数组中,排除掉下标在第二个数组中包含的值)
```sql
-- 注册函数
CREATE TEMPORARY FUNCTION udf_array_exclude_index AS 'udf.UDFArrayExcludeIndex';

-- 案例1
select udf_array_exclude_index(array(10,20,30,40,50),array(1,2,3)) as res;
-- 结果
+----------+
|   res    |
+----------+
| [10,50]  |
+----------+

```


###### udf_array_exclude_value : 数组排除数据(从第一个数组中,排除掉在第二个数组中包含的值)
```sql
-- 注册函数
CREATE TEMPORARY FUNCTION udf_array_exclude_value AS 'udf.UDFArrayExcludeValue';

-- 案例1
select udf_array_exclude_value(array('SS','UU','BB'),array('UU')) as res;
-- 结果
+--------------+
|     res      |
+--------------+
| ["SS","BB"]  |
+--------------+

```


###### udf_array_intersect : 数组交集(获得第数组之间的交集)
```sql
-- 注册函数
CREATE TEMPORARY FUNCTION udf_array_intersect AS 'udf.UDFArrayIntersect';

-- 案例1
select udf_array_intersect(array('SS','UU','BB'),array('UU')) as res;
-- 结果
+--------------+
|     res      |
+--------------+
| ["SS","BB"]  |
+--------------+


-- 案例2
select udf_array_intersect(array(101,202,303,404),array(101,404),array(404)) as res;
-- 结果
+--------+
|  res   |
+--------+
| [404]  |
+--------+

```


###### udf_array_slice : 数组截取(array,开始下标,截取长度)
```sql
-- 注册函数
CREATE TEMPORARY FUNCTION udf_array_slice AS 'udf.UDFArraySlice';

-- 案例1
select udf_array_slice(array('SS','UU','BB','XX','YY','ZZ'),0,3) as res;
-- 结果
+-------------------+
|        res        |
+-------------------+
| ["SS","UU","BB"]  |
+-------------------+


-- 案例2
select udf_array_slice(array(101,202,303,404),1,1) as res;
-- 结果
+--------+
|  res   |
+--------+
| [202]  |
+--------+

```

###### udf_is_number : 判断是否为数字,兼容字符串
```sql
-- 注册函数
CREATE TEMPORARY FUNCTION udf_is_number AS 'udf.UDFIsNumber';

-- 案例1
select udf_is_number('1000.12345') as res;
-- 结果
+-------+
|  res  |
+-------+
| true  |
+-------+


-- 案例2
select udf_is_number('1000.1xyz5') as res;
-- 结果
+--------+
|  res   |
+--------+
| false  |
+--------+

```
