# JDBC转REST服务

这是一个JDBC转REST服务的实现。

## 1、数据查询

## 1.1、从数据表分页查询开始

- 请求方式：`GET`或`POST`
- 接口地址：/jdbc/rest/api/tables/<表名>
- 请求参数：`QueryString`、`PostForm`
- 请求参数：

| 参数名称              | 参数类型  | 是否必须 | 缺省值 | 参数说明 |
| -------------------- | ------- | ------- | ----------- | ----------------------- |
| offset               | 数值     |  否     | 0           | 返回记录开始索引值             |
| limit                | 数值     |  否     | 0           | 最大返回数量值<br>不能超过1000   |
| _select              | 字符串   |  否     |           | 查询字段，格式为：<br>字段1,字段2,字段3,字段4<br>sum:字段1,avg:字段2,max:字段3,min:字段4->>别名1  |
| _distinct              | 字符串   |  否     |           | 为true时表示排除重行  |
| _groupby              | 字符串   |  否     |           | 分组查询，格式为：<br>分组字段->>having:sum(test):$gt:12<br>分组字段  |
| _order              | 字符串   |  否     |           | 字段排序，格式为：<br>-字段1,字段2  |
| _count              | 字符串   |  否     |           | 统计记录，格式为：字段名  |
| _result.signleton   | 布尔   |  否     | map          | 为true时表示单对象返回  |
| _result.row_struct   | 枚举   |  否     | map          | 可选值：<br>list表示列表<br>map表示对象  |
| _result.contain_meta | 布尔     |  否     | false        | 为true时在信封中返回数据元信息 |
| _result.all_column   | 布尔     |  否     | true        | 为true时返回所有字段数据，<br>否则由_result.column参数指定返回    |
| _result.column       | 字符串数组 |  否     |             | 可多个，用户设置返回字段或字段返回类型 |
| _result.column.&lt;field&gt;.type | 枚举  |  否     | raw       | 可选值：<br>raw表示保留Jdbc原值<br>map表示转换为对象值<br>list表示转换为列表值<br>base64表示转换为Base64编码值          |
| _result.column.&lt;field&gt;.alais | 字符串  |  否     |        | 设置指定的字段返回为其他名称          |
| &lt;field&gt;[$&lt;type&gt;] | 字符串  |  否     |        | 字段名格式：字段名$JDBC类型<br>值格式：操作符[.&lt;值&gt;]          |

- 操作符：

| 操作符               | 说明 | 示例 |
| -------------------- | ---------------------- | --------------------- |
| $eq     | 等于 | name=$eq.abc, value$integer=$eq.4 |
| $gt | 大于 | value$float=$gt.3 |
| $gte | 大于等于 | value$integer=$gte.4 |
| $lt | 小于 | value$integer=$lt.4 |
| $lte | 小于等于 | value$integer=$lte.4 |
| $ne | 不等于 | value$integer=$ne.4, name=$ne.abc |
| $in | 在..内 | value=$in.abc,1234 |
| $nin | 不在..内 | value=$nin.abc,1234 |
| $isnull | 为空 | value=$isnull |
| $notnull | 不为空 | value=$notnull |
| $istrue | 为真 | value=$istrue |
| $nottrue | 不为真 | value=$nottrue |
| $isfalse | 为假 | value=$isfalse |
| $notfalse | 不为假 | value=$notfalse |
| $like | 包含字符串，区分大小写 | value=$like.test%25，<br>其中%25代表% |
| $ilike | 包含字符串，不区分大小写 | value=$ilike.test%25 |
| $nlike | 不包含字符串，区分大小写 | value=$nlike.test%25 |
| $nilike | 不包含字符串，不区分大小写 | value=$nilike.test%25 |
| $between | 在..之间 | value=$between.1,4 |

- 应答格式：

```json
{
  "code": 0, // 响应代码，成功为0，其他为失败
  "message": "string", // 响应信息，如：操作成功
  "data": { // 分页数据对象
    "total": 6,
      "offset": 0,
      "data": [ // 数据行列表
        ..., // 数据行对象或数组
        ..., // 请求参数_result.row_struct为map时数据行为对象方式：{...}
        ..., // 请求参数_result.row_struct为list时数据行为数组方式：[...]
      ] 
  },
  "meta": { // 数据元信息，请求参数“_result.contain_meta”为true时存在
    "columns": [ // 字段信息列表
      { // 字段信息
        "index": 0, // 字段索引，从0开始
        "name": "id", // 字段名，如果字段设置了别名，这里将返回别名
        "label": "id", // 字段标签
        "sql_type": "uuid", // SQL中的类型
        "jdbc_type": "OTHER", // JDBC类型
        "display_size": 2147483647, // 显示大小
        "precision": 2147483647, // 精度
        "scale": 0, // 刻度
        "read_only": false, // 是否只读
        "writable": true, // 是否可写
        "auto_increment": false, // 是否自增
        "searchable": true, // 是否可查询
        "currency": false, // 是否货币
        "nullable": false, // 是否为空
        "case_sensitive": false, // 是否区分大小写
        "table_name": "table" // 字段所属表名
      },
      ... // 多个字段
    ],
    "table_name": "routes", // 第一个字段所属表名，可为空
    "column_count": 22, // 查询返回的字段数量
  }
}
```

- 查询示例

```bash
curl "http://localhost:7188/jdbc/rest/api/tables/mytable"
```
