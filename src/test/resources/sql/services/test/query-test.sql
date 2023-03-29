

/*

################################################
# 定义返回格式(YAML格式)
################################################

title: 测试

# 是否单行返回，默认为false
signleton: false

# 单行返回且只有一个字段则只返回字段内容，默认为false
column_compact: false

# 字段定义
#columns:
#- name: <字段名>
#  type: raw, map, list, base64

################################################
*/
select * from course offset #{input.offset, jdbcType=INTEGER} limit 100;
