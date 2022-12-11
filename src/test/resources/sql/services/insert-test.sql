/*

################################################
# 定义返回格式(YAML格式)
################################################

# 是否单行返回，默认为false
signleton: true

# 单行返回且只有一个字段则只返回字段内容，默认为false
column_compact: true

################################################
*/

insert into test(
  name, creator_id, created_time, updated_time
) values(
  #{input.name, jdbcType=VARCHAR},
  #{user.user_name, jdbcType=VARCHAR},
  #{req.time, jdbcType=TIMESTAMP},
  #{req.time, jdbcType=TIMESTAMP}
)
