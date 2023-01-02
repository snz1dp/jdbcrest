package com.snz1.jdbc.rest.data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class ManipulationRequest extends JdbcRestfulRequest {
 
  // 主键
  @Setter
  @JsonIgnore
  private Object primary_key;

  // 输入数据
  @Setter
  @Getter
  private Object input_data;

  // 输入主键
  @Setter
  @Getter
  @JsonIgnore
  private Object input_key;

  // 字段
  @Setter
  @Getter
  @JsonIgnore
  private List<TableColumn> columns;

  // 自定义主键
  @Setter
  @Getter
  @JsonIgnore
  private RequestCustomKey custom_key = new RequestCustomKey();

  @JsonIgnore
  private List<Map<String, Object>> _input_list;

  // 唯一索引
  @JsonIgnore
  private TableIndex unique_index;

  @JsonIgnore
  private List<TableIndex> _unique_index;

  // 是否
  @Setter
  @Getter
  @JsonIgnore
  private boolean patch_update;

  // 条件过滤
  @Setter
  @Getter
  private List<WhereCloumn> where = new LinkedList<>();

  // 复制表元信息
  public void copyTableMeta(TableMeta table_meta) {
    this.setDefinition(table_meta.getDefinition());
    this.setPrimary_key(table_meta.getPrimary_key());
    this.setUnique_indexs(table_meta.getUnique_indexs());
    this.setColumns(table_meta.getColumns());
  }
  
  // 是否有主键
  public boolean hasPrimary_key() {
    if (this.custom_key.hasCustom_key()) {
      return true;
    }
    return this.primary_key != null;
  }

  // 是否符合主键
  public boolean testComposite_key() {
    if (this.custom_key.hasCustom_key()) {
      return this.custom_key.testComposite_key();
    }
    if (primary_key != null) {
      return primary_key instanceof List;
    }
    if (this.getUnique_index() == null ||
      this.getUnique_indexs().size() == 1) return false;
    return true;
  }

  // 是否有主键输入
  public boolean hasInput_key() {
    return this.input_key != null;
  }

  // 获取主键
  public Object getPrimary_key() {
    if (this.custom_key.hasCustom_key()) return this.custom_key.getCustom_key();
    return this.primary_key;
  }

  // 是否子增长ID
  public boolean hasAutoGenerated() {
    for (TableColumn col : columns) {
      if (col.getAuto_increment() != null && col.getAuto_increment()) {
        return true;
      }
    }
    return false;
  }

  // 是单数据
  public boolean testSignletonData() {
    if (this.input_data == null) return true;
    return !(this.input_data instanceof List);
  }

  @JsonIgnore
  @SuppressWarnings("unchecked")
  public List<Map<String, Object>> getInput_list() {
    if (this._input_list != null) return this._input_list;
    if (this.getInput_data() instanceof List) {
      return this._input_list = (List<Map<String, Object>>)this.getInput_data();
    } else {
      return this._input_list = Arrays.asList((Map<String, Object>)this.getInput_data());
    }
  }

  @JsonIgnore
  public Map<String, Object> getInput_map() {
    return this.getInput_list().get(0);
  }

    // 是否有行主键，包括唯一索引
  public boolean hasRow_key() {
    return this.hasPrimary_key() || this.hasUnique_index();
  }

  // 获取行主键
  public Object getRow_key() {
    if (this.hasPrimary_key()) return this.getPrimary_key();
    if (this.getUnique_index() == null) return null;
    if (this.getUnique_indexs().size() == 1) return this.getUnique_index().getColumn();
    List<String> keynames = new LinkedList<>();
    for (TableIndex keyindex : this.getUnique_indexs()) {
      keynames.add(keyindex.getColumn());
    }
    return keynames;
  }

  public boolean testRow_key(String column) {
    Object row_key = getRow_key();
    if (row_key == null) return false;
    if (row_key instanceof List) {
      List<?> row_keylist = (List<?>)row_key;
      for (int i = 0; i < row_keylist.size(); i++) {
        if (StringUtils.equalsIgnoreCase(column, (String)row_keylist.get(i))) {
          return true;
        }
      }
      return false;
    } else {
      return StringUtils.equalsIgnoreCase(column, (String)row_key);
    }
  }

  // 是否有唯一索引
  public boolean hasUnique_index() {
    return this.unique_index != null;
  }

  public void setUnique_indexs(List<TableIndex> unique_index) {
    if (unique_index == null || unique_index.size() == 0) {
      this.unique_index = null;
      this._unique_index = null;
    } else if (unique_index.size() > 0) {
      this.unique_index = unique_index.get(0);
      this._unique_index = unique_index;
    }
  }

  @JsonIgnore
  public List<TableIndex> getUnique_indexs() {
    return _unique_index;
  }

  public void setUnique_index(TableIndex unique_index) {
    if (unique_index == null) {
      this.unique_index = null;
      this._unique_index = null;
    } else {
      this.unique_index = unique_index;
      this._unique_index = new ArrayList<>(Arrays.asList(unique_index));
    }
  }

  @JsonIgnore
  public TableIndex getUnique_index() {
    return unique_index;
  }

  public TableColumn findColumn(String column) {
    int table_end = column.indexOf('.');
    if (table_end != -1) {
      String table_name = column.substring(0, table_end);
      column = column.substring(table_end + 1);
      for (TableColumn col : columns) {
        if (StringUtils.equalsIgnoreCase(column, col.getName()) && 
          StringUtils.equalsIgnoreCase(table_name, col.getTable_name())
        ) {
          return col;
        }
      }
    } else {
      for (TableColumn col : columns) {
        if (StringUtils.equalsIgnoreCase(column, col.getName())) {
          return col;
        }
      }
    }
    return null;
  }


  // 有查询条件
  public boolean hasWhere() {
    return this.where != null && this.where.size() > 0;
  }

  public boolean hasColumns() {
    return this.columns != null;
  }

  // 重新编译查询条件
  public void rebuildWhere() {
    if (!this.hasColumns()) return;
    this.where.forEach(w -> {
      if (w.getType() != null) return;
      TableColumn col = this.findColumn(w.getColumn());
      if (col == null) return;
      w.setType(col.getJdbc_type());
    });
  }

}
