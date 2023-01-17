package com.snz1.jdbc.rest.data;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

import lombok.Data;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TableIndexs implements Serializable {
  
  private List<TableIndex> unique_indexs;

  private List<TableIndex> normal_indexs;

  public TableIndexs addUnique_index(TableIndex index) {
    if (this.unique_indexs == null) {
      this.unique_indexs = new LinkedList<>();
    }
    this.unique_indexs.add(index);
    return this;
  }

  public TableIndexs addNormal_index(TableIndex index) {
    if (this.normal_indexs == null) {
      this.normal_indexs = new LinkedList<>();
    }
    this.normal_indexs.add(index);
    return this;
  }

}
