package com.snz1.jdbc.rest.service.impl;

import java.io.ByteArrayInputStream;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.codec.binary.Base64;

import com.snz1.jdbc.rest.data.JdbcQueryRequest;
import com.snz1.jdbc.rest.data.JdbcQueryResponse;
import com.snz1.jdbc.rest.data.ResultDefinition;
import com.snz1.jdbc.rest.data.TableColumn;
import com.snz1.jdbc.rest.data.TableDefinition;
import com.snz1.jdbc.rest.data.TableIndex;
import com.snz1.jdbc.rest.data.TableMeta;
import com.snz1.jdbc.rest.service.JdbcTypeConverterFactory;
import com.snz1.jdbc.rest.service.LoggedUserContext;
import com.snz1.jdbc.rest.service.SQLDialectProvider;
import com.snz1.jdbc.rest.utils.JdbcUtils;
import com.snz1.utils.JsonUtils;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public abstract class AbstractJdbcQueryRequestHandler<T> extends AbstractRequestHandler<T> {

  private JdbcQueryRequest request;


  // 执行获取结果集
  @SuppressWarnings("null")
  protected JdbcQueryResponse<List<Object>> doFetchResultSet(
    ResultSet rs, ResultDefinition return_meta,
    Object primary_key, List<TableIndex> unique_index,
    TableDefinition table_definition
  ) throws SQLException {
    boolean onepack = true; 
    boolean meta = false;
    boolean objlist = false;

    if (return_meta != null) {
      meta = return_meta.isContain_meta();
      onepack = return_meta.isColumn_compact();
      objlist = ResultDefinition.ResultRowStruct.list.equals(return_meta.getRow_struct());
    }

    TableMeta result_meta = TableMeta.of(
      rs.getMetaData(),
      return_meta,
      primary_key,
      unique_index,
      table_definition
    );
    List<Object> rows = new LinkedList<>();

    while(rs.next()) {
      List<Object> row_list = null;
      Map<String, Object> row_map = null;
      Object row_data = null;
      if (onepack && result_meta.getColumn_count() == 1) {
        row_data = null;
      } else if (objlist) {
        row_list = new LinkedList<>();
      } else {
        row_map = new LinkedHashMap<>();
      }
      for (TableColumn col_item : result_meta.getColumns()) {
        String col_name = col_item.getName();
        Object col_obj = JdbcUtils.getResultSetValue(rs, col_item.getIndex() + 1);
        if (col_obj != null) {
          ResultDefinition.ResultColumn coldef = return_meta != null ? return_meta.getColumns().get(col_item.getName()) : null;
          if (coldef != null && !Objects.equals(coldef.getType(), ResultDefinition.ResultType.raw)) {
            if (Objects.equals(coldef.getType(), ResultDefinition.ResultType.list)) {
              if (col_item.getJdbc_type() == JDBCType.BLOB) {
                col_obj = JsonUtils.fromJson(new ByteArrayInputStream((byte[])col_obj), List.class);
              } else {
                col_obj = JsonUtils.fromJson(col_obj.toString(), List.class);
              }
            } else if (Objects.equals(coldef.getType(), ResultDefinition.ResultType.map)) {
              if (col_item.getJdbc_type() == JDBCType.BLOB) {
                col_obj = JsonUtils.fromJson(new ByteArrayInputStream((byte[])col_obj), Map.class);
              } else {
                col_obj = JsonUtils.fromJson(col_obj.toString(), Map.class);
              }
            } else {
              if (col_item.getJdbc_type() == JDBCType.BLOB) {
                col_obj = Base64.encodeBase64String((byte[])col_obj);
              } else {
                col_obj = Base64.encodeBase64String(col_obj.toString().getBytes());
              }
            }
          } else if (col_item.getJdbc_type() == JDBCType.BLOB) {
            col_obj = Base64.encodeBase64String((byte[])col_obj);
          }
        }
        if (onepack && result_meta.getColumn_count() == 1) {
          row_data = col_obj;
        } else if (objlist) {
          row_list.add(col_obj);
        } else if (col_obj != null) {
          row_map.put(col_name, col_obj);
        }
      }

      if (onepack && result_meta.getColumn_count() == 1) {
        rows.add(row_data);
      } else if (objlist) {
        rows.add(row_list);
      } else {
        rows.add(row_map);
      }
    }
    JdbcQueryResponse<List<Object>> ret = new JdbcQueryResponse<>();
    if (meta) {
      ret.setMeta(result_meta);
    }
    ret.setData(rows);
    return ret;
  }

  // 执行获取主键
  @SuppressWarnings("unchecked")
  protected Object doFetchTablePrimaryKey(Connection conn, String table_name) throws SQLException {
    Object primary_key = null;
    ResultSet ks = conn.getMetaData().getPrimaryKeys(null, null, table_name);
    try {
      JdbcQueryResponse<List<Object>> list = doFetchResultSet(ks, null, null, null, null);
      if (list.getData() != null && list.getData().size() > 0) {
        List<Object> primary_key_lst = new LinkedList<>();
        for (Object keycol : list.getData()) {
          Map<String, Object> colobj = (Map<String, Object>)keycol;
          if (colobj.containsKey("column_name")) {
            primary_key_lst.add(colobj.get("column_name"));
          } else {
            primary_key_lst.add(colobj.get("COLUMN_NAME"));
          }
        }
        if (primary_key_lst.size() > 0) {
          primary_key = primary_key_lst.size() == 1 ? primary_key_lst.get(0) : primary_key_lst;
        }
      }
    } finally {
      JdbcUtils.closeResultSet(ks);
    }
    return primary_key;
  }


  // 执行获取唯一索引
  @SuppressWarnings("unchecked")
  protected List<TableIndex> doFetchTableUniqueIndex(Connection conn, String table_name) throws SQLException {
    List<TableIndex> index_lst = new LinkedList<>();
    ResultSet ks = conn.getMetaData().getIndexInfo(null, null, table_name, true, false);
    try {
      JdbcQueryResponse<List<Object>> list = doFetchResultSet(ks, null, null, null, null);
      if (list.getData() != null && list.getData().size() > 0) {
        for (Object index_item : list.getData()) {
          Map<String, Object> colobj = (Map<String, Object>)index_item;
          TableIndex index = new TableIndex();
          if (colobj.containsKey("index_name")) {
            index.setName((String)colobj.get("index_name"));
          } else {
            index.setName((String)colobj.get("INDEX_NAME"));
          }
          if (colobj.containsKey("column_name")) {
            index.setColumn((String)colobj.get("column_name"));
          } else {
            index.setColumn((String)colobj.get("COLUMN_NAME"));
          }
          if (colobj.containsKey("non_unique")) {
            if (colobj.get("non_unique") instanceof Boolean) {
              index.setUnique(Objects.equals(colobj.get("non_unique"), Boolean.FALSE));
            } else {
              index.setUnique(Objects.equals(colobj.get("non_unique"), 0));
            }
          } else if (colobj.get("NON_UNIQUE") instanceof Boolean) {
            index.setUnique(Objects.equals(colobj.get("NON_UNIQUE"), Boolean.FALSE));
          } else {
            index.setUnique(Objects.equals(colobj.get("NON_UNIQUE"), 0));
          }
          
          if (colobj.containsKey("asc_or_desc")) {
            index.setOrder((String)colobj.get("asc_or_desc"));
          } else {
            index.setOrder((String)colobj.get("ASC_OR_DESC"));
          }
          if (colobj.containsKey("type")) {
            index.setType((Integer)colobj.get("type"));
          } else {
            index.setType((Integer)colobj.get("type"));
          }
          index_lst.add(index);
        }
      }
    } finally {
      JdbcUtils.closeResultSet(ks);
    }
    return index_lst;
  }

  public AbstractJdbcQueryRequestHandler(
    JdbcQueryRequest request, SQLDialectProvider sql_dialect_provider,
    JdbcTypeConverterFactory type_converter_factory, LoggedUserContext loggedUserContext
  ) {
    super(sql_dialect_provider, type_converter_factory, loggedUserContext);
    this.request = request;
  }

}
