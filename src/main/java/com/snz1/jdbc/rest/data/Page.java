package com.snz1.jdbc.rest.data;

import java.io.Serializable;
import java.util.List;

import lombok.Data;

@Data
public class Page<T> implements Serializable {
  /**
	 * 总量
	 */
	private Long total;
	
	/**
	 * 当前起始索引
	 */
	private Long offset;
	
	/**
	 * 当前分页数据列表
	 */
	private List<T> data;
	
}
