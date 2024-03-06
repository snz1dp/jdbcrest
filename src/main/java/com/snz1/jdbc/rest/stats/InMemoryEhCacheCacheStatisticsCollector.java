package com.snz1.jdbc.rest.stats;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cache.ehcache.EhCacheCacheManager;
import org.springframework.stereotype.Service;

import gateway.sc.v2.config.CacheStatistics;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@ConditionalOnProperty(prefix="spring.cache", name="type", havingValue="ehcache", matchIfMissing=false)
public class InMemoryEhCacheCacheStatisticsCollector implements CacheStatisticsCollector {

	@Autowired
	private EhCacheCacheManager ehCacheCacheManager;
	
	private CacheStatistics cacheStatistics = new CacheStatistics();
	
	private Long lastAccessTime;
	
	private Lock accessLock = new ReentrantLock();

	public InMemoryEhCacheCacheStatisticsCollector() {
		cacheStatistics.type = "ehcache";
		cacheStatistics.provider = "ehcache";
	}
	
	@Override
	public CacheStatistics getCacheStatistics() {

		// 1分钟收集一次数据
		if (lastAccessTime != null && System.currentTimeMillis() - lastAccessTime < 60 * 1000L)
			return cacheStatistics;
		
		long start_time = System.currentTimeMillis();
		if (!accessLock.tryLock()) return cacheStatistics;
		try {
			cacheStatistics.count = ehCacheCacheManager.getCacheNames().size();
			lastAccessTime = System.currentTimeMillis();
			return cacheStatistics;
		} finally {
			accessLock.unlock();
			if (log.isDebugEnabled()) {
				log.debug("获取在线统计数据耗时" + (System.currentTimeMillis() - start_time) + "毫秒");
			}
		}

	}

}
