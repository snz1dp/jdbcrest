package com.snz1.jdbc.rest.stats;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.redisson.api.RKeys;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import gateway.sc.v2.config.CacheStatistics;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@ConditionalOnProperty(prefix="spring.cache", name="type", havingValue="redis", matchIfMissing=false)
public class InMemoryRedissonCacheStatisticsCollector implements CacheStatisticsCollector {

	@Autowired
	private RedissonClient redissonClient;
	
	private CacheStatistics cacheStatistics = new CacheStatistics();
	
	private Long lastAccessTime;
	
	private Lock accessLock = new ReentrantLock();

	public InMemoryRedissonCacheStatisticsCollector() {
		cacheStatistics.type = "redis";
		cacheStatistics.provider = "redisson";
	}
	
	@Override
	public CacheStatistics getCacheStatistics() {

		// 1分钟收集一次数据
		if (lastAccessTime != null && System.currentTimeMillis() - lastAccessTime < 60 * 1000L)
			return cacheStatistics;
		
		long start_time = System.currentTimeMillis();
		if (!accessLock.tryLock()) return cacheStatistics;
		try {
			RKeys keys = redissonClient.getKeys();
			cacheStatistics.count = keys.count();
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
