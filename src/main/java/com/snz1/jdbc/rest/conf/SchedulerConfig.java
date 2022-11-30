package com.snz1.jdbc.rest.conf;

import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.stereotype.Component;

/**
 * 在此其中配置调度，便于统一管理（不然一定忘了哪配置了调度）
 */
@EnableScheduling
@Component
public class SchedulerConfig {

  @org.springframework.scheduling.annotation.Scheduled(
    cron = "0 0/5 * * * ?"
  )
  public void schedule() {
  }

}
