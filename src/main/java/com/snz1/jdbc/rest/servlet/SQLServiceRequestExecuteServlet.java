package com.snz1.jdbc.rest.servlet;

import java.io.IOException;

import javax.annotation.Resource;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.springframework.http.MediaType;
import org.springframework.web.servlet.HttpServletBean;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.snz1.jdbc.rest.RunConfig;
import com.snz1.jdbc.rest.data.JdbcQueryResponse;
import com.snz1.jdbc.rest.data.SQLServiceDefinition;
import com.snz1.jdbc.rest.data.SQLServiceRequest;
import com.snz1.jdbc.rest.service.AppInfoResolver;
import com.snz1.jdbc.rest.service.JdbcRestProvider;
import com.snz1.jdbc.rest.service.SQLServiceRegistry;
import com.snz1.jdbc.rest.utils.RequestUtils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@WebServlet(urlPatterns = SQLServiceRequestExecuteServlet.PATH)
public class SQLServiceRequestExecuteServlet extends HttpServletBean {

  public static final String PATH = "/services/*";

  @Resource
  private RunConfig runConfig;

  @Resource
  private SQLServiceRegistry serviceRegistry;

  @Resource
  private JdbcRestProvider serviceProvider;

  @Resource
  private ObjectMapper objectMapper;

  @Resource
  private AppInfoResolver appInfoResolver;

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    String service_path = req.getRequestURI().substring(runConfig.getWebroot().length());
    if (log.isDebugEnabled()) {
      log.debug("SQL服务请求地址={}", service_path);
    }
    if (StringUtils.endsWith(service_path, "/")) {
      service_path = service_path.substring(0, service_path.length() - 1);
    }

    SQLServiceDefinition sql_service = serviceRegistry.getService(service_path);
    Validate.notNull(sql_service, "SQL服务不存在");
    SQLServiceRequest sql_request = SQLServiceRequest.of(sql_service);
    sql_request.setInput_data(RequestUtils.fetchManipulationRequestData(req, false));

    Object result = serviceProvider.executeSQLService(sql_request);
    JdbcQueryResponse<Object> resposne = new JdbcQueryResponse<>();
    resposne.setData(result);
    resposne.setLic(appInfoResolver.getLicenseMeta());
    
    resp.addHeader("Content-Type", MediaType.APPLICATION_JSON_VALUE);
    objectMapper.writeValue(resp.getOutputStream(), resposne);
    resp.flushBuffer();
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    String service_path = req.getRequestURI().substring(runConfig.getWebroot().length());
    if (log.isDebugEnabled()) {
      log.debug("SQL服务请求地址={}", service_path);
    }
    if (StringUtils.endsWith(service_path, "/")) {
      service_path = service_path.substring(0, service_path.length() - 1);
    }
    resp.addHeader("Content-Type", MediaType.APPLICATION_JSON_VALUE);

    JdbcQueryResponse<Object> resposne = new JdbcQueryResponse<>();
    resposne.setLic(appInfoResolver.getLicenseMeta());

    if (StringUtils.equals(service_path, "/services")) {
      resposne.setData(serviceRegistry.getServices());
      objectMapper.writeValue(resp.getOutputStream(), resposne);
    } else {
      SQLServiceDefinition sql_service = serviceRegistry.getService(service_path);
      Validate.notNull(sql_service, "SQL服务不存在");
      resposne.setData(sql_service);
      objectMapper.writeValue(resp.getOutputStream(), resposne);
    }
    resp.flushBuffer();
  }

}
