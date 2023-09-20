package com.snz1.jdbc.rest.utils;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;

import org.apache.commons.lang3.StringUtils;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.DumperOptions.FlowStyle;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.nodes.NodeTuple;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.representer.Representer;

import com.snz1.jdbc.rest.data.JdbcProviderMeta;
import com.snz1.jdbc.rest.data.TableDefinition;

import org.yaml.snakeyaml.introspector.Property;

public abstract class YamlUtils {
  
  public static <T> T fromYaml(InputStream ism, Class<T> clazz) {
    return fromYaml(new InputStreamReader(ism), clazz);
  }

  public static <T> T fromYaml(Reader reader, Class<T> clazz) {
    LoaderOptions loader_options = new LoaderOptions();
    Yaml yaml = new Yaml(loader_options);
    return yaml.loadAs(reader, clazz);
  }
  
  public static <T> T fromYaml(String reader, Class<T> clazz) {
    return fromYaml(new StringReader(reader), clazz);
  }

  public static String toYaml(Object object) {
    DumperOptions dumper_options = new DumperOptions();
    dumper_options.setDefaultFlowStyle(FlowStyle.BLOCK);
    Yaml yaml = new Yaml(new CustomRepresenter(dumper_options), dumper_options);
    return yaml.dump(object);
  }

  public static class CustomRepresenter extends Representer {
  public CustomRepresenter(DumperOptions options) {
    super(options);
    this.addClassTag(gateway.sc.v2.PermissionDefinition.class, Tag.MAP);
    this.addClassTag(gateway.sc.v2.Position.class, Tag.MAP);
    this.addClassTag(gateway.sc.v2.Department.class, Tag.MAP);
    this.addClassTag(gateway.sc.v2.FunctionNode.class, Tag.MAP);
    this.addClassTag(gateway.sc.v2.OrgTreeNode.class, Tag.MAP);
    this.addClassTag(gateway.sc.v2.FunctionTreeNode.class, Tag.MAP);
    this.addClassTag(gateway.sc.v2.Role.class, Tag.MAP);
    this.addClassTag(gateway.sc.v2.RoleGroup.class, Tag.MAP);
    this.addClassTag(gateway.sc.v2.User.class, Tag.MAP);
    this.addClassTag(gateway.sc.v2.FunctionNode.Type.class, Tag.STR);
    this.addClassTag(gateway.sc.v2.OrgNode.Type.class, Tag.STR);
    this.addClassTag(JdbcProviderMeta.class, Tag.MAP);
    this.addClassTag(TableDefinition.class, Tag.MAP);
  }

  @Override
  protected NodeTuple representJavaBeanProperty(Object javaBean, Property property,
      Object propertyValue, Tag customTag) {
    if (propertyValue == null || (
      StringUtils.equals("functionType", property.getName()) &&
      javaBean instanceof gateway.sc.v2.FunctionNode
    )) {
      return null;
    } else {
      return super.representJavaBeanProperty(javaBean, property, propertyValue, Tag.MAP);
    }
  }
}


}
