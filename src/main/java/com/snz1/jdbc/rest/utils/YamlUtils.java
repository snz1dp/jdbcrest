package com.snz1.jdbc.rest.utils;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;

import com.esotericsoftware.yamlbeans.YamlException;
import com.esotericsoftware.yamlbeans.YamlReader;
import com.esotericsoftware.yamlbeans.YamlWriter;
import com.esotericsoftware.yamlbeans.YamlConfig;

public abstract class YamlUtils {
  
  public static <T> T fromYaml(InputStream ism, Class<T> clazz) throws YamlException {
    return fromYaml(new InputStreamReader(ism), clazz);
  }

  public static <T> T fromYaml(Reader reader, Class<T> clazz) throws YamlException {
    YamlReader yamlReader = new YamlReader(reader);
    return yamlReader.read(clazz);
  }
  
  public static <T> T fromYaml(String reader, Class<T> clazz) throws YamlException {
    return fromYaml(new StringReader(reader), clazz);
  }

  public static String toYaml(Object object) throws YamlException {
    StringWriter string_writer = new StringWriter();
    YamlWriter yaml_writer = new YamlWriter(string_writer);
    try {
      yaml_writer.getConfig().writeConfig.setEscapeUnicode(false);
      yaml_writer.getConfig().writeConfig.setIndentSize(2);
      yaml_writer.getConfig().writeConfig.setAutoAnchor(false);
      yaml_writer.getConfig().writeConfig.setWriteClassname(YamlConfig.WriteClassName.NEVER);
      yaml_writer.getConfig().setPrivateFields(false);
      yaml_writer.write(object);
    } finally {
      yaml_writer.close();
    }
    return string_writer.toString();
  }

}
