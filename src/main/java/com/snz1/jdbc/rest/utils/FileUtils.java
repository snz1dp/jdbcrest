package com.snz1.jdbc.rest.utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.Date;

@SuppressWarnings("deprecation")
public abstract class FileUtils extends org.apache.commons.io.FileUtils {
  
  public static Date getCreateTime(File file) throws IOException {
    FileTime created_time = Files.readAttributes(file.toPath(), BasicFileAttributes.class).creationTime();
    return new Date(created_time.toMillis());
  }

}
