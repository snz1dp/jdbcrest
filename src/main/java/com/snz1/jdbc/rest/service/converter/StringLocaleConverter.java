package com.snz1.jdbc.rest.service.converter;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.commons.beanutils.locale.BaseLocaleConverter;

import com.snz1.utils.JsonUtils;

public class StringLocaleConverter extends BaseLocaleConverter {

  public StringLocaleConverter() {
    super(Locale.getDefault(), null, false);
  }

  protected Object parse(final Object value, final String pattern) throws ParseException {
    String result = null;
    if ((value instanceof Integer) ||
        (value instanceof Long) ||
        (value instanceof BigInteger) ||
        (value instanceof Byte) ||
        (value instanceof Short)) {
      result = value + "";
    } else if ((value instanceof Double) ||
        (value instanceof BigDecimal) ||
        (value instanceof Float)) {
      result = value + "";
    } else if (value instanceof Date) { // java.util.Date, java.sql.Date, java.sql.Time, java.sql.Timestamp
      final SimpleDateFormat dateFormat = new SimpleDateFormat(JsonUtils.JsonDateFormat, locale);
      result = dateFormat.format(value);
    } else if (value instanceof String) {
      return value;
    } else if (value.getClass().isEnum() || value.getClass().isPrimitive()) {
      return value.toString();
    } else {
      result = JsonUtils.toJson(value);
    }
    return result;
  }

}
