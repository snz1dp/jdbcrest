package com.snz1.jdbc.rest.service.converter;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.beanutils.locale.BaseLocaleConverter;

import com.snz1.utils.JsonUtils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
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

      result = getDecimalFormat(locale, pattern).format(((Number) value).longValue());
    } else if ((value instanceof Double) ||
        (value instanceof BigDecimal) ||
        (value instanceof Float)) {

      result = getDecimalFormat(locale, pattern).format(((Number) value).doubleValue());
    } else if (value instanceof Date) { // java.util.Date, java.sql.Date, java.sql.Time, java.sql.Timestamp

      final SimpleDateFormat dateFormat = new SimpleDateFormat(JsonUtils.JsonDateFormat, locale);

      result = dateFormat.format(value);
    } else if (value instanceof Map || value instanceof Date || value instanceof List) {
      result = JsonUtils.toJson(value);
    } else {
      result = value.toString();
    }

    return result;
  }

  private DecimalFormat getDecimalFormat(final Locale locale, final String pattern) {
    final DecimalFormat numberFormat = (DecimalFormat) NumberFormat.getInstance(locale);
    // if some constructors default pattern to null, it makes only sense to handle
    // null pattern gracefully
    if (pattern != null) {
      if (locPattern) {
        numberFormat.applyLocalizedPattern(pattern);
      } else {
        numberFormat.applyPattern(pattern);
      }
    } else {
      log.debug("No pattern provided, using default.");
    }
    return numberFormat;
  }

}
