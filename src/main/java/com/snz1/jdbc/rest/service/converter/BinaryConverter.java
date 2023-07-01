package com.snz1.jdbc.rest.service.converter;

import java.sql.JDBCType;
import org.springframework.stereotype.Component;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;

@Component("jdbcrest::BinaryConverter")
public class BinaryConverter extends AbstractConverter {

  public BinaryConverter() {
    super(JDBCType.BINARY, JDBCType.VARBINARY, JDBCType.BLOB, JDBCType.CLOB, JDBCType.NCLOB);
  }

  @Override
  public Object convertObject(Object input) {
    if (input instanceof String) {
      try {
        return Base64.decodeBase64((String)input);
      } catch(Throwable t) {
        try {
          return Hex.decodeHex((String)input);
        } catch (DecoderException e) {
          return ((String)input).getBytes();
        }
      }
    }
    return input;
  }

}
