package com.lonicera.paxos.core.codec;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.reflect.Type;

public class ObjectCodec {

  private static final ObjectMapper objectMapper = new ObjectMapper();

  public byte[] encode(Object object) {
    try {
      return objectMapper.writeValueAsBytes(object);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public <T> T decode(byte[] bytes, Type t) {
    try {
      return objectMapper.readValue(bytes, new TypeReference<T>() {
        @Override
        public Type getType() {
          return t;
        }
      });
    } catch (Exception e) {
      throw new IllegalArgumentException(e);
    }
  }

  private static final ObjectCodec CODEC = new ObjectCodec();

  public static ObjectCodec codec(){
    return CODEC;
  }

}
