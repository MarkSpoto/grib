package io.phdata.tools.grib.core.common;

import java.lang.reflect.Array;
import java.util.EnumSet;
import java.util.Map;
import java.util.function.Function;

import com.google.common.collect.Maps;


/**
 * Created by cisaksson on 8/13/19.
 */
public class EnumUtils {

  public static <T, E extends Enum<E>> Function<T, E> lookupMap(Class<E> clazz, Function<E, T> mapper) {
    @SuppressWarnings("unchecked")
    E[] emptyArray = (E[]) Array.newInstance(clazz, 0);
    return lookupMap(EnumSet.allOf(clazz).toArray(emptyArray), mapper);
  }

  public static <T, E extends Enum<E>> Function<T, E> lookupMap(E[] values, Function<E, T> mapper) {
    Map<T, E> index = Maps.newHashMapWithExpectedSize(values.length);
    for (E value : values) {
      index.put(mapper.apply(value), value);
    }
    return index::get;
  }
}