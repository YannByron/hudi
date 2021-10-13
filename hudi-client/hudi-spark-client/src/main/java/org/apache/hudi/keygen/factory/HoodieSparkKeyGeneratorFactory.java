/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.keygen.factory;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieKeyGeneratorException;
import org.apache.hudi.keygen.ComplexKeyGenerator;
import org.apache.hudi.keygen.CustomKeyGenerator;
import org.apache.hudi.keygen.GlobalDeleteKeyGenerator;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.keygen.NonpartitionedKeyGenerator;
import org.apache.hudi.keygen.SimpleKeyGenerator;
import org.apache.hudi.keygen.TimestampBasedKeyGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Locale;

/**
 * Factory help to create {@link org.apache.hudi.keygen.KeyGenerator}.
 * <p>
 * This factory will try {@link HoodieWriteConfig#KEYGENERATOR_CLASS_NAME} firstly, this ensures the class prop
 * will not be overwritten by {@link KeyGeneratorType}
 */
public class HoodieSparkKeyGeneratorFactory {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieSparkKeyGeneratorFactory.class);

  public static KeyGenerator createKeyGenerator(TypedProperties props) throws IOException {
    String keyGeneratorClass = getKeyGeneratorClassName(props);
    try {
      return (KeyGenerator) ReflectionUtils.loadClass(keyGeneratorClass, props);
    } catch (Throwable e) {
      throw new IOException("Could not load key generator class " + keyGeneratorClass, e);
    }
  }

  public static String getKeyGeneratorClassName(TypedProperties props) {
    String keyGeneratorClass = props.getString(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key(), null);

    if (StringUtils.isNullOrEmpty(keyGeneratorClass)) {
      String keyGeneratorType = props.getString(HoodieWriteConfig.KEYGENERATOR_TYPE.key(), KeyGeneratorType.SIMPLE.name());
      KeyGeneratorType keyGeneratorTypeEnum;
      try {
        keyGeneratorTypeEnum = KeyGeneratorType.valueOf(keyGeneratorType.toUpperCase(Locale.ROOT));
      } catch (IllegalArgumentException e) {
        throw new HoodieKeyGeneratorException("Unsupported keyGenerator Type " + keyGeneratorType);
      }
      switch (keyGeneratorTypeEnum) {
        case SIMPLE:
          keyGeneratorClass = SimpleKeyGenerator.class.getName();
          break;
        case COMPLEX:
          keyGeneratorClass = ComplexKeyGenerator.class.getName();
          break;
        case TIMESTAMP:
          keyGeneratorClass = TimestampBasedKeyGenerator.class.getName();
          break;
        case CUSTOM:
          keyGeneratorClass = CustomKeyGenerator.class.getName();
          break;
        case NON_PARTITION:
          keyGeneratorClass = NonpartitionedKeyGenerator.class.getName();
          break;
        case GLOBAL_DELETE:
          keyGeneratorClass = GlobalDeleteKeyGenerator.class.getName();
          break;
        default:
          throw new HoodieKeyGeneratorException("Unsupported keyGenerator Type " + keyGeneratorType);
      }
    }
    return keyGeneratorClass;
  }

  /**
   * Convert hoodie-common KeyGenerator to SparkKeyGeneratorInterface implement.
   */
  public static String convertToSparkKeyGenerator(String keyGeneratorClassName) {
    switch (keyGeneratorClassName) {
      case "org.apache.hudi.keygen.ComplexAvroKeyGenerator":
        return "org.apache.hudi.keygen.ComplexKeyGenerator";
      case "org.apache.hudi.keygen.CustomAvroKeyGenerator":
        return "org.apache.hudi.keygen.CustomKeyGenerator";
      case "org.apache.hudi.keygen.GlobalAvroDeleteKeyGenerator":
        return "org.apache.hudi.keygen.GlobalDeleteKeyGenerator";
      case "org.apache.hudi.keygen.NonpartitionedAvroKeyGenerator":
        return "org.apache.hudi.keygen.NonpartitionedKeyGenerator";
      case "org.apache.hudi.keygen.SimpleAvroKeyGenerator":
        return "org.apache.hudi.keygen.SimpleKeyGenerator";
      case "org.apache.hudi.keygen.TimestampBasedAvroKeyGenerator":
        return "org.apache.hudi.keygen.TimestampBasedKeyGenerator";
      default:
        return keyGeneratorClassName;
    }
  }
}
