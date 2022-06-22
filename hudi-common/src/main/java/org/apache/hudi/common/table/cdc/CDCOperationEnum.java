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

package org.apache.hudi.common.table.cdc;

import org.apache.hudi.exception.HoodieNotSupportedException;

public enum CDCOperationEnum {
  INSERT("i"),
  UPDATE("u"),
  DELETE("d");

  private final String value;

  CDCOperationEnum(String value) {
    this.value = value;
  }

  public String getValue() {
    return this.value;
  }

  public static CDCOperationEnum parse(String value) {
    if (value.equals("i")) {
      return INSERT;
    } else if (value.equals("u")) {
      return UPDATE;
    } else if (value.equals("d")) {
      return DELETE;
    } else {
      throw new HoodieNotSupportedException("Unsupported value: " + value);
    }
  }
}
