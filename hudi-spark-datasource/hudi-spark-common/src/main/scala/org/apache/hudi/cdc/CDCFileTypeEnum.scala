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

package org.apache.hudi.cdc

object CDCFileTypeEnum extends Enumeration {
  type CDCFileType = Value
  // Here the `FileGroupReplaced` means a file group is replaced in an instant.
  // Unlike the other cdc file type that there is a single file can be loaded to retrieve the changing data,
  // if `FileGroupReplaced`, we will mark all data from a whole file slice as `delete`.
  // So in [[ ChangeFileForSingleFileGroup ]], if `cdcFileType` is `FileGroupReplaced`,
  // `cdcFile` must be null, and `dependentFileSlice` is not empty and represent the file group
  // that will be marked as `delete`.
  val CDCLogFile, PureAddFile, PureRemoveFile, MorLogFile, FileGroupReplaced = Value
}
