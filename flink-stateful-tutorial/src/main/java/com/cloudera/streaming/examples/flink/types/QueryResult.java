/*
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.streaming.examples.flink.types;

import java.util.Objects;

/**
 * Results for an item quantity query.
 */
public class QueryResult {

	public long queryId;

	public ItemInfo itemInfo;

	public QueryResult() {
	}

	public QueryResult(long queryId, ItemInfo itemInfo) {
		this.queryId = queryId;
		this.itemInfo = itemInfo;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		QueryResult that = (QueryResult) o;
		return queryId == that.queryId &&
				Objects.equals(itemInfo, that.itemInfo);
	}

	@Override
	public int hashCode() {
		return Objects.hash(queryId, itemInfo);
	}

	@Override
	public String toString() {
		return "QueryResult{" +
				"queryId=" + queryId +
				", itemInfo=" + itemInfo +
				'}';
	}
}
