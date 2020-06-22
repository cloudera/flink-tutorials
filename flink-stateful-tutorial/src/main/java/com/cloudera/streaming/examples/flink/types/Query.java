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
 * Query for the actually available quantity of an item.
 */
public class Query {

	public long queryId;

	public long ts = System.currentTimeMillis();

	public String itemId;

	public Query() {}

	public Query(long queryId, String itemId) {
		this(queryId, null, itemId);
	}

	public Query(long queryId, Long ts, String itemId) {
		this.queryId = queryId;
		this.itemId = itemId;
		if (ts != null) {
			this.ts = ts;
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		Query query = (Query) o;
		return queryId == query.queryId &&
				ts == query.ts &&
				Objects.equals(itemId, query.itemId);
	}

	@Override
	public int hashCode() {
		return Objects.hash(queryId, ts, itemId);
	}

	@Override
	public String toString() {
		return "Query{" +
				"queryId=" + queryId +
				", ts=" + ts +
				", itemId='" + itemId + '\'' +
				'}';
	}
}
