/*
 * Licensed to the Apache Software Foundation (ASF) under one
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

public class ItemInfo {

	public String itemId;

	public int quantity;

	public ItemInfo() {
	}

	public ItemInfo(String itemId, int quantity) {
		this.itemId = itemId;
		this.quantity = quantity;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) { return true; }
		if (o == null || getClass() != o.getClass()) { return false; }
		ItemInfo itemInfo = (ItemInfo) o;
		return quantity == itemInfo.quantity &&
				Objects.equals(itemId, itemInfo.itemId);
	}

	@Override
	public int hashCode() {
		return Objects.hash(itemId, quantity);
	}

	@Override
	public String toString() {
		return "ItemInfo{" +
				"itemId='" + itemId + '\'' +
				", quantity=" + quantity +
				'}';
	}
}
