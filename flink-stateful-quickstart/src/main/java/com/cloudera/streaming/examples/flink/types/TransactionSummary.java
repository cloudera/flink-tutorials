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

public class TransactionSummary {

	public String itemId;

	public int numSuccessfulTransactions = 0;

	public int numFailedTransactions = 0;

	public long totalVolume = 0;

	public TransactionSummary() {
	}

	public TransactionSummary(String itemId, int numSuccessfulTransactions, int numFailedTransactions, long totalVolume) {
		this.itemId = itemId;
		this.numSuccessfulTransactions = numSuccessfulTransactions;
		this.numFailedTransactions = numFailedTransactions;
		this.totalVolume = totalVolume;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) { return true; }
		if (o == null || getClass() != o.getClass()) { return false; }
		TransactionSummary that = (TransactionSummary) o;
		return numSuccessfulTransactions == that.numSuccessfulTransactions &&
				numFailedTransactions == that.numFailedTransactions &&
				totalVolume == that.totalVolume &&
				Objects.equals(itemId, that.itemId);
	}

	@Override
	public int hashCode() {
		return Objects.hash(itemId, numSuccessfulTransactions, numFailedTransactions, totalVolume);
	}

	@Override
	public String toString() {
		return "TransactionSummary{" +
				"itemId='" + itemId + '\'' +
				", numSuccessfulTransactions=" + numSuccessfulTransactions +
				", numFailedTransactions=" + numFailedTransactions +
				", totalVolume=" + totalVolume +
				'}';
	}
}
