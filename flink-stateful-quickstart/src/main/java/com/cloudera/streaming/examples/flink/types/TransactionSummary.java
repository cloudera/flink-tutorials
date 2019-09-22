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
