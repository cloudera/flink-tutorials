package com.cloudera.streaming.examples.flink.utils;

import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;

import com.codahale.metrics.ExponentiallyDecayingReservoir;

public class ExponentialHistogram implements Histogram {

	private final com.codahale.metrics.Histogram dropwizardHistogram;

	public ExponentialHistogram(int size, double alpha) {
		this.dropwizardHistogram = new com.codahale.metrics.Histogram(
				new ExponentiallyDecayingReservoir(size, alpha));
	}

	public ExponentialHistogram() {
		this.dropwizardHistogram = new com.codahale.metrics.Histogram(
				new ExponentiallyDecayingReservoir());
	}

	@Override
	public void update(long value) {
		dropwizardHistogram.update(value);

	}

	@Override
	public long getCount() {
		return dropwizardHistogram.getCount();
	}

	@Override
	public HistogramStatistics getStatistics() {
		return new SlidingHistogramStatistics(dropwizardHistogram.getSnapshot());
	}

	public static final class SlidingHistogramStatistics extends HistogramStatistics {

		private final com.codahale.metrics.Snapshot snapshot;

		SlidingHistogramStatistics(com.codahale.metrics.Snapshot snapshot) {
			this.snapshot = snapshot;
		}

		@Override
		public double getQuantile(double quantile) {
			return snapshot.getValue(quantile);
		}

		@Override
		public long[] getValues() {
			return snapshot.getValues();
		}

		@Override
		public int size() {
			return snapshot.size();
		}

		@Override
		public double getMean() {
			return snapshot.getMean();
		}

		@Override
		public double getStdDev() {
			return snapshot.getStdDev();
		}

		@Override
		public long getMax() {
			return snapshot.getMax();
		}

		@Override
		public long getMin() {
			return snapshot.getMin();
		}
	}

}
