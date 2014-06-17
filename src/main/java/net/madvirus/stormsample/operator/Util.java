package net.madvirus.stormsample.operator;

import storm.trident.operation.BaseFilter;
import storm.trident.operation.Filter;
import storm.trident.tuple.TridentTuple;

public class Util {

	public static Filter printer() {
		return new PrintFilter();
	}

	public static class PrintFilter extends BaseFilter {
		private static final long serialVersionUID = 1L;

		@Override
		public boolean isKeep(TridentTuple tuple) {
			System.err.println("tuple = " + tuple);
			return false;
		}
		
	}
}
