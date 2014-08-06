package net.madvirus.stormsample.state;

public class CountValue2 {

	private Long txId;
	private Long count;

	public CountValue2(Long txId, Long count) {
		this.txId = txId;
		this.count = count;
	}

	public Long getTxId() {
		return txId;
	}

	public Long getCount() {
		return count;
	}

	public void update(Long newTxId, Long newCount) {
		this.txId = newTxId;
		this.count = newCount;
	}

}
