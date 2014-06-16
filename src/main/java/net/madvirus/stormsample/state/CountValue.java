package net.madvirus.stormsample.state;

public class CountValue {

	private Long currentTxId;
	private Long prevCount;
	private Long currCount;

	public CountValue(Long currentTxId, Long prevCount, Long currCount) {
		this.currentTxId = currentTxId;
		this.prevCount = prevCount;
		this.currCount = currCount;
	}

	public Long getCurrentTxId() {
		return currentTxId;
	}

	public void setCurrentTxId(Long currentTxId) {
		this.currentTxId = currentTxId;
	}

	public Long getPrevCount() {
		return prevCount;
	}

	public void setPrevCount(Long prevCount) {
		this.prevCount = prevCount;
	}

	public Long getCurrCount() {
		return currCount;
	}

	public void setCurrCount(Long currCount) {
		this.currCount = currCount;
	}

	public void update(Long currTxid, Long prev, Long curr) {
		this.currentTxId = currTxid;
		this.prevCount = prev;
		this.currCount = curr;
	}

}
