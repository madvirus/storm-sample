package net.madvirus.stormsample.operator;

public class ShopLog {

	private String type;
	private long productId;
	private long timestamp;

	public ShopLog(String type, long productId, long timestamp) {
		super();
		this.type = type;
		this.productId = productId;
		this.timestamp = timestamp;
	}

	public String getType() {
		return type;
	}

	public long getProductId() {
		return productId;
	}

	public long getTimestamp() {
		return timestamp;
	}

	@Override
	public String toString() {
		return "ShopLog [type=" + type + ", productId=" + productId + ", timestamp=" + timestamp + "]";
	}

}
