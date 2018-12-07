package DataStructures;

public class TupleSimilarity implements Comparable<TupleSimilarity>{
	private Integer key;
	private Double value;
	
	public TupleSimilarity(Integer key, Double value) {
		super();
		this.key = key;
		this.value = value;
	}
	public Integer getKey() {
		return key;
	}
	public void setKey(Integer key) {
		this.key = key;
	}
	public Double getValue() {
		return value;
	}
	public void setValue(Double value) {
		this.value = value;
	}
	
	@Override
	public int compareTo(TupleSimilarity other) {
		return other.getValue().compareTo(value);
	}
	
	@Override
	public String toString() {
		return key + ": " + value;
	}
}
