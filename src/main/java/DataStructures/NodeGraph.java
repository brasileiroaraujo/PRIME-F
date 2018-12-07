package DataStructures;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import scala.Tuple2;

public class NodeGraph {
	private Integer token;
	private int id;
	private Set<Integer> blocks;
	private TreeSet<TupleSimilarity> neighbors;
	private boolean isSource;
	private boolean marked;
//	private double sumWeight = 0.0;
//	private double numberOfNeighbors = 0.0;
	private boolean blackFlag = false;
	private long startTime;
	private int maxNumberOfNeighbors;
	
	
	public NodeGraph(int token, int id, Set<Integer> blocks, boolean isSource, int maxNumberOfNeighbors) {
		super();
		this.token = token;
		this.id = id;
		this.blocks = blocks;
		this.neighbors = new TreeSet<TupleSimilarity>();
		this.isSource = isSource;
		this.maxNumberOfNeighbors = maxNumberOfNeighbors;
		this.startTime = System.currentTimeMillis();
	}
	
	public NodeGraph(int token, int id, Set<Integer> blocks, boolean isSource , int maxNumberOfNeighbors,
			boolean markedToCompare) {
		super();
		this.token = token;
		this.id = id;
		this.blocks = blocks;
		this.neighbors = new TreeSet<TupleSimilarity>();
		this.isSource = isSource;
		this.maxNumberOfNeighbors = maxNumberOfNeighbors;
		this.startTime = System.currentTimeMillis();
		this.marked = markedToCompare;
	}
	
	
//	public Node(int id, Set<Integer> blocks, Set<Tuple2<Integer, Double>> neighbors, boolean isSource, Integer tokenTemporary) {
//		super();
//		this.id = id;
//		this.blocks = blocks;
//		this.neighbors = neighbors;
//		this.isSource = isSource;
//		this.tokenTemporary = tokenTemporary;
//	}


	public NodeGraph(int id) {
		super();
		this.id = id;
		this.startTime = System.currentTimeMillis();
	}
	
	public NodeGraph() {
		super();
		this.id = -100;
		this.blocks = new HashSet<>();
		this.neighbors = new TreeSet<>();
		this.isSource = false;
		this.startTime = System.currentTimeMillis();
	}
	

	public boolean isSource() {
		return isSource;
	}


	public void setSource(boolean isSource) {
		this.isSource = isSource;
	}


	public int getId() {
		return id;
	}


	public void setId(int id) {
		this.id = id;
	}


	public Set<Integer> getBlocks() {
		return blocks;
	}


	public void setBlocks(Set<Integer> blocks) {
		this.blocks = blocks;
	}


	public Set<TupleSimilarity> getNeighbors() {
		return neighbors;
	}
	

//	public void setNeighbors(Set<Tuple2<Integer, Double>> neighbors) {
//		this.neighbors = neighbors;
//	}
	
//	public void addNeighbor(Tuple2<Integer, Double> neighbor) {
//		this.sumWeight += neighbor._2();
//		this.numberOfNeighbors ++;
//		this.neighbors.add(neighbor);
//	}
//	

	
	public void addNeighbor(TupleSimilarity neighbor) {
		this.neighbors.add(neighbor);
		if (neighbors.size() > maxNumberOfNeighbors) {
			neighbors.pollLast();
		}
	}
	
//	public void addAllNeighbor(NodeGraph n2) {
//		this.neighbors.addAll(n2.getNeighbors());
//	}
	
//	public void addAllNeighbor(Set<Tuple2<Integer, Double>> neighbors) {
//		for (Tuple2<Integer, Double> neighbor : neighbors) {
//			addNeighbor(neighbor);
//		}
//	}
	
	public void pruning() {
		double threshold = setThreshold();
		TreeSet<TupleSimilarity> prunnedNeighbors = new TreeSet<>();
		
		for (TupleSimilarity neighbor : neighbors) {
			if (neighbor.getValue() >= threshold) {
				prunnedNeighbors.add(neighbor);
			}
		}
		
		this.neighbors = prunnedNeighbors;
		
	}
	
	private double setThreshold() {
		double numberOfNeighbors = neighbors.size();
		double sumWeight = 0.0;
		
		for (TupleSimilarity neighbor : neighbors) {
			sumWeight += neighbor.getValue();
		}
		return sumWeight/numberOfNeighbors;
	}

	public boolean isMarked() {
		return marked;
	}


	public void setMarked(boolean marked) {
		this.marked = marked;
	}


	public Integer getToken() {
		return token;
	}


	public void setToken(Integer newToken) {
		this.token = newToken;
	}


	@Override
	public String toString() {
//		String output = id + ":";
		String output = "";
		for (TupleSimilarity neighbor : neighbors) {
			output += neighbor.getKey() + ",";
		}
		if (!neighbors.isEmpty()) {
			return output.substring(0,output.length()-1);
		} else {
			return output;
		}
		
	}

	public void setBlackFlag(boolean b) {
		this.blackFlag = b;
	}
	
	public boolean isBlackFlag() {
		return blackFlag;
	}

	public long getStartTime() {
		return startTime;
	}

	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}

	
	
}
