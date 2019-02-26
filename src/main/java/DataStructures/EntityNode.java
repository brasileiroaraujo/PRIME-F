package DataStructures;

import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.math3.stat.descriptive.moment.Variance;

import scala.Tuple2;

public class EntityNode {
	private Integer token;
	private int id;
	private Set<Integer> blocks;
	private TreeSet<TupleSimilarity> neighbors;
	private boolean isSource;
	private int maxNumberOfNeighbors;
	private int incrementId;
	
	
	public EntityNode(int token, int id, Set<Integer> blocks, boolean isSource, int maxNumberOfNeighbors, int incrementId) {
		super();
		this.token = token;
		this.id = id;
		this.blocks = blocks;
		this.neighbors = new TreeSet<TupleSimilarity>();
		this.isSource = isSource;
		this.maxNumberOfNeighbors = maxNumberOfNeighbors;
		this.incrementId = incrementId;
	}
	
	public EntityNode() {
		super();
		this.id = -100;
		this.blocks = new HashSet<>();
		this.neighbors = new TreeSet<>();
		this.isSource = false;
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
	public void addAllNeighbors(Set<TupleSimilarity> neighborsInput) {
		this.neighbors.addAll(neighborsInput);
		while (neighbors.size() > maxNumberOfNeighbors) {
			neighbors.pollLast();
		}
	}
	
	public void addNeighbor(TupleSimilarity neighbor) {
//		if (this.neighbors.contains(neighbor)) {
//			this.neighbors.remove(neighbor);
//			this.neighbors.add(neighbor);
//		} else {
//			this.neighbors.add(neighbor);
//		}
		if (!this.neighbors.contains(neighbor)) {
			this.neighbors.add(neighbor);
		}
		
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
	
	public void pruningWNP() {
		double threshold = getMeanDistribution();
		TreeSet<TupleSimilarity> prunnedNeighbors = new TreeSet<>();
		
		for (TupleSimilarity neighbor : neighbors) {
			if (neighbor.getValue() >= threshold) {
				prunnedNeighbors.add(neighbor);
			}
		}
		
		this.neighbors = prunnedNeighbors;
		
	}
	
	public void pruningOutliers() {
		Tuple2<Double, Double> meanAndDP = getMeanAndDPDistribution();
		double threshold = meanAndDP._1() - meanAndDP._2();
		TreeSet<TupleSimilarity> prunnedNeighbors = new TreeSet<>();
		
		for (TupleSimilarity neighbor : neighbors) {
			if (neighbor.getValue() >= threshold) {
				prunnedNeighbors.add(neighbor);
			}
		}
		
		this.neighbors = prunnedNeighbors;
		
	}
	
	private Tuple2<Double, Double> getMeanAndDPDistribution() {
		double numberOfNeighbors = neighbors.size();
		double sumWeight = 0.0;
		double maxSim = -1;
		double[] values = new double[neighbors.size()];
		int index = 0;
		
		for (TupleSimilarity neighbor : neighbors) {
			sumWeight += neighbor.getValue();
			values[index] = neighbor.getValue();
			if (neighbor.getValue() > maxSim) {
				maxSim = neighbor.getValue();
			}
		}
		
		return new Tuple2<Double, Double>((sumWeight/numberOfNeighbors), Math.sqrt(new Variance().evaluate(values)));
	}

	private double getMeanDistribution() {
		double numberOfNeighbors = neighbors.size();
		double sumWeight = 0.0;
		
		for (TupleSimilarity neighbor : neighbors) {
			sumWeight += neighbor.getValue();
		}
		return sumWeight/numberOfNeighbors;
	}
	
	private Tuple2<Double, Double> getMeanAndMaxDistribution() {
		double numberOfNeighbors = neighbors.size();
		double sumWeight = 0.0;
		double maxSim = -1;
		
		for (TupleSimilarity neighbor : neighbors) {
			sumWeight += neighbor.getValue();
			if (neighbor.getValue() > maxSim) {
				maxSim = neighbor.getValue();
			}
		}
		return new Tuple2<Double, Double>((sumWeight/numberOfNeighbors), maxSim);
	}

	public Integer getToken() {
		return token;
	}


	public void setToken(Integer newToken) {
		this.token = newToken;
	}


	public void setMaxNumberOfNeighbors(int maxNumberOfNeighbors) {
		this.maxNumberOfNeighbors = maxNumberOfNeighbors;
	}

	public int getIncrementId() {
		return incrementId;
	}

	public void setIncrementId(int incrementId) {
		this.incrementId = incrementId;
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

}
