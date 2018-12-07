package PRIMEbigdata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer082;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

import DataStructures.Attribute;
import DataStructures.Cluster;
import DataStructures.ClusterGraph;
import DataStructures.EntityProfile;
import DataStructures.Node;
import DataStructures.NodeGraph;
import DataStructures.TupleSimilarity;
import scala.Tuple2;
import tokens.KeywordGenerator;
import tokens.KeywordGeneratorImpl;

public class PRIMEBigdataGraph {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", args[0]);
		// only required for Kafka 0.8
		properties.setProperty("zookeeper.connect", args[1]);
		properties.setProperty("group.id", "test");
		
		DataStream<String> lines = env.addSource(new FlinkKafkaConsumer082<>("mytopic", new SimpleStringSchema(), properties));
		
		// the rebelance call is causing a repartitioning of the data so that all machines
		DataStream<EntityProfile> entities = lines.rebalance().map(s -> new EntityProfile((String) s));
		
		DataStream<ClusterGraph> streamOfPairs = entities.rebalance().flatMap(new FlatMapFunction<EntityProfile, ClusterGraph>() {

			@Override
			public void flatMap(EntityProfile se, Collector<ClusterGraph> output) throws Exception {
				Set<Integer> cleanTokens = new HashSet<Integer>();

				for (Attribute att : se.getAttributes()) {
					KeywordGenerator kw = new KeywordGeneratorImpl();
					for (String string : kw.generateKeyWords(att.getValue())) {
						cleanTokens.add(string.hashCode());
					}
				}

				for (Integer tk : cleanTokens) {
					ClusterGraph cluster = new ClusterGraph(tk);
					if (se.isSource()) {
						cluster.addInSource(new NodeGraph(tk, se.getKey(), cleanTokens, se.isSource(), Integer.parseInt(args[2]), true));
					} else {
						cluster.addInTarget(new NodeGraph(tk, se.getKey(), cleanTokens, se.isSource(), Integer.parseInt(args[2]), true));
					}
					output.collect(cluster);
				}
			}
		});
		
		
		KeyedStream<ClusterGraph, Integer> entityBlocks = streamOfPairs.keyBy(new KeySelector<ClusterGraph, Integer>() {
			@Override
			public Integer getKey(ClusterGraph cluster) throws Exception {
				return cluster.getTokenkey();
			}
		});//.timeWindow(Time.seconds(Integer.parseInt(args[2])), Time.seconds(Integer.parseInt(args[3])));//define the window
		
		
		AllWindowedStream<ClusterGraph, TimeWindow> graph = entityBlocks.reduce(new ReduceFunction<ClusterGraph>() {
			
			@Override
			public ClusterGraph reduce(ClusterGraph c1, ClusterGraph c2) throws Exception {
				c1.merge(c2);
				return c1;
			}
		}).timeWindowAll(Time.seconds(Integer.parseInt(args[3])), Time.seconds(Integer.parseInt(args[4])));
		
		SingleOutputStreamOperator<NodeGraph> groupedGraph = graph.apply(new AllWindowFunction<ClusterGraph, NodeGraph, TimeWindow>() {

			@Override
			public void apply(TimeWindow window, Iterable<ClusterGraph> values, Collector<NodeGraph> out)
					throws Exception {
				for (ClusterGraph cluster : values) {
					for (NodeGraph sourceNodes : cluster.getEntitiesFromSource()) {
						out.collect(sourceNodes);
					}
				}
				
			}
		});
		
		
		SingleOutputStreamOperator<NodeGraph> prunedGraph = groupedGraph.keyBy(new KeySelector<NodeGraph, Integer>() {
			@Override
			public Integer getKey(NodeGraph node) throws Exception {
				return node.getId();
			}
		}).reduce(new ReduceFunction<NodeGraph>() {
			
			@Override
			public NodeGraph reduce(NodeGraph n1, NodeGraph n2) throws Exception {
				for (TupleSimilarity neighbors2 : n2.getNeighbors()) {
					n1.addNeighbor(neighbors2);
				}
				return n1;
			}
		});
		
		
		DataStreamSink<String> output = prunedGraph.rebalance().map(new MapFunction<NodeGraph, String>() {
			@Override
			public String map(NodeGraph node) throws Exception {
				return node.getId() + ">" + node.toString();
			}
		}).print();//.writeAsText(args[5]);
		
		
//		SingleOutputStreamOperator<Cluster> entityClusters = entityBlocks.reduce(new ReduceFunction<Cluster>() {
//			
//			@Override
//			public Cluster reduce(Cluster c1, Cluster c2) throws Exception {
//				if (c1.getLastIncrementID() > c2.getLastIncrementID()) {
//					c1.mergeCollections();
//					c2.markEntities();
//				} else if (c1.getLastIncrementID() < c2.getLastIncrementID()) {
//					c2.mergeCollections();
//					c1.markEntities();
//				} else {
//					c1.mergeCollections();
//					c2.mergeCollections();
//				}
//				c1.addAllNewCollection(c2.getNewCollection());
//				c1.addAllCollection(c2.getCollection());
//				c1.setLastIncrementID(Math.max(c1.getLastIncrementID(), c2.getLastIncrementID()));
//				return c1;
//			}
//			
//		});
//		
//		
//		SingleOutputStreamOperator<Node> pairEntityBlock = entityClusters.rebalance().flatMap(new FlatMapFunction<Cluster, Node>() {
//
//			@Override
//			public void flatMap(Cluster value, Collector<Node> out) throws Exception {
//				ArrayList<Node> entitiesToCompare = new ArrayList<Node>(value.getCollection());
//				ArrayList<Node> comparedEntites = new ArrayList<Node>();
//				Map<Integer, Node> outputMap = new HashMap<Integer, Node>();
//				
//				for (int i = 0; i < entitiesToCompare.size(); i++) {
//					Node n1 = entitiesToCompare.get(i);
//					for (int j = i+1; j < entitiesToCompare.size(); j++) {
//						Node n2 = entitiesToCompare.get(j);
//						//Only compare nodes from distinct sources and marked as new (avoid recompute comparisons)
//						if (n1.isSource() != n2.isSource() && (n1.isMarked() || n2.isMarked())) {
//							double similarity = calculateSimilarity(n1.getToken(), n1.getBlocks(), n2.getBlocks());
//							if (similarity >= 0) {
////								numberOfComparisons.add(1);
//								if (n1.isSource()) {
//									if (outputMap.containsKey(n1.getId())) {
//										outputMap.get(n1.getId()).addNeighbor(new Tuple2<Integer, Double>(n2.getId(), similarity));
//									} else {
//										n1.addNeighbor(new Tuple2<Integer, Double>(n2.getId(), similarity));
//										outputMap.put(n1.getId(), n1);
//									}
//								} else {
//									if (outputMap.containsKey(n2.getId())) {
//										outputMap.get(n2.getId()).addNeighbor(new Tuple2<Integer, Double>(n1.getId(), similarity));
//									} else {
//										n2.addNeighbor(new Tuple2<Integer, Double>(n1.getId(), similarity));
//										outputMap.put(n2.getId(), n2);
//									}
//								}
//							}
//						}
//					}
//				}
//				
//				
//				for (Node n : outputMap.values()) {
//					n.setMarked(false);
//					out.collect(n);
//				}
//				
//			}
//			
//			private double calculateSimilarity(Integer blockKey, Set<Integer> ent1, Set<Integer> ent2) {
//				int maxSize = Math.max(ent1.size() - 1, ent2.size() - 1);
//				Set<Integer> intersect = new HashSet<Integer>(ent1);
//				intersect.retainAll(ent2);
//
//				// MACOBI strategy
//				if (!Collections.min(intersect).equals(blockKey)) {
//					return -1;
//				}
//
//				if (maxSize > 0) {
//					double x = (double) intersect.size() / maxSize;
//					return x;
//				} else {
//					return 0;
//				}
//			}
//		});
//		
//		
//		WindowedStream<Node, Integer, TimeWindow> nodeKeyed = pairEntityBlock.keyBy(new KeySelector<Node, Integer>() {
//			@Override
//			public Integer getKey(Node node) throws Exception {
//				return node.getId();
//			}
//		}).timeWindow(Time.seconds(Integer.parseInt(args[2])), Time.seconds(Integer.parseInt(args[3])));//define the window;
//		
//		
//		SingleOutputStreamOperator<Node> nodesGrouped = nodeKeyed.reduce(new ReduceFunction<Node>() {
//
//			@Override
//			public Node reduce(Node n1, Node n2) throws Exception {
//				n1.addAllNeighbor(n2);
//				return n1;//new Node(n1.getToken(), n1.getId(), n1.getBlocks(), n1.getNeighbors(), n1.isSource());
//			}
//		});
//		
//		
//		SingleOutputStreamOperator<String> prunedGraph = nodesGrouped.rebalance().map(new MapFunction<Node, String>() {
//
//			@Override
//			public String map(Node node) throws Exception {
//				node.pruning();
//				return node.getId() + ">" + node.toString();
//			}
//		});
//		
//		DataStreamSink<String> prunedBlocks = prunedGraph.rebalance().filter(new FilterFunction<String>() {
//			
//			@Override
//			public boolean filter(String value) throws Exception {
//				if (value.split(">").length > 1) {
//					return true;
//				}
//				return false;
//			}
//		}).writeAsText(args[4]);
		
		
		env.execute();
	}
}
