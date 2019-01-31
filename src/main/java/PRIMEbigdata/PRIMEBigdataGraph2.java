package PRIMEbigdata;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

import DataStructures.Attribute;
import DataStructures.ClusterGraph;
import DataStructures.EntityProfile;
import DataStructures.NodeGraph;
import DataStructures.TupleSimilarity;
import tokens.KeywordGenerator;
import tokens.KeywordGeneratorImpl;

//localhost:9092 localhost:2181 20 200 20 outputs/
public class PRIMEBigdataGraph2 {
	
	private IntCounter numLines = new IntCounter();
	
	public static void main(String[] args) throws Exception {
		
		
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", args[0]);
		// only required for Kafka 0.8
		properties.setProperty("zookeeper.connect", args[1]);
		properties.setProperty("group.id", "test");
		
		DataStream<String> lines = env.addSource(new FlinkKafkaConsumer("mytopic", new SimpleStringSchema(), properties));
		
		// the rebelance call is causing a repartitioning of the data so that all machines
		DataStream<EntityProfile> entities = lines.rebalance().map(s -> new EntityProfile(s));
		
		//Extract the token from the attribute values.
		DataStream<ClusterGraph> streamOfPairs = entities.rebalance().flatMap(new FlatMapFunction<EntityProfile, ClusterGraph>() {

			@Override
			public void flatMap(EntityProfile se, Collector<ClusterGraph> output) throws Exception {
				Set<Integer> cleanTokens = new HashSet<Integer>();

				for (Attribute att : se.getAttributes()) {
					KeywordGenerator kw = new KeywordGeneratorImpl();
					for (String string : generateTokens(att.getValue())) {
						cleanTokens.add(string.hashCode());
					}
				}
				
				if ((se.isSource() && se.getKey() == 30) || (!se.isSource() && se.getKey() == 426)) {
					System.out.println();
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

			private Set<String> generateTokens(String string) {
				Pattern p = Pattern.compile("[^a-zA-Z\\s0-9]");
				Matcher m = p.matcher("");
				m.reset(string);
				String standardString = m.replaceAll("");
				
				KeywordGenerator kw = new KeywordGeneratorImpl();
				return kw.generateKeyWords(standardString);
			}
		});
		
		
		//Applies the token as a key.
		WindowedStream<ClusterGraph, Integer, TimeWindow> entityBlocks = streamOfPairs.keyBy(new KeySelector<ClusterGraph, Integer>() {
			@Override
			public Integer getKey(ClusterGraph cluster) throws Exception {
				return cluster.getTokenkey();
			}
		}).timeWindow(Time.seconds(Integer.parseInt(args[3])), Time.seconds(Integer.parseInt(args[4])));//define the window
		
		
		//Group the entities with the same token (blocking using the token as a key).
		SingleOutputStreamOperator<ClusterGraph> graph = entityBlocks.reduce(new ReduceFunction<ClusterGraph>() {
			
			@Override
			public ClusterGraph reduce(ClusterGraph c1, ClusterGraph c2) throws Exception {
				c1.merge(c2);
				return c1;
			}
		});//.timeWindowAll(Time.seconds(Integer.parseInt(args[3])), Time.seconds(Integer.parseInt(args[4])));
		
		//Remove the blocks with a huge number of entities.
		SingleOutputStreamOperator<ClusterGraph> filterBlocking = graph.filter(new FilterFunction<ClusterGraph>() {
			
			@Override
			public boolean filter(ClusterGraph c) throws Exception {
				return c.size() < 100;
			}
		});
		
		//Generate a pair of entities with a number 1 associated. Summarizing, this step counts (sums) the occurrences of each entity pair. The higher (number of occurrences) the better (chances to be match).
		SingleOutputStreamOperator<Tuple2<String, Double>> entityPairs = filterBlocking.flatMap(new FlatMapFunction<ClusterGraph, Tuple2<String, Double>>() {

			@Override
			public void flatMap(ClusterGraph value, Collector<Tuple2<String, Double>> out) throws Exception {
				for (NodeGraph s : value.getEntitiesFromSource()) {
					for (NodeGraph t : value.getEntitiesFromTarget()) {
						if (s.getId() == 30 && t.getId() == 426) {
							System.out.println();
						}
						out.collect(new Tuple2<String, Double>(s.getId() + "-" + t.getId(), getDirectSimilarity()));//getDirectSimilarity(value.size())));
					}
				}
				
			}

			private Double getSensibleSimilarity(int size) {
				double sim = 1.0/(size/2);
				return sim;
			}
			
			private Double getDirectSimilarity() {
				return 1.0;
			}
		}).keyBy(0).timeWindow(Time.seconds(Integer.parseInt(args[3])), Time.seconds(Integer.parseInt(args[4]))).sum(1);//group by the tuple field "0" and sum up tuple field "1"
		
		//Generates a pair where the key is the entity from source.
		SingleOutputStreamOperator<NodeGraph> groupedGraph = entityPairs.map(new MapFunction<Tuple2<String,Double>, NodeGraph>() {

			@Override
			public NodeGraph map(Tuple2<String, Double> value) throws Exception {
				String[] pair = value.f0.split("-");
				NodeGraph node = new NodeGraph(Integer.parseInt(pair[0]), Integer.parseInt(args[2]));
				node.addNeighbor(new TupleSimilarity(Integer.parseInt(pair[1]), value.f1.doubleValue()));
				return node;
			}
		});
		
		//Groups the entities from target (neighbors) associated with a particular entity from source.
		WindowedStream<NodeGraph, Integer, TimeWindow> keyedGraph = groupedGraph.keyBy(new KeySelector<NodeGraph, Integer>() {
			@Override
			public Integer getKey(NodeGraph node) throws Exception {
				return node.getId();
			}
		}).timeWindow(Time.seconds(Integer.parseInt(args[3])), Time.seconds(Integer.parseInt(args[4])));//define the window;;
		
		
		SingleOutputStreamOperator<NodeGraph> prunedGraph = keyedGraph.reduce(new ReduceFunction<NodeGraph>() {
			
			@Override
			public NodeGraph reduce(NodeGraph n1, NodeGraph n2) throws Exception {
				for (TupleSimilarity neighbors2 : n2.getNeighbors()) {
					if (n1.getId() == 30 && neighbors2.getKey() == 426) {
						System.out.println();
					}
					n1.addNeighbor(neighbors2);
				}
				return n1;
			}
		});
		
		//Execute a pruning of the neighbors
		DataStreamSink<String> output = prunedGraph.rebalance().map(new MapFunction<NodeGraph, String>() {
			@Override
			public String map(NodeGraph node) throws Exception {
				node.pruningWNP();
				return node.getId() + ">" + node.toString();
			}
		}).writeAsText(args[5]);
		
		
		
		env.execute();
	}
}
