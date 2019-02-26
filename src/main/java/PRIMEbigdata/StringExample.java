package PRIMEbigdata;

import java.util.Properties;

import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

//localhost:9092 localhost:2181 20 200 20 outputs/
public class StringExample {
	
	private IntCounter numLines = new IntCounter();
	
	public static void main(String[] args) throws Exception {
		
		
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
//		StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("flink-master", 8081, "/home/user/udfs.jar");
		env.setParallelism(Integer.parseInt(args[6]));
//		ExecutionEnvironment env = ExecutionEnvironment
//		        .createRemoteEnvironment("flink-master", 8081, "/home/user/udfs.jar");

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", args[0]);
		// only required for Kafka 0.8
		properties.setProperty("zookeeper.connect", args[1]);
		properties.setProperty("group.id", "test");
		
		DataStream<String> lines = env.addSource(new FlinkKafkaConsumer011<String>("mytopic", new SimpleStringSchema(), properties));
		
		lines.print();
		
		// the rebelance call is causing a repartitioning of the data so that all machines
//		DataStream<EntityProfile> entities = lines.rebalance().map(s -> new EntityProfile(s));
		
		//Extract the token from the attribute values.
//		DataStream<Tuple2<Integer, String>> streamOfPairs = lines.rebalance().flatMap(new FlatMapFunction<String, Tuple2<Integer, String>>() {
//
//			@Override
//			public void flatMap(String s, Collector<Tuple2<Integer, String>> output) throws Exception {
//				Set<Integer> cleanTokens = new HashSet<Integer>();
//				EntityProfile se = new EntityProfile(s);
//
//				for (Attribute att : se.getAttributes()) {
//					KeywordGenerator kw = new KeywordGeneratorImpl();
//					for (String string : generateTokens(att.getValue())) {
//						cleanTokens.add(string.hashCode());
//					}
//				}
//				
////				if ((se.isSource() && se.getKey() == 514) || (!se.isSource() && se.getKey() == 970)) {
////					System.out.println();
////				}
//
//				for (Integer tk : cleanTokens) {
//					output.collect(new Tuple2<Integer, String>(tk, se.getKey()+"#"+se.isSource()));
////					ClusterGraph cluster = new ClusterGraph(tk, se.getIncrementID());
////					if (se.isSource()) {
////						cluster.addInSource(new NodeGraph(tk, se.getKey(), cleanTokens, se.isSource(), Integer.parseInt(args[2]), true, se.getIncrementID()));
////					} else {
////						cluster.addInTarget(new NodeGraph(tk, se.getKey(), cleanTokens, se.isSource(), Integer.parseInt(args[2]), true, se.getIncrementID()));
////					}
////					output.collect(cluster);
//				}
//			}
//
//			private Set<String> generateTokens(String string) {
//				Pattern p = Pattern.compile("[^a-zA-Z\\s0-9]");
//				Matcher m = p.matcher("");
//				m.reset(string);
//				String standardString = m.replaceAll("");
//				
//				KeywordGenerator kw = new KeywordGeneratorImpl();
//				return kw.generateKeyWords(standardString);
//			}
//		});
//		
//		
//		//Applies the token as a key.
//		WindowedStream<Tuple2<Integer, String>, Tuple, TimeWindow> entityBlocks = streamOfPairs.keyBy(0)
//				.timeWindow(Time.seconds(Integer.parseInt(args[3])), Time.seconds(Integer.parseInt(args[4])));//define the window
//		
//		SingleOutputStreamOperator<Tuple2<Integer, String>> blocks = entityBlocks.reduce(new ReduceFunction<Tuple2<Integer,String>>() {
//			
//			@Override
//			public Tuple2<Integer, String> reduce(Tuple2<Integer, String> value1, Tuple2<Integer, String> value2)
//					throws Exception {
//				return new Tuple2<Integer, String>(value1.f0, value1.f1 + "<>" + value2.f1);
//			}
//		});
//		
//		blocks.flatMap(new FlatMapFunction<Tuple2<Integer,String>, Tuple2<String,Double>>() {
//
//			@Override
//			public void flatMap(Tuple2<Integer, String> value, Collector<Tuple2<String, Double>> out) throws Exception {
//				String[] entities = value.f1.split("<>");
//				if (entities.length > 1) {
//					for (int i = 0; i < entities.length; i++) {
//						for (int j = i+1; j < entities.length; j++) {
//							String[] e1 = entities[i].split("#");
//							String[] e2 = entities[j].split("#");
//							if (!e1[1].equalsIgnoreCase(e2[1])) {
//								out.collect(new Tuple2<String, Double>(e1[0]+"-"+e2[0], 1.0));
//							}
//						}
//					}
//				}
//				
//			}
//		}).keyBy(0)/*.timeWindow(Time.seconds(Integer.parseInt(args[3])), Time.seconds(Integer.parseInt(args[4])))*/.sum(1).print();
		
		
		
		
		
		
//		//Group the entities with the same token (blocking using the token as a key).
//		SingleOutputStreamOperator<ClusterGraph> graph = entityBlocks.reduce(new ReduceFunction<ClusterGraph>() {
//			
//			@Override
//			public ClusterGraph reduce(ClusterGraph c1, ClusterGraph c2) throws Exception {
//				c1.merge2(c2);
//				return c1;
//			}
//		});//.timeWindowAll(Time.seconds(Integer.parseInt(args[3])), Time.seconds(Integer.parseInt(args[4])));
//		
//		//Remove the blocks with a huge number of entities.
//		SingleOutputStreamOperator<ClusterGraph> filterBlocking = graph.filter(new FilterFunction<ClusterGraph>() {
//			
//			@Override
//			public boolean filter(ClusterGraph c) throws Exception {
//				return true;
////				return c.size() < 200;
//			}
//		});
//		
//		//Generate a pair of entities with a number 1 associated. Summarizing, this step counts (sums) the occurrences of each entity pair. The higher (number of occurrences) the better (chances to be match).
//		SingleOutputStreamOperator<Tuple2<String, Double>> entityPairs = graph.flatMap(new FlatMapFunction<ClusterGraph, Tuple2<String, Double>>() {
//
//			@Override
//			public void flatMap(ClusterGraph value, Collector<Tuple2<String, Double>> out) throws Exception {
//				for (NodeGraph s : value.getEntitiesFromSource()) {
//					for (NodeGraph t : value.getEntitiesFromTarget()) {
////						if (s.getId() == 514 && t.getId() == 970) {
////							System.out.println();
////						}
////						if (s.getIncrement() == value.getCurrentIncrement() || t.getIncrement() == value.getCurrentIncrement()) {
//							out.collect(new Tuple2<String, Double>(s.getId() + "-" + t.getId(), getPartialySimilarity(s.getBlocks().size(), t.getBlocks().size())));//getSensibleSimilarity(value.size())));//getDirectSimilarity()));
////						}
//					}
//				}
//				
//			}
//
//			private Double getPartialySimilarity(int sizeS, int sizeT) {
//				return 1.0/(Math.min(sizeS, sizeT));//ti's possible use max() also, depending the strategy.
//			}
//
//			private Double getSensibleSimilarity(int size) {
//				double sim = 1.0/(size/2);
//				return sim;
//			}
//			
//			private Double getDirectSimilarity() {
//				return 1.0;
//			}
//		}).keyBy(0).timeWindow(Time.seconds(Integer.parseInt(args[3])), Time.seconds(Integer.parseInt(args[4]))).sum(1);//group by the tuple field "0" and sum up tuple field "1"
//		
//		//Generates a pair where the key is the entity from source.
//		SingleOutputStreamOperator<NodeGraph> groupedGraph = entityPairs.map(new MapFunction<Tuple2<String,Double>, NodeGraph>() {
//
//			@Override
//			public NodeGraph map(Tuple2<String, Double> value) throws Exception {
//				String[] pair = value.f0.split("-");
//				NodeGraph node = new NodeGraph(Integer.parseInt(pair[0]), Integer.parseInt(args[2]));
////				if (Integer.parseInt(pair[0]) == 514 && Integer.parseInt(pair[1]) == 970) {
////					System.out.println();
////				}
//				node.addNeighbor(new TupleSimilarity(Integer.parseInt(pair[1]), value.f1.doubleValue()));
//				return node;
//			}
//		});
//		
//		//Groups the entities from target (neighbors) associated with a particular entity from source.
//		WindowedStream<NodeGraph, Integer, TimeWindow> keyedGraph = groupedGraph.keyBy(new KeySelector<NodeGraph, Integer>() {
//			@Override
//			public Integer getKey(NodeGraph node) throws Exception {
//				return node.getId();
//			}
//		}).timeWindow(Time.seconds(Integer.parseInt(args[3])), Time.seconds(Integer.parseInt(args[4])));//define the window;;
//		
//		
//		SingleOutputStreamOperator<NodeGraph> prunedGraph = keyedGraph.reduce(new ReduceFunction<NodeGraph>() {
//			
//			@Override
//			public NodeGraph reduce(NodeGraph n1, NodeGraph n2) throws Exception {
//				for (TupleSimilarity neighbors2 : n2.getNeighbors()) {
////					if (n1.getId() == 514 /*&& neighbors2.getKey() == 970*/) {
////						System.out.println();
////					}
//					n1.addNeighbor(neighbors2);
//				}
//				return n1;
//			}
//		});
//		
//		//Execute a pruning of the neighbors
//		DataStreamSink<String> output = prunedGraph.rebalance().map(new MapFunction<NodeGraph, String>() {
//			@Override
//			public String map(NodeGraph node) throws Exception {
////				if (node.getId() == 30) {
////					System.out.println();
////				}
//				node.pruningWNP();
//				return node.getId() + ">" + node.toString();
//			}
//		}).writeAsText(args[5]);
		
		
		
		env.execute();
	}
}
