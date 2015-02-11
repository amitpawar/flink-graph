package flink.graphs.example;

import java.util.regex.Pattern;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;



import flink.graphs.Edge;
import flink.graphs.Vertex;
import flink.graphs.Graph;

public class GraphColouringExample {

	public static void main(String[] args) {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		DataSource<String> input = env.readTextFile("/add file path here or get from config file");

		DataSet<Vertex<Long, Long>> nodes = input.flatMap(new NodeReader()).distinct();
		
		DataSet<Edge<Long, Double>> edges = input.flatMap(new EdgeReader()).distinct();
		
		
		Graph<Long,Long,Double> graph = new Graph<Long,Long,Double>(nodes,edges,env);

	}

	@SuppressWarnings("serial")
	public static class EdgeReader implements
	FlatMapFunction<String, Edge<Long, Double>> {

		private static final Pattern SEPARATOR = Pattern.compile("[ \t,]");

		public void flatMap(String s,
				Collector<Edge<Long, Double>> collector)
						throws Exception {
			if (!s.startsWith("%")) {
				String[] tokens = SEPARATOR.split(s);

				long source = Long.parseLong(tokens[0]);
				long target = Long.parseLong(tokens[1]);

				collector.collect(new Edge<Long,Double>(source,target));
			}
		}
	}
	
	@SuppressWarnings("serial")
	public static class NodeReader implements
	FlatMapFunction<String, Vertex<Long, Long>> {

		private static final Pattern SEPARATOR = Pattern.compile("[ \t,]");

		public void flatMap(String s,
				Collector<Vertex<Long, Long>> collector)
						throws Exception {
			if (!s.startsWith("%")) {
				String[] tokens = SEPARATOR.split(s);

				long ctr = 0;
				long source = Long.parseLong(tokens[0]);
				long target = Long.parseLong(tokens[1]);

				collector.collect(new Vertex<Long, Long>(ctr++,source));
				collector.collect(new Vertex<Long, Long>(ctr++, target));
			}
		}
	}

}


