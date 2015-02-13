package flink.graphs.example;

import java.io.Serializable;
import java.util.regex.Pattern;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import flink.graphs.Edge;
import flink.graphs.Graph;
import flink.graphs.Vertex;
import flink.graphs.library.GraphColouring;

public class GraphColouringExample {

	private static int maxiteration;

	public static void main(String[] args) throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();
		maxiteration = 10;

		DataSource<String> input = env
				.readTextFile("/home/hung/aim3project.graph/src/test/resources/smallGraph/IMPROTest");

		DataSet<Vertex<Long, Long>> nodes = input.flatMap(new NodeReader())
				.distinct();

		DataSet<Edge<Long, NullValue>> edges = input.flatMap(new EdgeReader())
				.distinct();

		Graph<Long, Long, NullValue> graph = new Graph<Long, Long, NullValue>(nodes,
				edges, env);

		
		Graph<Long, Tuple3<Integer, Integer, Integer>, NullValue> graphInputMapped = graph
				.mapVertices(new InitVerticesMapper());

		graphInputMapped.getEdges().print();
//		DeltaIteration<Graph<Long, Tuple3<Integer, Integer, Integer>, NullValue>, Graph<Long, Tuple3<Integer, Integer, Integer>, NullValue>> 
//		iteration = graphInputMapped.
		        //verticesWithInitialId.iterateDelta(verticesWithInitialId, maxIterations, 0);

		
		
		int colour = 0;
		Graph<Long, Tuple3<Integer, Integer, Integer>, NullValue> graphFiltered = graphInputMapped;
		while (colour < 4) {
			GraphColouring algorithm = new GraphColouring(maxiteration, colour);

			
			Graph<Long, Tuple3<Integer, Integer, Integer>, NullValue> resultGraph = graphFiltered
					.run(algorithm);
			graphFiltered = resultGraph.filterOnVertices(new FilterVertex());

			DataSet<Vertex<Long, Tuple3<Integer, Integer, Integer>>> result = graphFiltered
					.getVertices();
			result.print();
			//graphFiltered = resultGraph; // filtered

			env.execute();
			
			// check if the filtered result is empty, if so, break
			colour++;
			System.out.println("Colours:" + colour);
		}
		
	}

	@SuppressWarnings("serial")
	public static final class FilterVertex implements
			FilterFunction<Tuple3<Integer, Integer, Integer>> {

		@Override
		public boolean filter(Tuple3<Integer, Integer, Integer> value) throws Exception {
			return value.f0 == -1;
		}
	}

	@SuppressWarnings("serial")
	public static class EdgeReader implements
			FlatMapFunction<String, Edge<Long, NullValue>> {

		private static final Pattern SEPARATOR = Pattern.compile("[ \t,]");

		public void flatMap(String s, Collector<Edge<Long, NullValue>> collector)
				throws Exception {
			if (!s.startsWith("%")) {
				String[] tokens = SEPARATOR.split(s);

				long source = Long.parseLong(tokens[0]);
				long target = Long.parseLong(tokens[1]);

				collector.collect(new Edge<Long, NullValue>(source, target));
			}
		}
	}

	public static final class InitVerticesMapper<K extends Comparable<K> & Serializable>
			implements
			MapFunction<Vertex<K, Double>, Tuple3<Integer, Integer, Integer>> {

		public Tuple3<Integer, Integer, Integer> map(Vertex<K, Double> value) {

			return new Tuple3<Integer, Integer, Integer>(-1, -1, -1);
		}
	}

	@SuppressWarnings("serial")
	public static class NodeReader implements
			FlatMapFunction<String, Vertex<Long, Long>> {

		private static final Pattern SEPARATOR = Pattern.compile("[ \t,]");

		public void flatMap(String s, Collector<Vertex<Long, Long>> collector)
				throws Exception {
			if (!s.startsWith("%")) {
				String[] tokens = SEPARATOR.split(s);

				long ctr = 0;
				long source = Long.parseLong(tokens[0]);
				long target = Long.parseLong(tokens[1]);

				collector.collect(new Vertex<Long, Long>(ctr++, source));
				collector.collect(new Vertex<Long, Long>(ctr++, target));
			}
		}
	}

}
