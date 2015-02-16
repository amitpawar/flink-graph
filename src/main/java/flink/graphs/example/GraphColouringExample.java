package flink.graphs.example;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.regex.Pattern;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.RemoteCollectorConsumer;
import org.apache.flink.api.java.io.RemoteCollectorImpl;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import flink.graphs.Edge;
import flink.graphs.Graph;
import flink.graphs.Vertex;
import flink.graphs.library.GraphColouring;

public class GraphColouringExample {
	
	private static String argPathToArc = "";
	private static String argPathOut = "";
	private static int maxiteration;
	
	public static void main(String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}
		final ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();

		maxiteration = 10;
		
		DataSource<String> input = env.readTextFile(argPathToArc);

		DataSet<Vertex<Long, Tuple4<Integer, Integer, Integer, Integer>>> nodes = input
				.flatMap(new NodeReader()).distinct();

		DataSet<Edge<Long, NullValue>> edges = input.flatMap(new EdgeReader())
				.distinct();

		Graph<Long, Tuple4<Integer, Integer, Integer, Integer>, NullValue> graph = new Graph<Long, Tuple4<Integer, Integer, Integer, Integer>, NullValue>(
				nodes, edges, env);

		int colour = 0;
		Graph<Long, Tuple4<Integer, Integer, Integer, Integer>, NullValue> graphFiltered = graph;

		

		int edgesRemaining = 0;

		do {
			System.out.println("Colours:" + colour);
			GraphColouring<Long> algorithm = new GraphColouring<Long>(
					maxiteration, colour);

			
			Graph<Long, Tuple4<Integer, Integer, Integer, Integer>, NullValue> resultGraph = graphFiltered
					.run(algorithm);
			System.out.println("ResultGraph");
			

			graphFiltered = resultGraph.filterOnVertices(new FilterVertex());
			Graph<Long, Tuple4<Integer, Integer, Integer, Integer>, NullValue> colourGraph = resultGraph
					.filterOnVertices(new FilterNonColourVertex());

			DataSet<Integer> num = graphFiltered.numberOfEdges();
			ArrayList<Integer> collection = new ArrayList<Integer>();
			
			RemoteCollectorImpl.collectLocal(num, collection);
			
			
			colourGraph.getVertices().writeAsCsv(argPathOut + colour,
					WriteMode.OVERWRITE);

			env.execute("Third build colour " + colour);
			edgesRemaining = collection.get(0);

		

			System.out.println("Edges remaining: " + edgesRemaining
					+ " Colour: " + colour);
			colour++;
			
			RemoteCollectorImpl.shutdownAll();
			System.gc();
			// check if the filtered result is empty, if so, break colour++;
		} while (edgesRemaining != 0);

		DataSet<Tuple2<Long, Long>> degrees = graphFiltered.getDegrees();
		graphFiltered = graphFiltered.joinWithVertices(degrees,
				new ColourIsolatedNodes<Long>(colour));
		
		graphFiltered.getVertices().writeAsCsv(argPathOut + colour,
				WriteMode.OVERWRITE);
		env.execute("First build colour " + colour);

		RemoteCollectorImpl.shutdownAll();
	}

	@SuppressWarnings("serial")
	public static final class ColourIsolatedNodes<K extends Comparable<K> & Serializable>
			implements
			MapFunction<Tuple2<Tuple4<Integer, Integer, Integer, Integer>, Long>, Tuple4<Integer, Integer, Integer, Integer>> {

		private Integer colour;

		public ColourIsolatedNodes(Integer colour) {
			this.colour = colour;
		}

		@Override
		public Tuple4<Integer, Integer, Integer, Integer> map(
				Tuple2<Tuple4<Integer, Integer, Integer, Integer>, Long> value)
				throws Exception {
			if (value.f1.longValue() == 0) {
				value.f0.f0 = colour;
				value.f0.f3 = 0;
			}
			return value.f0;
		}

	}

	@SuppressWarnings("serial")
	public static final class FilterVertex implements
			FilterFunction<Tuple4<Integer, Integer, Integer, Integer>> {

		@Override
		public boolean filter(Tuple4<Integer, Integer, Integer, Integer> value)
				throws Exception {
			// TODO Auto-generated method stub
			return value.f0 == -1;
		}
	}

	@SuppressWarnings("serial")
	public static final class FilterNonColourVertex implements
			FilterFunction<Tuple4<Integer, Integer, Integer, Integer>> {

		@Override
		public boolean filter(Tuple4<Integer, Integer, Integer, Integer> value)
				throws Exception {
			return value.f0 != -1;
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

				collector.collect(new Edge<Long, NullValue>(source, target,
						new NullValue()));
				collector.collect(new Edge<Long, NullValue>(target, source,
						new NullValue()));
			}
		}
	}

	@SuppressWarnings("serial")
	public static class NodeReader
			implements
			FlatMapFunction<String, Vertex<Long, Tuple4<Integer, Integer, Integer, Integer>>> {

		private static final Pattern SEPARATOR = Pattern.compile("[ \t,]");

		public void flatMap(
				String s,
				Collector<Vertex<Long, Tuple4<Integer, Integer, Integer, Integer>>> collector)
				throws Exception {
			if (!s.startsWith("%")) {
				String[] tokens = SEPARATOR.split(s);
				
				long source = Long.parseLong(tokens[0]);
				long target = Long.parseLong(tokens[1]);

				collector
						.collect(new Vertex<Long, Tuple4<Integer, Integer, Integer, Integer>>(
								source,
								new Tuple4<Integer, Integer, Integer, Integer>(
										-1, -1, -1, -1)));
				collector
						.collect(new Vertex<Long, Tuple4<Integer, Integer, Integer, Integer>>(
								target,
								new Tuple4<Integer, Integer, Integer, Integer>(
										-1, -1, -1, -1)));
			}
		}
	}

	public static boolean parseParameters(String[] args) {

		if (args.length < 3 || args.length > 3) {
			System.err
					.println("Usage: [path to arc file] [output path] [maxIterations]");
			return false;
		}

		argPathToArc = args[0];		
		argPathOut = args[1];
		maxiteration = Integer.parseInt(args[2]);

		return true;
	}

}
