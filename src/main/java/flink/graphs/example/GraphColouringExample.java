package flink.graphs.example;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.regex.Pattern;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.RemoteCollectorConsumer;
import org.apache.flink.api.java.io.RemoteCollectorImpl;
import org.apache.flink.api.java.io.RemoteCollectorOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import flink.graphs.Edge;
import flink.graphs.Graph;
import flink.graphs.Vertex;
import flink.graphs.library.GraphColouring;

public class GraphColouringExample {

	private static int maxiteration;

	public static void main(String[] args) throws Exception {

		// ExecutionEnvironment env =
		// ExecutionEnvironment.getExecutionEnvironment();
		ExecutionEnvironment env = ExecutionEnvironment
				.createLocalEnvironment();

		maxiteration = 10;

		// Id cannot be 0
		DataSource<String> input = env.readTextFile("/Users/dgll/graph6");

		DataSet<Vertex<Long, Tuple4<Integer, Integer, Integer, Integer>>> nodes = input
				.flatMap(new NodeReader()).distinct();

		DataSet<Edge<Long, NullValue>> edges = input.flatMap(new EdgeReader())
				.distinct();

		Graph<Long, Tuple4<Integer, Integer, Integer, Integer>, NullValue> graph = new Graph<Long, Tuple4<Integer, Integer, Integer, Integer>, NullValue>(
				nodes, edges, env);

		// Graph<Long, Tuple3<Integer, Integer, Integer>, NullValue>
		// graphInputMapped = graph
		// .mapVertices(new InitVerticesMapper());

		// graph.getEdges().print();
		// DeltaIteration<Graph<Long, Tuple3<Integer, Integer, Integer>,
		// NullValue>, Graph<Long, Tuple3<Integer, Integer, Integer>,
		// NullValue>>
		// iteration = graphInputMapped.
		// verticesWithInitialId.iterateDelta(verticesWithInitialId,
		// maxIterations, 0);

		int colour = 0;

		// GraphColouring algorithm = new GraphColouring(maxiteration, colour);
		// Graph<Long, Tuple3<Integer, Integer, Integer>, NullValue> resultGraph
		// = graph
		// .run(algorithm);

		// resultGraph.getVertices().print();
		// env.execute();

		Graph<Long, Tuple4<Integer, Integer, Integer, Integer>, NullValue> graphFiltered = graph;
		/*
		 * while (colour < 4) { System.out.println("Colours:" + colour);
		 * GraphColouring<Long> algorithm = new
		 * GraphColouring<Long>(maxiteration, colour); DataSet<Tuple2<Long,
		 * Long>> degrees = graphFiltered.getDegrees(); graphFiltered =
		 * graphFiltered.joinWithVertices(degrees, new
		 * ColourIsolatedNodes<Long>(colour));
		 * System.out.println("ColourIsolatedNodes");
		 * graphFiltered.getVertices().print();
		 * env.execute("First build colour " + colour);
		 * //graphFiltered.mapVertices(new ColourIsolatedNodes<Long>(colour));
		 * Graph<Long, Tuple4<Integer, Integer, Integer, Integer>, NullValue>
		 * resultGraph = graphFiltered.run(algorithm);
		 * System.out.println("ResultGraph"); resultGraph.getVertices().print();
		 * env.execute("Second build colour " + colour); graphFiltered =
		 * resultGraph.filterOnVertices(new FilterVertex());
		 * 
		 * DataSet<Vertex<Long, Tuple4<Integer, Integer, Integer, Integer>>>
		 * result = graphFiltered.getVertices();
		 * System.out.println("FilteredGraph"); result.print(); //graphFiltered
		 * = resultGraph; // filtered env.execute("Third build colour " +
		 * colour);
		 * 
		 * // check if the filtered result is empty, if so, break colour++; }
		 */

		int verticesRemaining = 0;

		do {
			System.out.println("Colours:" + colour);
			GraphColouring<Long> algorithm = new GraphColouring<Long>(
					maxiteration, colour);

			/*
			 * DataSet<Tuple2<Long, Long>> degrees = graphFiltered.getDegrees();
			 * graphFiltered = graphFiltered.joinWithVertices(degrees, new
			 * ColourIsolatedNodes<Long>(colour));
			 * System.out.println("ColourIsolatedNodes");
			 * //graphFiltered.getVertices().print();
			 * graphFiltered.getVertices()
			 * .writeAsCsv("/Users/dgll/IT4BI/IMPRO3/secondOut/iso"+colour,
			 * WriteMode.OVERWRITE); env.execute("First build colour " +
			 * colour);
			 */
			// graphFiltered.mapVertices(new ColourIsolatedNodes<Long>(colour));
			Graph<Long, Tuple4<Integer, Integer, Integer, Integer>, NullValue> resultGraph = graphFiltered
					.run(algorithm);
			System.out.println("ResultGraph");
			// resultGraph.getVertices().print();
			resultGraph.getVertices().writeAsCsv(
					"/Users/dgll/IT4BI/IMPRO3/thirdOut/res" + colour,
					WriteMode.OVERWRITE);
			env.execute("Second build colour " + colour);

			graphFiltered = resultGraph.filterOnVertices(new FilterVetex());
			Graph<Long, Tuple4<Integer, Integer, Integer, Integer>, NullValue> colourGraph = resultGraph
					.filterOnVertices(new FilterNonColourVertex());

			// DataSet<Vertex<Long, Tuple4<Integer, Integer, Integer, Integer>>>
			// result = graphFiltered.getVertices();
			System.out.println("FilteredGraph");
			// result.print();
			// graphFiltered = resultGraph; // filtered

			final ArrayList<Integer> collection = new ArrayList<Integer>();
			DataSet<Integer> num = graphFiltered.numberOfEdges();
			RemoteCollectorImpl.collectLocal(num,
					new RemoteCollectorConsumer<Integer>() {
						// user defined IRemoteCollectorConsumer
						@Override
						public void collect(Integer element) {
							// System.out.println(element);
							collection.add(element);
						}
					});
			colourGraph.getVertices().writeAsCsv(
					"/Users/dgll/IT4BI/IMPRO3/thirdOut/op" + colour,
					WriteMode.OVERWRITE);

			env.execute("Third build colour " + colour);
			verticesRemaining = collection.get(0);

			// outGraph.getVertices().print();
			// graphNoColour.getVertices().print();
			// colourGraph.getVertices().writeAsCsv("/Users/dgll/IT4BI/IMPRO3/firstOut/op"+colour,
			// WriteMode.OVERWRITE);

			System.out.println("Vertices remaining: " + verticesRemaining
					+ " Colour: " + colour);
			colour++;

			// check if the filtered result is empty, if so, break colour++;
		} while (verticesRemaining != 0);

		DataSet<Tuple2<Long, Long>> degrees = graphFiltered.getDegrees();
		graphFiltered = graphFiltered.joinWithVertices(degrees,
				new ColourIsolatedNodes<Long>(colour));
		//System.out.println("ColourIsolatedNodes");
		// graphFiltered.getVertices().print();
		graphFiltered.getVertices().writeAsCsv(
				"/Users/dgll/IT4BI/IMPRO3/thirdOut/iso" + colour,
				WriteMode.OVERWRITE);
		env.execute("First build colour " + colour);

		// outGraph.getVertices().writeAsCsv("/home/amit/impro/output/op"+(colour+1),
		// WriteMode.OVERWRITE);
		// env.execute();

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
	public static final class FilterVertex
			implements
			FilterFunction<Vertex<Long, Tuple4<Integer, Integer, Integer, Integer>>> {

		@Override
		public boolean filter(
				Vertex<Long, Tuple4<Integer, Integer, Integer, Integer>> value)
				throws Exception {
			// TODO Auto-generated method stub
			return value.f1.f0 == -1;
		}
	}

	@SuppressWarnings("serial")
	public static final class FilterNonColourVertex
			implements
			FilterFunction<Vertex<Long, Tuple4<Integer, Integer, Integer, Integer>>> {

		@Override
		public boolean filter(
				Vertex<Long, Tuple4<Integer, Integer, Integer, Integer>> value)
				throws Exception {
			return value.f1.f0 != -1;
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

	// public static final class InitVerticesMapper<K extends Comparable<K> &
	// Serializable>
	// implements
	// MapFunction<Vertex<K, Double>, Vertex<Long, Tuple3<Integer, Integer,
	// Integer>>> {
	//
	// public Vertex<Long, Tuple3<Integer, Integer, Integer>> map(Vertex<K,
	// Double> value) {
	//
	//
	// return new Tuple3<Integer, Integer, Integer>(-1, -1, -1);
	// }
	// }
	//
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

				long ctr = 0;
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

}
