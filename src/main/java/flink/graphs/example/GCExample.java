package flink.graphs.example;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.regex.Pattern;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.RemoteCollectorConsumer;
import org.apache.flink.api.java.io.RemoteCollectorImpl;
import org.apache.flink.api.java.io.TypeSerializerInputFormat;
import org.apache.flink.api.java.io.TypeSerializerOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeInfoParser;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.spargel.java.record.SpargelIteration;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import flink.graphs.library.GraphColouring;
import flink.graphs.Edge;
import flink.graphs.Graph;
import flink.graphs.Vertex;

public class GCExample {

	private static int maxiteration;

	public static void main(String[] args) throws Exception {

		//ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
		
		maxiteration = 10;

		DataSource<String> input = env.readTextFile("/Users/dgll/graph6");

		DataSet<Vertex<Long, Tuple4<Integer, Integer, Integer, Integer>>> nodes = input.flatMap(new NodeReader()).distinct();

		DataSet<Edge<Long, NullValue>> edges = input.flatMap(new EdgeReader()).distinct();

		Graph<Long, Tuple4<Integer, Integer, Integer, Integer>, NullValue> graph = new Graph<Long, Tuple4<Integer,Integer, Integer, Integer>, NullValue>(nodes, edges, env);

		int colour = 0;
		int verticesRemaining = 0;

		String cachePath = "/Users/dgll/IT4BI/IMPRO3/fourthOut/cache";
		String nodesPath = cachePath + "/nodes/state" + "_" + 0;
		String edgesPath = cachePath + "/edges/state" + "_" + 0;
		
		TupleTypeInfo<Tuple2<Long, Tuple4<Integer, Integer, Integer, Integer>>> nodesType = new TupleTypeInfo<>(BasicTypeInfo.getInfoFor(Long.class), TypeInfoParser.parse("Tuple4<Integer, Integer, Integer, Integer>"));
		TypeSerializerOutputFormat<Tuple2<Long, Tuple4<Integer, Integer, Integer, Integer>>> nodesOutput = new TypeSerializerOutputFormat<>();
		nodesOutput.setInputType(nodesType);
		nodesOutput.setSerializer(nodesType.createSerializer());
		nodesOutput.setWriteMode(FileSystem.WriteMode.OVERWRITE);
		nodesOutput.setOutputDirectoryMode(FileOutputFormat.OutputDirectoryMode.ALWAYS);
		nodesOutput.setOutputFilePath(new Path(nodesPath));
		
		TypeSerializerInputFormat<Tuple2<Long, Tuple4<Integer, Integer, Integer, Integer>>> nodesInput = new TypeSerializerInputFormat<>(nodesType.createSerializer());
		nodesInput.setFilePath(nodesPath);
		graph.getVerticesAsTuple2().write(nodesOutput, nodesPath);
		
		
		
		TupleTypeInfo<Tuple3<Long, Long, NullValue>> edgesType = new TupleTypeInfo<>(BasicTypeInfo.getInfoFor(Long.class), BasicTypeInfo.getInfoFor(Long.class), TypeInfoParser.parse("NullValue"));
		TypeSerializerOutputFormat<Tuple3<Long, Long, NullValue>> edgesOutput = new TypeSerializerOutputFormat<>();
		edgesOutput.setInputType(edgesType);
		edgesOutput.setSerializer(edgesType.createSerializer());
		edgesOutput.setWriteMode(FileSystem.WriteMode.OVERWRITE);
		edgesOutput.setOutputDirectoryMode(FileOutputFormat.OutputDirectoryMode.ALWAYS);
		edgesOutput.setOutputFilePath(new Path(edgesPath));
		
		TypeSerializerInputFormat<Tuple3<Long, Long, NullValue>> edgesInput = new TypeSerializerInputFormat<>(edgesType.createSerializer());
		edgesInput.setFilePath(edgesPath);
		graph.getEdgesAsTuple3().write(edgesOutput, edgesPath);
		
		env.execute("GraphColouring prepare");
		do {
			System.out.println("Colours:" + colour);
			
			nodesInput.setFilePath(nodesPath);
			edgesInput.setFilePath(edgesPath);
			DataSet<Tuple2<Long, Tuple4<Integer, Integer, Integer, Integer>>> nodess = env.createInput(nodesInput, nodesType);
			System.out.println(nodess.getType().getArity());
			DataSet<Tuple3<Long, Long, NullValue>> edgess = env.createInput(edgesInput, edgesType);
			
			DataSet<Vertex<Long, Tuple4<Integer, Integer, Integer, Integer>>> nodesss = nodess.map(new VertexMapper());
			DataSet<Edge<Long, NullValue>> edgesss = edgess.map(new EdgeMapper());
			Graph<Long, Tuple4<Integer, Integer, Integer, Integer>, NullValue> graph2 = Graph.fromDataSet(nodesss, edgesss, env);
			GraphColouring<Long> algorithm = new GraphColouring<Long>(maxiteration, colour);
			Graph<Long, Tuple4<Integer, Integer, Integer, Integer>, NullValue> resultGraph = graph2.run(algorithm);
			System.out.println("ResultGraph");
			resultGraph.getVertices().writeAsCsv("/Users/dgll/IT4BI/IMPRO3/fourthOut/res"+colour, WriteMode.OVERWRITE);
			
			Graph<Long, Tuple4<Integer, Integer, Integer, Integer>, NullValue> nonColourGraph = resultGraph.filterOnVertices(new FilterVertex());
			Graph<Long, Tuple4<Integer, Integer, Integer, Integer>, NullValue> colourGraph = resultGraph.filterOnVertices(new FilterNonColourVertex());
			System.out.println("FilteredGraph");

			final ArrayList<Integer> collection = new ArrayList<Integer>();
			DataSet<Integer> num = nonColourGraph.numberOfEdges();
			RemoteCollectorImpl.collectLocal(num,
					new RemoteCollectorConsumer<Integer>() {
						@Override
						public void collect(Integer element) {
							collection.add(element);
						}
					});
			colourGraph.getVertices().writeAsCsv("/Users/dgll/IT4BI/IMPRO3/fourthOut/op"+colour, WriteMode.OVERWRITE);
		
			nonColourGraph.getVerticesAsTuple2().write(nodesOutput, cachePath + "/nodes/state" + "_" + 0);
			nonColourGraph.getEdgesAsTuple3().write(edgesOutput, cachePath + "/edges/state" + "_" + 0);
			
			env.execute("Third build colour " + colour);
			verticesRemaining = collection.get(0);

			System.out.println("Vertices remaining: " + verticesRemaining + " Colour: " + colour);
			colour++;

		} while (verticesRemaining != 0);
		
		
		nodesInput.setFilePath(nodesPath);
		edgesInput.setFilePath(edgesPath);
		DataSet<Tuple2<Long, Tuple4<Integer, Integer, Integer, Integer>>> nodess = env.createInput(nodesInput, nodesType);
		DataSet<Tuple3<Long, Long, NullValue>> edgess = env.createInput(edgesInput, edgesType);
		
		//Graph<Long, Tuple4<Integer, Integer, Integer, Integer>, NullValue> graph2 = new Graph<Long, Tuple4<Integer,Integer, Integer, Integer>, NullValue>(nodess, edgess, env);//Graph.fromTupleDataSet(nodess, edgess, env);
		Graph<Long, Tuple4<Integer, Integer, Integer, Integer>, NullValue> graph2 = new Graph<Long, Tuple4<Integer,Integer, Integer, Integer>, NullValue>(nodess, edgess, env);
		
		DataSet<Tuple2<Long, Long>> degrees = graph2.inDegrees();
		graph2 = graph2.joinWithVertices(degrees, new ColourIsolatedNodes<Long>(colour));
		System.out.println("ColourIsolatedNodes");
		//graphFiltered.getVertices().print();
		graph2.getVertices().writeAsCsv("/Users/dgll/IT4BI/IMPRO3/fourthOut/iso"+colour, WriteMode.OVERWRITE);
		env.execute("First build colour " + colour);
		
		//outGraph.getVertices().writeAsCsv("/home/amit/impro/output/op"+(colour+1), WriteMode.OVERWRITE);
		//env.execute();
		
		RemoteCollectorImpl.shutdownAll();
	}

	@SuppressWarnings("serial")
	public static final class VertexMapper<K extends Comparable<K> & Serializable> 
		implements MapFunction<Tuple2<Long, Tuple4<Integer, Integer, Integer, Integer>>, Vertex<Long, Tuple4<Integer, Integer, Integer, Integer>>> {

		@Override
		public Vertex<Long, Tuple4<Integer, Integer, Integer, Integer>> map(
				Tuple2<Long, Tuple4<Integer, Integer, Integer, Integer>> value)
				throws Exception {
			
			return new Vertex<>(value.f0, value.f1);
		}
		
	}
	
	@SuppressWarnings("serial")
	public static final class EdgeMapper<K extends Comparable<K> & Serializable> 
		implements MapFunction<Tuple3<Long, Long, NullValue>, Edge<Long, NullValue>> {

		@Override
		public Edge<Long, NullValue> map(Tuple3<Long, Long, NullValue> value)
				throws Exception {
			// TODO Auto-generated method stub
			return new Edge<Long, NullValue>(value.f0, value.f1, value.f2);
		}
		
	}
	
	@SuppressWarnings("serial")
	public static final class ColourIsolatedNodes<K extends Comparable<K> & Serializable> 
		implements MapFunction<Tuple2<Tuple4<Integer, Integer, Integer, Integer>, Long>, Tuple4<Integer, Integer, Integer, Integer>> {
		
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
			// TODO Auto-generated method stub
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

				collector.collect(new Edge<Long, NullValue>(source, target, new NullValue()));
				collector.collect(new Edge<Long, NullValue>(target, source, new NullValue()));
			}
		}
	}

//	public static final class InitVerticesMapper<K extends Comparable<K> & Serializable>
//			implements
//			MapFunction<Vertex<K, Double>, Vertex<Long, Tuple3<Integer, Integer, Integer>>> {
//
//		public Vertex<Long, Tuple3<Integer, Integer, Integer>> map(Vertex<K, Double> value) {
//
//			
//			return new Tuple3<Integer, Integer, Integer>(-1, -1, -1);
//		}
//	}
//
	@SuppressWarnings("serial")
	public static class NodeReader implements
			FlatMapFunction<String, Vertex<Long, Tuple4<Integer, Integer, Integer, Integer>>> {

		private static final Pattern SEPARATOR = Pattern.compile("[ \t,]");

		public void flatMap(String s, Collector<Vertex<Long, Tuple4<Integer, Integer, Integer, Integer>>> collector)
				throws Exception {
			if (!s.startsWith("%")) {
				String[] tokens = SEPARATOR.split(s);

				long ctr = 0;
				long source = Long.parseLong(tokens[0]);
				long target = Long.parseLong(tokens[1]);

				collector.collect(new Vertex<Long, Tuple4<Integer, Integer, Integer, Integer>>(source, new Tuple4<Integer, Integer, Integer, Integer>(-1, -1, -1, -1)));
				collector.collect(new Vertex<Long, Tuple4<Integer, Integer, Integer, Integer>>(target, new Tuple4<Integer, Integer, Integer, Integer>(-1, -1, -1, -1)));
			}
		}
	}

}