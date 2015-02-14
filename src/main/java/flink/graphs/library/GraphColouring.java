package flink.graphs.library;

import java.io.Serializable;
import java.util.Random;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.spargel.java.MessageIterator;
import org.apache.flink.spargel.java.MessagingFunction;
import org.apache.flink.spargel.java.VertexCentricIteration;
import org.apache.flink.spargel.java.VertexUpdateFunction;
import org.apache.flink.spargel.java.record.SpargelIteration;
import org.apache.flink.types.NullValue;

import flink.graphs.Graph;
import flink.graphs.GraphAlgorithm;

// Tuple3:Color, state, degree
public class GraphColouring<K extends Comparable<K> & Serializable> implements
		GraphAlgorithm<K, Tuple3<Integer, Integer, Integer>, NullValue> {

	/*
	 * Unknown 0 Tentatively 1 inS 2 NotInS
	 */
	private static final int UNKNOWN = 0;
	private static final int TENTATIVELYINS = 1;
	private static final int INS = 2;
	private static final int NOTINS = 3;

	private static final int SDegreeInit1 = 0;
	private static final int SDegreeInit2 = 1;
	private static final int SSelection = 2;
	private static final int SConflictResolution = 3;
	private static final int SDegreeAdj1 = 4;
	private static final int SDegreeAdj2 = 5;
	private static final int SColorAss = 6;

	// f0 colour, f1 type, f2 degree

	private int maxIterations;
	private int colour;

	public GraphColouring(int maxIterations, int colour) {
		this.maxIterations = maxIterations;
		this.colour = colour;
	}

	@Override
	public Graph<K, Tuple3<Integer, Integer, Integer>, NullValue> run(
			Graph<K, Tuple3<Integer, Integer, Integer>, NullValue> network) {
		return network.runVertexCentricIteration(new VertexUpdater<K>(colour),
				new MessagingFunc<K>(), maxIterations);
	}
	
	@SuppressWarnings("serial")
	public static final class VertexUpdater<K extends Comparable<K> & Serializable>
			extends
			VertexUpdateFunction<K, Tuple3<Integer, Integer, Integer>, Long> {

		private int colour = 0;

		public VertexUpdater(int colour) {
			this.colour = colour;
		}

		@Override
		public void updateVertex(K vertexKey,
				Tuple3<Integer, Integer, Integer> vertexValue,
				MessageIterator<Long> inMessages) throws Exception {
			// TODO do the filtering, otherwise this will be executed for all
			// nodes, even the ones that have colour
			if (getSuperstepNumber() == 1) { // DegreeInit1
				Tuple3<Integer, Integer, Integer> newState = vertexValue.copy();
				newState.f1 = UNKNOWN;
				setNewVertexValue(newState);
				System.out.println("Update-superstep1 " + getSuperstepNumber());
			} else if (getSuperstepNumber() == 2) { // DegreeInit2
				int degree = 0;
				while (inMessages.hasNext()) {
					inMessages.next();
					degree++;
				}
				Tuple3<Integer, Integer, Integer> newState = vertexValue.copy();
				newState.f2 = degree;
				setNewVertexValue(newState);
				System.out.println("Update-superstep2 " + getSuperstepNumber());
			} else if (((getSuperstepNumber() - 3)) % 4 == 0) { // Select
				System.out.println("Update-superstep3.1 " + getSuperstepNumber() + " " + vertexValue.f1.intValue() + " " + vertexKey);

				if (vertexValue.f1.intValue() == UNKNOWN) {
					System.out.println("Update-superstep3.2 " + getSuperstepNumber());
					Random r = new Random();
					if (vertexValue.f2.doubleValue() == 0 || true) {
							//|| (r.nextDouble() < ((double) 1 / ((double) 2 * vertexValue.f2
								//	.doubleValue())))) {
						Tuple3<Integer, Integer, Integer> newState = vertexValue
								.copy();
						newState.f1 = TENTATIVELYINS;
						setNewVertexValue(newState);
						System.out.println("Update-superstep3 " + getSuperstepNumber());
					}
				}
				System.out.println("Update-superstep3 " + getSuperstepNumber());
			} else if (((getSuperstepNumber() - 3)) % 4 == 1) { // Conflict
				if (vertexValue.f1.intValue() == TENTATIVELYINS) {
					boolean amMinimum = true;
					while (inMessages.hasNext()) {
						if (inMessages.next().compareTo((Long) vertexKey) <= 0) { // the
																						// incoming
																						// is
																						// lower
							amMinimum = false;
							break;
						}
					}
					Tuple3<Integer, Integer, Integer> newState = vertexValue
							.copy();
					newState.f1 = amMinimum ? INS : UNKNOWN;
					if (newState.f1 == INS) {
						newState.f0 = colour;
					}
					setNewVertexValue(newState);
				}
				System.out.println("Update-superstep4 " + getSuperstepNumber());
			} else if (((getSuperstepNumber() - 3)) % 4 == 2) { // DegreeAdj1
				if (vertexValue.f1.intValue() == UNKNOWN) {
					if (inMessages.hasNext()) {
						Tuple3<Integer, Integer, Integer> newState = vertexValue
								.copy();
						newState.f1 = NOTINS;
						setNewVertexValue(newState);
					}
				}
				System.out.println("Update-superstep5 " + getSuperstepNumber());
			} else if (((getSuperstepNumber() - 3)) % 4 == 3) { // DegreeAdj2
				if (vertexValue.f1.intValue() == UNKNOWN) {
					int degree = 0;
					while (inMessages.hasNext()) {
						inMessages.next();
						degree++;
					}
					Tuple3<Integer, Integer, Integer> newState = vertexValue
							.copy();
					newState.f2 -= degree;
					setNewVertexValue(newState);
				}
				System.out.println("Update-superstep6 " + getSuperstepNumber());
			}
		}
	}

	@SuppressWarnings("serial")
	public static final class MessagingFunc<K extends Comparable<K> & Serializable>
			extends
			MessagingFunction<K, Tuple3<Integer, Integer, Integer>, Long, NullValue> {

		@Override
		public void sendMessages(K vertexKey,
				Tuple3<Integer, Integer, Integer> vertexValue) throws Exception {

			if (getSuperstepNumber() == 1) { // DegreeInit1
				sendMessageToAllNeighbors(new Long(0));
				System.out.println("Send-superstep1 " + getSuperstepNumber());
			} else if (getSuperstepNumber() == 2) { // DegreeInit2
				sendMessageToAllNeighbors(new Long(0));
				System.out.println("Send-superstep2 " + getSuperstepNumber());
			} else if (getSuperstepNumber() == 3) { // DegreeInit2
				sendMessageToAllNeighbors(new Long(0));
				System.out.println("Send-superstep2 " + getSuperstepNumber());
			} else if (((getSuperstepNumber() - 4)) % 4 == 0) { // Select
				if (vertexValue.f1.intValue() == TENTATIVELYINS) {
					sendMessageToAllNeighbors((Long) vertexKey);
				}
				System.out.println("Send-superstep3 " + getSuperstepNumber());
			} else if (((getSuperstepNumber() - 4)) % 4 == 1) { // Conflict
				if (vertexValue.f1.intValue() == INS) {
					sendMessageToAllNeighbors(new Long(0));
				}
				System.out.println("Send-superstep4 " + getSuperstepNumber());
			} else if (((getSuperstepNumber() - 4)) % 4 == 2) { // DegreeAdj1
				if (vertexValue.f1.intValue() == NOTINS) {
					sendMessageToAllNeighbors(new Long(0));
				}
				System.out.println("Send-superstep5 " + getSuperstepNumber());
			} else if (((getSuperstepNumber() - 4)) % 4 == 3) { // DegreeAdj2
				System.out.println("Send-superstep6 " + getSuperstepNumber());
				sendMessageToAllNeighbors(new Long(0));
			}
		}
	}

	/*
	 * //INIT 1 and 2 should only execute once per colour and INIT 1 should not
	 * receive anything
	 * 
	 * @SuppressWarnings("serial") public static final class VertexDegreeInit1<K
	 * extends Comparable<K> & Serializable> extends VertexUpdateFunction<K,
	 * Tuple3<Integer, Integer, Integer>, NullValue> {
	 * 
	 * @Override public void updateVertex(K vertexKey, Tuple3<Integer, Integer,
	 * Integer> vertexValue, MessageIterator<NullValue> inMessages) throws
	 * Exception { if (vertexValue.f0.intValue() == -1) { Tuple3<Integer,
	 * Integer, Integer> newState = vertexValue.copy(); newState.f1 = UNKNOWN;
	 * setNewVertexValue(newState); } } }
	 * 
	 * @SuppressWarnings("serial") public static final class
	 * VertexDegreeInitM1<K extends Comparable<K> & Serializable> extends
	 * MessagingFunction<K, Tuple3<Integer, Integer, Integer>, NullValue,
	 * NullValue> {
	 * 
	 * @Override public void sendMessages(K vertexKey, Tuple3<Integer, Integer,
	 * Integer> vertexValue) throws Exception { if (vertexValue.f0.intValue() ==
	 * -1 && vertexValue.f1.intValue() == UNKNOWN) {
	 * sendMessageToAllNeighbors(new NullValue()); } } }
	 * 
	 * @SuppressWarnings("serial") public static final class VertexDegreeInit2<K
	 * extends Comparable<K> & Serializable> extends VertexUpdateFunction<K,
	 * Tuple3<Integer, Integer, Integer>, NullValue> {
	 * 
	 * @Override public void updateVertex(K vertexKey, Tuple3<Integer, Integer,
	 * Integer> vertexValue, MessageIterator<NullValue> inMessages) throws
	 * Exception { if (vertexValue.f0.intValue() == -1) { int degree = 0; while
	 * (inMessages.hasNext()) { inMessages.next(); degree++; } Tuple3<Integer,
	 * Integer, Integer> newState = vertexValue.copy(); newState.f2 = degree;
	 * setNewVertexValue(newState); } } }
	 * 
	 * @SuppressWarnings("serial") public static final class
	 * VertexDegreeInitM2<K extends Comparable<K> & Serializable> extends
	 * MessagingFunction<K, Tuple3<Integer, Integer, Integer>, NullValue,
	 * NullValue> {
	 * 
	 * @Override public void sendMessages(K vertexKey, Tuple3<Integer, Integer,
	 * Integer> vertexValue) throws Exception { if (vertexValue.f0.intValue() ==
	 * -1 && vertexValue.f1.intValue() == UNKNOWN) {
	 * sendMessageToAllNeighbors(new NullValue()); } } }
	 * 
	 * // This should not receive any message
	 * 
	 * @SuppressWarnings("serial") public static final class VertexSelection<K
	 * extends Comparable<K> & Serializable> extends VertexUpdateFunction<K,
	 * Tuple3<Integer, Integer, Integer>, NullValue> {
	 * 
	 * @Override public void updateVertex(K vertexKey, Tuple3<Integer, Integer,
	 * Integer> vertexValue, MessageIterator<NullValue> inMessages) throws
	 * Exception { if (vertexValue.f0.intValue() == -1 &&
	 * vertexValue.f1.intValue() == UNKNOWN) { Random r = new Random(); if
	 * (r.nextDouble() < ((double) 1 /((double) 2 *
	 * vertexValue.f2.doubleValue()))) { Tuple3<Integer, Integer, Integer>
	 * newState = vertexValue.copy(); newState.f1 = TENTATIVELYINS;
	 * setNewVertexValue(newState); } } } }
	 * 
	 * @SuppressWarnings("serial") public static final class VertexSelectionM<K
	 * extends Comparable<K> & Serializable> extends MessagingFunction<K,
	 * Tuple3<Integer, Integer, Integer>, Integer, NullValue> {
	 * 
	 * @Override public void sendMessages(K vertexKey, Tuple3<Integer, Integer,
	 * Integer> vertexValue) throws Exception { if (vertexValue.f0.intValue() ==
	 * -1) { if (vertexValue.f1.intValue() == TENTATIVELYINS) {
	 * sendMessageToAllNeighbors((Integer) vertexKey); } } } }
	 * 
	 * @SuppressWarnings("serial") public static final class
	 * VertexConflictResolution<K extends Comparable<K> & Serializable> extends
	 * VertexUpdateFunction<K, Tuple3<Integer, Integer, Integer>, Integer> {
	 * 
	 * @Override public void updateVertex(K vertexKey, Tuple3<Integer, Integer,
	 * Integer> vertexValue, MessageIterator<Integer> inMessages) throws
	 * Exception { if (vertexValue.f0.intValue() == -1 &&
	 * vertexValue.f1.intValue() == TENTATIVELYINS) { boolean amMinimum = true;
	 * while (inMessages.hasNext()) { if (inMessages.next().compareTo((Integer)
	 * vertexKey) <= 0) { //the incoming is lower amMinimum = false; break; } }
	 * Tuple3<Integer, Integer, Integer> newState = vertexValue.copy();
	 * newState.f1 = amMinimum ? INS : UNKNOWN; setNewVertexValue(newState); } }
	 * }
	 * 
	 * @SuppressWarnings("serial") public static final class
	 * VertexConflictResolutionM<K extends Comparable<K> & Serializable> extends
	 * MessagingFunction<K, Tuple3<Integer, Integer, Integer>, NullValue,
	 * NullValue> {
	 * 
	 * @Override public void sendMessages(K vertexKey, Tuple3<Integer, Integer,
	 * Integer> vertexValue) throws Exception { if (vertexValue.f0.intValue() ==
	 * -1) { if (vertexValue.f1.intValue() == INS) {
	 * sendMessageToAllNeighbors(new NullValue()); } } } }
	 * 
	 * @SuppressWarnings("serial") public static final class VertexDiscovery<K
	 * extends Comparable<K> & Serializable> extends VertexUpdateFunction<K,
	 * Tuple3<Integer, Integer, Integer>, NullValue> {
	 * 
	 * @Override public void updateVertex(K vertexKey, Tuple3<Integer, Integer,
	 * Integer> vertexValue, MessageIterator<NullValue> inMessages) throws
	 * Exception { if (vertexValue.f0.intValue() == -1 &&
	 * vertexValue.f1.intValue() == UNKNOWN) { if (inMessages.hasNext()) {
	 * Tuple3<Integer, Integer, Integer> newState = vertexValue.copy();
	 * newState.f1 = NOTINS; setNewVertexValue(newState); } } } }
	 * 
	 * @SuppressWarnings("serial") public static final class VertexDiscoveryM<K
	 * extends Comparable<K> & Serializable> extends MessagingFunction<K,
	 * Tuple3<Integer, Integer, Integer>, NullValue, NullValue> {
	 * 
	 * @Override public void sendMessages(K vertexKey, Tuple3<Integer, Integer,
	 * Integer> vertexValue) throws Exception { if (vertexValue.f0.intValue() ==
	 * -1 && vertexValue.f1.intValue() == INS) { sendMessageToAllNeighbors(new
	 * NullValue()); } } }
	 * 
	 * @SuppressWarnings("serial") public static final class
	 * VertexDegreeAdjusting<K extends Comparable<K> & Serializable> extends
	 * VertexUpdateFunction<K, Tuple3<Integer, Integer, Integer>, NullValue> {
	 * 
	 * @Override public void updateVertex(K vertexKey, Tuple3<Integer, Integer,
	 * Integer> vertexValue, MessageIterator<NullValue> inMessages) throws
	 * Exception { if (vertexValue.f0.intValue() == -1 &&
	 * vertexValue.f1.intValue() == UNKNOWN) { int degree = 0; while
	 * (inMessages.hasNext()) { inMessages.next(); degree++; } Tuple3<Integer,
	 * Integer, Integer> newState = vertexValue.copy(); newState.f2 -= degree;
	 * setNewVertexValue(newState); } } }
	 * 
	 * @SuppressWarnings("serial") public static final class
	 * VertexDegreeAdjustingM<K extends Comparable<K> & Serializable> extends
	 * MessagingFunction<K, Tuple3<Integer, Integer, Integer>, NullValue,
	 * NullValue> {
	 * 
	 * @Override public void sendMessages(K vertexKey, Tuple3<Integer, Integer,
	 * Integer> vertexValue) throws Exception { //USELESS, because it should not
	 * send anything } }
	 * 
	 * @SuppressWarnings("serial") public static final class
	 * VertexColourAssignment<K extends Comparable<K> & Serializable> extends
	 * VertexUpdateFunction<K, Tuple3<Integer, Integer, Integer>, NullValue> {
	 * 
	 * @Override public void updateVertex(K vertexKey, Tuple3<Integer, Integer,
	 * Integer> vertexValue, MessageIterator<NullValue> inMessages) throws
	 * Exception { if (vertexValue.f0.intValue() == -1) { if
	 * (vertexValue.f1.intValue() == INS) { Tuple3<Integer, Integer, Integer>
	 * newState = vertexValue.copy(); newState.f0 = 2; //TODO
	 * setNewVertexValue(newState); } else { Tuple3<Integer, Integer, Integer>
	 * newState = vertexValue.copy(); newState.f1 = UNKNOWN;
	 * setNewVertexValue(newState); } } } }
	 * 
	 * @SuppressWarnings("serial") public static final class
	 * VertexColourAssignmentM<K extends Comparable<K> & Serializable> extends
	 * MessagingFunction<K, Tuple3<Integer, Integer, Integer>, NullValue,
	 * NullValue> {
	 * 
	 * @Override public void sendMessages(K vertexKey, Tuple3<Integer, Integer,
	 * Integer> vertexValue) throws Exception { //USELESS } }
	 */
}
