package flink.graphs.library;

import java.io.Serializable;
import java.util.Random;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.types.NullValue;

import flink.graphs.Graph;
import flink.graphs.GraphAlgorithm;
import org.apache.flink.spargel.java.MessageIterator;
import org.apache.flink.spargel.java.MessagingFunction;

import org.apache.flink.spargel.java.VertexUpdateFunction;


// Tuple3:Color, state, degree
public class GraphColouring<K extends Comparable<K> & Serializable> implements
		GraphAlgorithm<K, Tuple4<Integer, Integer, Integer, Integer>, NullValue> {

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
	public Graph<K, Tuple4<Integer, Integer, Integer, Integer>, NullValue> run(Graph<K, Tuple4<Integer, Integer, Integer, Integer>, NullValue> network) {
		//network.getContext().getIdString();
		return network.runVertexCentricIteration(new VertexUpdater<K>(colour, network.getContext().getIdString()), new MessagingFunc<K>(network.getContext().getIdString()), maxIterations);
	}
	
	@SuppressWarnings("serial")
	public static final class VertexUpdater<K extends Comparable<K> & Serializable>
			extends
			VertexUpdateFunction<K, Tuple4<Integer, Integer, Integer, Integer>, Long> {

		private int colour = 0;
		private String id;

		public VertexUpdater(int colour, String id) {
			this.colour = colour;
			this.id = id;
		}
		
		@Override
		public void updateVertex(K vertexKey,
				Tuple4<Integer, Integer, Integer, Integer> vertexValue,
				MessageIterator<Long> inMessages) throws Exception {
			// TODO do the filtering, otherwise this will be executed for all
			// nodes, even the ones that have colour
			
			if (getSuperstepNumber() == 1) { // DegreeInit1
				if (vertexValue.f3 == -1) {
					Tuple4<Integer, Integer, Integer, Integer> newState = vertexValue.copy();
					newState.f1 = UNKNOWN;
					setNewVertexValue(newState);
					//System.out.println("ID " + id + " " + "Update-superstep1 " + getSuperstepNumber() + " vertexKey " + vertexKey + " vCol " + vertexValue.f0);
				}
			} else if (getSuperstepNumber() == 2) { // DegreeInit2
				if (vertexValue.f3 == -1) {
					int degree = 0;
					while (inMessages.hasNext()) {
						inMessages.next();
						degree++;
					}
					Tuple4<Integer, Integer, Integer, Integer> newState = vertexValue.copy();
					newState.f2 = degree;
					setNewVertexValue(newState);
					//System.out.println("ID " + id + " " + "Update-superstep2 " + getSuperstepNumber() + " vertexKey " + vertexKey + " vCol " + vertexValue.f0);
				}
			} else if (((getSuperstepNumber() - 3)) % 4 == 0) { // Select
				if (vertexValue.f3 == -1) {
					//System.out.println("ID " + id + " " + "Update-superstep3.1 " + getSuperstepNumber() + " vertexKey " + vertexKey + " vCol " + vertexValue.f0);
					if (vertexValue.f1.intValue() == UNKNOWN) {
						//System.out.println("ID " + id + " " + "Update-superstep3.2 " + getSuperstepNumber() + " vertexKey " + vertexKey + " vCol " + vertexValue.f0);
						Random r = new Random();
						if (vertexValue.f2.doubleValue() == 0 || (r.nextDouble() < (((double) 1 )/ (((double) 2) * (vertexValue.f2.doubleValue()))))) {
							Tuple4<Integer, Integer, Integer, Integer> newState = vertexValue.copy();
							newState.f1 = TENTATIVELYINS;
							setNewVertexValue(newState);
							//System.out.println("ID " + id + " " + "Update-superstep3.3 " + getSuperstepNumber() + " vertexKey " + vertexKey + " vCol " + vertexValue.f0);
						}
					}
					//System.out.println("ID " + id + " " + "Update-superstep3 " + getSuperstepNumber() + " vertexKey " + vertexKey + " vCol " + vertexValue.f0);
				}
			} else if (((getSuperstepNumber() - 3)) % 4 == 1) { // Conflict
				if (vertexValue.f3 == -1) {
					if (vertexValue.f1.intValue() == TENTATIVELYINS) {
						Boolean amMinimum = true;
						while (inMessages.hasNext()) {
							if (inMessages.next().compareTo((Long) vertexKey) <= 0) { // the incoming is lower
								amMinimum = false;
								break;
							}
						}
						Tuple4<Integer, Integer, Integer, Integer> newState = vertexValue.copy();
						newState.f1 = amMinimum ? INS : UNKNOWN;
						if (newState.f1 == INS) {
							newState.f0 = colour;
							newState.f3 = getSuperstepNumber()+1;
							//newState.f2 = getSuperstepNumber()+1;
						}
						setNewVertexValue(newState);
					}
					//System.out.println("ID " + id + " " + "Update-superstep4 " + getSuperstepNumber() + " vertexKey " + vertexKey + " vCol " + vertexValue.f0);
				}
			} else if (((getSuperstepNumber() - 3)) % 4 == 2) { // DegreeAdj1
				if (vertexValue.f3 == -1) {
					if (vertexValue.f1.intValue() == UNKNOWN) {
						if (inMessages.hasNext()) {
							Tuple4<Integer, Integer, Integer, Integer> newState = vertexValue.copy();
							newState.f1 = NOTINS;
							setNewVertexValue(newState);
						}
					}
					//System.out.println("ID " + id + " " + "Update-superstep5 " + getSuperstepNumber() + " vertexKey " + vertexKey + " vCol " + vertexValue.f0);
				}
			} else if (((getSuperstepNumber() - 3)) % 4 == 3) { // DegreeAdj2
				if (vertexValue.f3 == -1) {
					if (vertexValue.f1.intValue() == UNKNOWN) {
						int degree = 0;
						while (inMessages.hasNext()) {
							inMessages.next();
							degree++;
						}
						Tuple4<Integer, Integer, Integer, Integer> newState = vertexValue.copy();
						newState.f2 -= degree;
						setNewVertexValue(newState);
					}
					//System.out.println("ID " + id + " " + "Update-superstep6 " + getSuperstepNumber() + " vertexKey " + vertexKey + " vCol " + vertexValue.f0);
				}
			}
		}

	}

	@SuppressWarnings("serial")
	public static final class MessagingFunc<K extends Comparable<K> & Serializable>
			extends
			MessagingFunction<K, Tuple4<Integer, Integer, Integer, Integer>, Long, NullValue> {

		private String id;

		public MessagingFunc(String id) {
			this.id = id;
		}
		
		@Override
		public void sendMessages(K vertexKey,
				Tuple4<Integer, Integer, Integer, Integer> vertexValue) throws Exception {

			if (getSuperstepNumber() == 1) { // DegreeInit1
				if (vertexValue.f3 == -1) {
					sendMessageToAllNeighbors(new Long(0));
					//System.out.println("ID " + id + " " + "Send-superstep1 " + getSuperstepNumber() + " vertexKey " + vertexKey + " vCol " + vertexValue.f0);
				}
			} else if (getSuperstepNumber() == 2) { // DegreeInit2
				if (vertexValue.f3 == -1) {
					sendMessageToAllNeighbors(new Long(0));
					//System.out.println("ID " + id + " " + "Send-superstep2 " + getSuperstepNumber() + " vertexKey " + vertexKey + " vCol " + vertexValue.f0);
				}
			} else if (getSuperstepNumber() == 3) { // DegreeInit2
				if (vertexValue.f3 == -1) {
					sendMessageToAllNeighbors(new Long(0));
					//System.out.println("ID " + id + " " + "Send-superstep2.5 " + getSuperstepNumber() + " vertexKey " + vertexKey + " vCol " + vertexValue.f0);
				}
			} else if (((getSuperstepNumber() - 4)) % 4 == 0) { // Select
				if (vertexValue.f3 == -1) {
					if (vertexValue.f1.intValue() == TENTATIVELYINS) {
						sendMessageToAllNeighbors((Long) vertexKey);
					}
					//System.out.println("ID " + id + " " + "Send-superstep3 " + getSuperstepNumber() + " vertexKey " + vertexKey + " vCol " + vertexValue.f0);
				}
			} else if (((getSuperstepNumber() - 4)) % 4 == 1) { // Conflict
				if (vertexValue.f3 != -1) {
					if (vertexValue.f1.intValue() == INS && vertexValue.f3.intValue() == getSuperstepNumber()) {
						sendMessageToAllNeighbors(new Long(0));
					}
					//System.out.println("ID " + id + " " + "Send-superstep4 " + getSuperstepNumber() + " vertexKey " + vertexKey + " vCol " + vertexValue.f0);
				}
			} else if (((getSuperstepNumber() - 4)) % 4 == 2) { // DegreeAdj1
				if (vertexValue.f3 == -1) {
					if (vertexValue.f1.intValue() == NOTINS) {
						sendMessageToAllNeighbors(new Long(0));
					}
					//System.out.println("ID " + id + " " + "Send-superstep5 " + getSuperstepNumber() + " vertexKey " + vertexKey + " vCol " + vertexValue.f0);
				}
			} else if (((getSuperstepNumber() - 4)) % 4 == 3) { // DegreeAdj2
				if (vertexValue.f3 == -1) {
					//System.out.println("ID " + id + " " + "Send-superstep6 " + getSuperstepNumber() + " vertexKey " + vertexKey + " vCol " + vertexValue.f0);
					sendMessageToAllNeighbors(new Long(0));
				}
			}
		}
	}


}
