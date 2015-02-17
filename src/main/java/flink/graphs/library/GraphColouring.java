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

public class GraphColouring<K extends Comparable<K> & Serializable> implements
		GraphAlgorithm<K, Tuple4<Integer, Integer, Integer, Integer>, NullValue> {

	private static final int UNKNOWN = 0;
	private static final int TENTATIVELYINS = 1;
	private static final int INS = 2;
	private static final int NOTINS = 3;

	private int maxIterations;
	private int colour;

	public GraphColouring(int maxIterations, int colour) {
		this.maxIterations = maxIterations;
		this.colour = colour;
	}

	@Override
	public Graph<K, Tuple4<Integer, Integer, Integer, Integer>, NullValue> run(Graph<K, Tuple4<Integer, Integer, Integer, Integer>, NullValue> network) {
		return network.runVertexCentricIteration(new VertexUpdater<K>(colour), new MessagingFunc<K>(), maxIterations);
	}
	
	@SuppressWarnings("serial")
	public static final class VertexUpdater<K extends Comparable<K> & Serializable>
			extends
			VertexUpdateFunction<K, Tuple4<Integer, Integer, Integer, Integer>, Long> {

		private int colour = 0;

		public VertexUpdater(int colour) {
			this.colour = colour;
		}
		
		@Override
		public void updateVertex(K vertexKey,
				Tuple4<Integer, Integer, Integer, Integer> vertexValue,
				MessageIterator<Long> inMessages) throws Exception {			
			if ((getSuperstepNumber()) % 4 == 1) { // Select
				//System.out.println("Update-Select " + vertexKey);
				if (vertexValue.f3 == -1) {
					if (vertexValue.f1.intValue() == UNKNOWN) {
						Random r = new Random();
						if (vertexValue.f2.doubleValue() == 0) {
							Tuple4<Integer, Integer, Integer, Integer> newState = vertexValue.copy();
							newState.f1 = INS;
							newState.f0 = colour;
							newState.f3 = getSuperstepNumber()+1;
							setNewVertexValue(newState);
						}
						else if (r.nextDouble() < (((double) 1 )/ (((double) 2) * (vertexValue.f2.doubleValue())))) {
							Tuple4<Integer, Integer, Integer, Integer> newState = vertexValue.copy();
							newState.f1 = TENTATIVELYINS;
							setNewVertexValue(newState);
						}
						else {
							setNewVertexValue(vertexValue);
						}
					}
				}
			} else if ((getSuperstepNumber()) % 4 == 2) { // Conflict
				//System.out.println("Update-Conflict " + vertexKey);
				if (vertexValue.f3 == -1) {
					if (vertexValue.f1.intValue() == TENTATIVELYINS) {
						Boolean amMinimum = true;
						while (inMessages.hasNext()) {
							Long msg = inMessages.next();
							if (msg != 0) {
								if (msg.compareTo((Long) vertexKey) < 0) { // the incoming is lower
									amMinimum = false;
									break;
								}	
							}
						}
						Tuple4<Integer, Integer, Integer, Integer> newState = vertexValue.copy();
						newState.f1 = amMinimum ? INS : UNKNOWN;
						if (newState.f1 == INS) {
							newState.f0 = colour;
							newState.f3 = getSuperstepNumber()+1;
						}
						setNewVertexValue(newState);
					}
					else if (vertexValue.f1.intValue() == UNKNOWN) {
						setNewVertexValue(vertexValue);
					}
				}
			} else if ((getSuperstepNumber()) % 4 == 3) { // DegreeAdj1
				//System.out.println("Update-DegreeAdj1 " + vertexKey);
				if (vertexValue.f3 == -1) {
					if (vertexValue.f1.intValue() == UNKNOWN) {
						boolean neighbourGotIn = false;
						while(inMessages.hasNext()) {
							neighbourGotIn = inMessages.next() != 0;
							if (neighbourGotIn) {
								break;
							}
						}
						if (neighbourGotIn) {
							Tuple4<Integer, Integer, Integer, Integer> newState = vertexValue.copy();
							newState.f1 = NOTINS;
							setNewVertexValue(newState);
						}
						else {
							setNewVertexValue(vertexValue);
						}
					}
				}
			} else if ((getSuperstepNumber()) % 4 == 0) { // DegreeAdj2
				////System.out.println("Update-DegreeAdj2 " + vertexKey);
				if (vertexValue.f3 == -1) {
					if (vertexValue.f1.intValue() == UNKNOWN) {
						int degree = 0;
						while (inMessages.hasNext()) {
							degree += inMessages.next();
						}
						Tuple4<Integer, Integer, Integer, Integer> newState = vertexValue.copy();
						newState.f2 -= degree;
						setNewVertexValue(newState);
					}
				}
			}
		}

	}

	@SuppressWarnings("serial")
	public static final class MessagingFunc<K extends Comparable<K> & Serializable>
			extends
			MessagingFunction<K, Tuple4<Integer, Integer, Integer, Integer>, Long, NullValue> {
		
		@Override
		public void sendMessages(K vertexKey,
				Tuple4<Integer, Integer, Integer, Integer> vertexValue) throws Exception {
			if ((getSuperstepNumber()) % 4 == 1) {  // DegreeAdj2
				//System.out.println("Send-DegreeAdj2 " + vertexKey);
				sendMessageTo(vertexKey, new Long(0));
			}
			else if ((getSuperstepNumber()) % 4 == 2) { // Select
				//System.out.println("Send-Select " + vertexKey);
				if (vertexValue.f3 == -1) {
					if (vertexValue.f1.intValue() == TENTATIVELYINS) {
						sendMessageTo(vertexKey, (Long) vertexKey);						
						sendMessageToAllNeighbors((Long) vertexKey);
					}
					else if (vertexValue.f1.intValue() == UNKNOWN) {
						sendMessageTo(vertexKey, new Long(0));						
					}
				}
			} else if ((getSuperstepNumber()) % 4 == 3) { // Conflict
				//System.out.println("Send-Conflict " + vertexKey);
				//if (vertexValue.f3 != -1) {
					if (vertexValue.f1.intValue() == INS && vertexValue.f3.intValue() == getSuperstepNumber()) {
						sendMessageToAllNeighbors(new Long(1));
					}
					else if (vertexValue.f1.intValue() == UNKNOWN) {
						sendMessageTo(vertexKey, new Long(0));						
					}
				//}
			} else if ((getSuperstepNumber()) % 4 == 0) { // DegreeAdj1
				//System.out.println("Send-DegreeAdj1 " + vertexKey);
				if (vertexValue.f3 == -1) {
					if (vertexValue.f1.intValue() == NOTINS) {
						sendMessageToAllNeighbors(new Long(1));
					}
					else if (vertexValue.f1.intValue() == UNKNOWN) {
						sendMessageTo(vertexKey, new Long(0));						
					}
				}
			}
		}
	}
	

}