package flink.graphs.library;


import java.io.Serializable;
import java.util.Random;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.spargel.java.MessageIterator;
import org.apache.flink.spargel.java.MessagingFunction;
import org.apache.flink.spargel.java.VertexUpdateFunction;
import org.apache.flink.types.NullValue;

import flink.graphs.Graph;
import flink.graphs.GraphAlgorithm;

public class GraphColouring<K extends Comparable<K> & Serializable> implements GraphAlgorithm<K, Tuple3<Integer, Integer, Integer>, NullValue> {

	/*
	 * Unknown 0
	 * Tentatively 1
	 * inS 2
	 * NotInS
	 */
	private static final int UNKNOWN = 0;
	private static final int TENTATIVELYINS = 1;
	private static final int INS = 2;
	private static final int NOTINS = 3;
	
	//f0 colour, f1 type, f2 degree
	
    private int maxIterations;

    public GraphColouring(int maxIterations) {
        this.maxIterations = maxIterations;
    }

    @Override
    public Graph<K, Tuple3<Integer, Integer, Integer>, NullValue> run(Graph<K, Tuple3<Integer, Integer, Integer>, NullValue> network) {
        return network.runVertexCentricIteration(new VertexDegreeInit1<K>(), new VertexDegreeInitM1<K>(), maxIterations);
    	//return null;
    	//return network.runVertexCentricIteration(
                //new VertexRankUpdater<K>(numVertices, beta),
                //new RankMessenger<K>(),
                //maxIterations
        //);
    }

    //INIT 1 and 2 should only execute once per colour and INIT 1 should not receive anything
    @SuppressWarnings("serial")
    public static final class VertexDegreeInit1<K extends Comparable<K> & Serializable> 
    	extends VertexUpdateFunction<K, Tuple3<Integer, Integer, Integer>, NullValue> {

		@Override
		public void updateVertex(K vertexKey,
				Tuple3<Integer, Integer, Integer> vertexValue,
				MessageIterator<NullValue> inMessages) throws Exception {
			if (vertexValue.f0.intValue() == -1) {
				Tuple3<Integer, Integer, Integer> newState = vertexValue.copy();
				newState.f1 = UNKNOWN;
				setNewVertexValue(newState);
			}
		}
    }
    
    @SuppressWarnings("serial")
    public static final class VertexDegreeInitM1<K extends Comparable<K> & Serializable> 
		extends MessagingFunction<K, Tuple3<Integer, Integer, Integer>, NullValue, NullValue> {

		@Override
		public void sendMessages(K vertexKey,
				Tuple3<Integer, Integer, Integer> vertexValue) throws Exception {
			if (vertexValue.f0.intValue() == -1 && vertexValue.f1.intValue() == UNKNOWN) {
				sendMessageToAllNeighbors(new NullValue());
			}
		}
    }
	
	@SuppressWarnings("serial")
    public static final class VertexDegreeInit2<K extends Comparable<K> & Serializable> 
		extends VertexUpdateFunction<K, Tuple3<Integer, Integer, Integer>, NullValue> {

		@Override
		public void updateVertex(K vertexKey,
				Tuple3<Integer, Integer, Integer> vertexValue,
				MessageIterator<NullValue> inMessages) throws Exception {		
			if (vertexValue.f0.intValue() == -1) {
				int degree = 0;
				while (inMessages.hasNext()) {
					inMessages.next();
					degree++;
				}
				Tuple3<Integer, Integer, Integer> newState = vertexValue.copy();
				newState.f2 = degree;
				setNewVertexValue(newState);
			}
		}
	}
	
	@SuppressWarnings("serial")
    public static final class VertexDegreeInitM2<K extends Comparable<K> & Serializable> 
		extends MessagingFunction<K, Tuple3<Integer, Integer, Integer>, NullValue, NullValue> {

		@Override
		public void sendMessages(K vertexKey,
				Tuple3<Integer, Integer, Integer> vertexValue) throws Exception {
			if (vertexValue.f0.intValue() == -1 && vertexValue.f1.intValue() == UNKNOWN) {
				sendMessageToAllNeighbors(new NullValue());
			}
		}
	}
	
	// This should not receive any message
	@SuppressWarnings("serial")
    public static final class VertexSelection<K extends Comparable<K> & Serializable> 
		extends VertexUpdateFunction<K, Tuple3<Integer, Integer, Integer>, NullValue> {

		@Override
		public void updateVertex(K vertexKey,
				Tuple3<Integer, Integer, Integer> vertexValue,
				MessageIterator<NullValue> inMessages) throws Exception {	
			if (vertexValue.f0.intValue() == -1 && vertexValue.f1.intValue() == UNKNOWN) {
				Random r = new Random();
				if (r.nextDouble() < ((double) 1 /((double) 2 * vertexValue.f2.doubleValue()))) {
					Tuple3<Integer, Integer, Integer> newState = vertexValue.copy();
					newState.f1 = TENTATIVELYINS;
					setNewVertexValue(newState);
				}
			}
		}
	}
	
	@SuppressWarnings("serial")
    public static final class VertexSelectionM<K extends Comparable<K> & Serializable> 
		extends MessagingFunction<K, Tuple3<Integer, Integer, Integer>, Integer, NullValue> {
	
		@Override
		public void sendMessages(K vertexKey,
				Tuple3<Integer, Integer, Integer> vertexValue) throws Exception {
			if (vertexValue.f0.intValue() == -1) {
				if (vertexValue.f1.intValue() == TENTATIVELYINS) {
					sendMessageToAllNeighbors((Integer) vertexKey);
				}
			}
		}
	}
	
	@SuppressWarnings("serial")
    public static final class VertexConflictResolution<K extends Comparable<K> & Serializable> 
		extends VertexUpdateFunction<K, Tuple3<Integer, Integer, Integer>, Integer> {
	
		@Override
		public void updateVertex(K vertexKey,
				Tuple3<Integer, Integer, Integer> vertexValue,
				MessageIterator<Integer> inMessages) throws Exception {
			if (vertexValue.f0.intValue() == -1 && vertexValue.f1.intValue() == TENTATIVELYINS) {
				boolean amMinimum = true;
				while (inMessages.hasNext()) {
					if (inMessages.next().compareTo((Integer) vertexKey) <= 0) { //the incoming is lower
						amMinimum = false;
						break;
					}
				}
				Tuple3<Integer, Integer, Integer> newState = vertexValue.copy();
				newState.f1 = amMinimum ? INS : UNKNOWN;
				setNewVertexValue(newState);
			}
		}
	}
	
	@SuppressWarnings("serial")
    public static final class VertexConflictResolutionM<K extends Comparable<K> & Serializable> 
		extends MessagingFunction<K, Tuple3<Integer, Integer, Integer>, NullValue, NullValue> {
	
		@Override
		public void sendMessages(K vertexKey,
				Tuple3<Integer, Integer, Integer> vertexValue) throws Exception {
			if (vertexValue.f0.intValue() == -1) {
				if (vertexValue.f1.intValue() == INS) {
					sendMessageToAllNeighbors(new NullValue());
				}
			}
		}
	}
	
	@SuppressWarnings("serial")
    public static final class VertexDiscovery<K extends Comparable<K> & Serializable> 
		extends VertexUpdateFunction<K, Tuple3<Integer, Integer, Integer>, NullValue> {
	
		@Override
		public void updateVertex(K vertexKey,
				Tuple3<Integer, Integer, Integer> vertexValue,
				MessageIterator<NullValue> inMessages) throws Exception {
			if (vertexValue.f0.intValue() == -1 && vertexValue.f1.intValue() == UNKNOWN) {
				if (inMessages.hasNext()) {
					Tuple3<Integer, Integer, Integer> newState = vertexValue.copy();
					newState.f1 = NOTINS;
					setNewVertexValue(newState);
				}
			}
		}
	}
	
	@SuppressWarnings("serial")
    public static final class VertexDiscoveryM<K extends Comparable<K> & Serializable> 
		extends MessagingFunction<K, Tuple3<Integer, Integer, Integer>, NullValue, NullValue> {
	
		@Override
		public void sendMessages(K vertexKey,
				Tuple3<Integer, Integer, Integer> vertexValue) throws Exception {
			if (vertexValue.f0.intValue() == -1 && vertexValue.f1.intValue() == INS) {
				sendMessageToAllNeighbors(new NullValue());
			}
		}
	}
	
    @SuppressWarnings("serial")
    public static final class VertexDegreeAdjusting<K extends Comparable<K> & Serializable> 
		extends VertexUpdateFunction<K, Tuple3<Integer, Integer, Integer>, NullValue> {
	
		@Override
		public void updateVertex(K vertexKey,
				Tuple3<Integer, Integer, Integer> vertexValue,
				MessageIterator<NullValue> inMessages) throws Exception {
			if (vertexValue.f0.intValue() == -1 && vertexValue.f1.intValue() == UNKNOWN) {
				int degree = 0;
				while (inMessages.hasNext()) {
					inMessages.next();
					degree++;
				}
				Tuple3<Integer, Integer, Integer> newState = vertexValue.copy();
				newState.f2 -= degree;
				setNewVertexValue(newState);
			}
		}
	}
	
    @SuppressWarnings("serial")
	public static final class VertexDegreeAdjustingM<K extends Comparable<K> & Serializable> 
		extends MessagingFunction<K, Tuple3<Integer, Integer, Integer>, NullValue, NullValue> {
	
		@Override
		public void sendMessages(K vertexKey,
				Tuple3<Integer, Integer, Integer> vertexValue) throws Exception {
			//USELESS, because it should not send anything
		}
	}
    
    @SuppressWarnings("serial")
    public static final class VertexColourAssignment<K extends Comparable<K> & Serializable> 
		extends VertexUpdateFunction<K, Tuple3<Integer, Integer, Integer>, NullValue> {
	
		@Override
		public void updateVertex(K vertexKey,
				Tuple3<Integer, Integer, Integer> vertexValue,
				MessageIterator<NullValue> inMessages) throws Exception {
			if (vertexValue.f0.intValue() == -1) {
				if (vertexValue.f1.intValue() == INS) {
					Tuple3<Integer, Integer, Integer> newState = vertexValue.copy();
					newState.f0 = 2; //TODO
					setNewVertexValue(newState);
				}
				else {
					Tuple3<Integer, Integer, Integer> newState = vertexValue.copy();
					newState.f1 = UNKNOWN;
					setNewVertexValue(newState);
				}
			}
		}
	}
	
	@SuppressWarnings("serial")
    public static final class VertexColourAssignmentM<K extends Comparable<K> & Serializable> 
		extends MessagingFunction<K, Tuple3<Integer, Integer, Integer>, NullValue, NullValue> {
	
		@Override
		public void sendMessages(K vertexKey,
				Tuple3<Integer, Integer, Integer> vertexValue) throws Exception {
			//USELESS
		}
	}
}
