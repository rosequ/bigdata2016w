package ca.uwaterloo.cs.bigdata2016w.xeniaqian94.assignment4;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import tl.lin.data.array.ArrayListOfFloatsWritable;
import tl.lin.data.array.ArrayListOfIntsWritable;

/**
 * Representation of a graph node for PageRank.
 *
 * @author Jimmy Lin
 * @author Michael Schatz
 */
public class PageRankNodeMultisource implements Writable {
	public static enum Type {
		Complete((byte) 0), // PageRank mass and adjacency list.
		Mass((byte) 1), // PageRank mass only.
		Structure((byte) 2); // Adjacency list only.

		public byte val;

		private Type(byte v) {
			this.val = v;
		}
	};

	private static final Type[] mapping = new Type[] { Type.Complete, Type.Mass, Type.Structure };

	private Type type;
	private int nodeid;
	private ArrayListOfFloatsWritable pagerank;
	private ArrayListOfIntsWritable adjacenyList;

	public PageRankNodeMultisource() {
	}

	public float getPageRank(int i) {
		return pagerank.get(i);
	}
	
	public ArrayListOfFloatsWritable getPageRankArray() {
		return pagerank;
	}

	public void setPageRankArray(ArrayListOfFloatsWritable p) {
		this.pagerank = p;
	}

	public void setPageRank(Float p, int i) {
		this.pagerank.set(i, p);
	}

	public int getNodeId() {
		return nodeid;
	}

	public void setNodeId(int n) {
		this.nodeid = n;
	}

	public ArrayListOfIntsWritable getAdjacenyList() {
		return adjacenyList;
	}

	public void setAdjacencyList(ArrayListOfIntsWritable list) {
		this.adjacenyList = list;
	}

	public Type getType() {
		return type;
	}

	public void setType(Type type) {
		this.type = type;
	}

	/**
	 * Deserializes this object.
	 *
	 * @param in
	 *            source for raw byte representation
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		int b = in.readByte();
		type = mapping[b];
		nodeid = in.readInt();
		
		pagerank=new ArrayListOfFloatsWritable();

		if (type.equals(Type.Mass)) {
			pagerank.readFields(in);
			return;
		}

		if (type.equals(Type.Complete)) {
			pagerank.readFields(in);
		}

		adjacenyList = new ArrayListOfIntsWritable();
		adjacenyList.readFields(in);
	}

	/**
	 * Serializes this object.
	 *
	 * @param out
	 *            where to write the raw byte representation
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeByte(type.val);
		out.writeInt(nodeid);
		pagerank.write(out);

		if (type.equals(Type.Mass)) {
			return;
		}


		adjacenyList.write(out);
	}

	@Override
	public String toString() {
		return String.format("{%d %s %s}", nodeid, (pagerank == null ? "[]" : pagerank.toString(15)),
				(adjacenyList == null ? "[]" : adjacenyList.toString(10)));
	}

	/**
	 * Returns the serialized representation of this object as a byte array.
	 *
	 * @return byte array representing the serialized representation of this
	 *         object
	 * @throws IOException
	 */
	public byte[] serialize() throws IOException {
		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(bytesOut);
		write(dataOut);

		return bytesOut.toByteArray();
	}

	/**
	 * Creates object from a <code>DataInput</code>.
	 *
	 * @param in
	 *            source for reading the serialized representation
	 * @return newly-created object
	 * @throws IOException
	 */
	public static PageRankNodeMultisource create(DataInput in) throws IOException {
		PageRankNodeMultisource m = new PageRankNodeMultisource();
		m.readFields(in);

		return m;
	}

	/**
	 * Creates object from a byte array.
	 *
	 * @param bytes
	 *            raw serialized representation
	 * @return newly-created object
	 * @throws IOException
	 */
	public static PageRankNodeMultisource create(byte[] bytes) throws IOException {
		return create(new DataInputStream(new ByteArrayInputStream(bytes)));
	}
}
