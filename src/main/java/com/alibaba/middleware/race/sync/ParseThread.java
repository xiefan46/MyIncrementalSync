package com.alibaba.middleware.race.sync;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import com.alibaba.middleware.race.sync.model.Node;
import com.alibaba.middleware.race.sync.model.RecordLog;
import com.alibaba.middleware.race.sync.model.Table;
import com.generallycloud.baseio.buffer.ByteBuf;

public class ParseThread extends WorkThread {

	private Node<RecordLog>	rootRecord1;
	
	private Node<RecordLog>	rootRecord2;
	
	private boolean		use1;
	
	private Node<RecordLog>	result;
	
	private boolean 		done;
	
	private BlockingQueue<ByteBuf> bufs = new ArrayBlockingQueue<>(2);

	private int			limit;
	
	private Context		context;

	private ByteBufReader	byteBufReader	= new ByteBufReader();

	public ParseThread(Context context, int index) {
		super("parse-", index);
		this.context = context;
		this.setWork(true);
	}

	protected void work() throws Exception {
		BlockingQueue<ByteBuf> bufs = this.bufs;
		ByteBuf buf = bufs.poll(16, TimeUnit.MICROSECONDS);
		if (buf == null) {
			return;
		}
		if (done) {
			wait4Work();
		}
		Table table = context.getTable();
		byte[] tableSchema = table.getTableSchemaBytes();
		int cols = table.getColumnSize();
		ByteBufReader reader = this.byteBufReader;
		if (!buf.hasRemaining()) {
			this.done = true;
			return;
		}
		Node<RecordLog> result;
		Node<RecordLog> cr;
		if (use1) {
			use1 = false;
			if (rootRecord1 == null) {
				rootRecord1 = new Node<>();
				rootRecord1.setValue(RecordLog.newRecordLog(cols));
			}
			result = rootRecord1;
			cr = rootRecord1;
		}else{
			use1 = true;
			if (rootRecord2 == null) {
				rootRecord2 = new Node<>();
				rootRecord2.setValue(RecordLog.newRecordLog(cols));
			}
			result = rootRecord2;
			cr = rootRecord2;
		}
		limit = 1;
		RecordLog crv = cr.getValue();
		crv.reset();
		reader.read(table, buf, tableSchema, crv);
		for (; buf.hasRemaining();) {
			cr = getNext(cr, cols);
			crv = cr.getValue();
			crv.reset();
			reader.read(table, buf, tableSchema, crv);
		}
		context.getByteBufPool().free(buf);
		this.result = result;
		this.done = true;
	}

	private Node<RecordLog> getNext(Node<RecordLog> node, int cols) {
		limit++;
		Node<RecordLog> next = node.getNext();
		if (next == null) {
			next = new Node<>();
			next.setValue(RecordLog.newRecordLog(cols));
			node.setNext(next);
			return next;
		}
		return next;
	}

	public int getLimit() {
		return limit;
	}

	/**
	 * @return the rootRecord
	 */
	public Node<RecordLog> getResult() {
		return result;
	}
	
	public void offerBuf(ByteBuf buf){
		bufs.offer(buf);
	}
	
	public boolean isDone() {
		return done;
	}
	
	@Override
	public void startWork() {
		done = false;
		limit = 0;
		super.startWork();
	}
	

}
