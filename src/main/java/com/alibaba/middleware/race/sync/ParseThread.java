package com.alibaba.middleware.race.sync;

import com.alibaba.middleware.race.sync.model.Node;
import com.alibaba.middleware.race.sync.model.RecordLog;
import com.alibaba.middleware.race.sync.model.Table;
import com.generallycloud.baseio.buffer.ByteBuf;

public class ParseThread extends WorkThread {

	private ByteBuf		buf;

	private Node<RecordLog>	rootRecord;

	private int			limit;

	private ReaderThread	readerThread;

	private ByteBufReader	byteBufReader	= new ByteBufReader();

	private Dispatcher		dispatcher;

	public ParseThread(ReaderThread readerThread, ByteBuf buf, int index) {
		super("parse-", index);
		this.buf = buf;
		this.readerThread = readerThread;
		this.dispatcher = readerThread.getContext().getDispatcher();
	}

	protected void work() throws Exception {
		ReaderThread readerThread = this.readerThread;
		ByteBuf buf = this.buf;
		Context context = readerThread.getContext();
		Table table = context.getTable();
		byte[] tableSchema = table.getTableSchemaBytes();
		int cols = table.getColumnSize();
		ByteBufReader reader = this.byteBufReader;
		if (!buf.hasRemaining()) {
			return;
		}
		if (rootRecord == null) {
			rootRecord = new Node<>();
			rootRecord.setValue(RecordLog.newRecordLog(cols));
		}
		limit = 1;
		Node<RecordLog> cr = rootRecord;
		RecordLog crv = cr.getValue();
		crv.reset();
		reader.read(table, buf, tableSchema, crv);
		for (; buf.hasRemaining();) {
			cr = getNext(cr, cols);
			crv = cr.getValue();
			crv.reset();
			reader.read(table, buf, tableSchema, crv);
		}
		readerThread.parseDone(getIndex());
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

//	public void dispatch() throws InterruptedException {
//		int limit = this.limit;
//		Dispatcher dispatcher = this.dispatcher;
//		Node<RecordLog> r = this.rootRecord;
//		for (int i = 0; i < limit; i++) {
//			dispatcher.dispatch(r.getValue());
//			r = r.getNext();
//		}
//	}

	public ByteBuf getBuf() {
		return buf;
	}

	public void setBuf(ByteBuf buf) {
		this.buf = buf;
	}

	public int getLimit() {
		return limit;
	}

	/**
	 * @return the rootRecord
	 */
	public Node<RecordLog> getRootRecord() {
		return rootRecord;
	}

}
