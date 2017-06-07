package test.sync;

import java.io.File;
import java.io.RandomAccessFile;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.middleware.race.sync.ChannelReader;
import com.alibaba.middleware.race.sync.Constants;
import com.alibaba.middleware.race.sync.RAFInputStream;
import com.alibaba.middleware.race.sync.ReadChannel;
import com.alibaba.middleware.race.sync.SimpleReadChannel;
import com.alibaba.middleware.race.sync.model.Record;

/**
 * @author wangkai
 */
public class TestRecordLogCodec {

	public static void main(String[] args) throws Exception {

		File file = new File(Constants.DATA_HOME+"/canal.txt");
		
		RandomAccessFile raf = new RandomAccessFile(file,"r");

		RAFInputStream inputStream = new RAFInputStream(raf);

		ReadChannel channel = new SimpleReadChannel(inputStream, 1024 * 128);

		ChannelReader reader = ChannelReader.get();

		byte[] cs = "middleware3|student".getBytes();

		int all = 0;
		
		long old = System.currentTimeMillis();
		
		for (; channel.hasRemaining();) {

			Record r = reader.read(channel, cs, 0, Long.MAX_VALUE);
			if (r == null) {
				continue;
			}
			all++;
//			System.out.println(JSONObject.toJSONString(r));
		}
		
		System.out.println("time:"+(System.currentTimeMillis() - old));
		
		System.out.println(all);
	}
}
