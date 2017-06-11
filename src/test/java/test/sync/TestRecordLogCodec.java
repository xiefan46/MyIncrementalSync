package test.sync;

import java.io.File;
import java.io.RandomAccessFile;

import com.alibaba.middleware.race.sync.ChannelReader;
import com.alibaba.middleware.race.sync.Constants;
import com.alibaba.middleware.race.sync.channel.RAFInputStream;
import com.alibaba.middleware.race.sync.channel.ReadChannel;
import com.alibaba.middleware.race.sync.channel.SimpleReadChannel;
import com.alibaba.middleware.race.sync.model.RecordLog;

/**
 * @author wangkai
 */
public class TestRecordLogCodec {

	public static void main(String[] args) throws Exception {

//		File file = new File(Constants.DATA_HOME+"/1.txt");
//		File file = new File(Constants.TESTER_HOME+"/canal.txt");
		File file = new File(Constants.TESTER_HOME+"/123.txt");
		
		RandomAccessFile raf = new RandomAccessFile(file,"r");

		RAFInputStream inputStream = new RAFInputStream(raf);

		ReadChannel channel = new SimpleReadChannel(inputStream, 1024 * 1024 * 1);

		ChannelReader reader = ChannelReader.get();

		byte[] cs = "middleware3|student".getBytes();

		int all = 0;
		
		long old = System.currentTimeMillis();
		
		for (; channel.hasRemaining();) {
			
			RecordLog r = reader.read(channel, cs,10);
			if (r == null) {
//				System.out.println("------------------");
				continue;
			}
			all++;
//			System.out.println(JSONObject.toJSONString(r));
		}
		
		System.out.println("time:"+(System.currentTimeMillis() - old));
		
		System.out.println(all);
	}
}
