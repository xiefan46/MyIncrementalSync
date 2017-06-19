package test.sync;

import java.io.File;
import java.io.IOException;

import com.alibaba.middleware.race.sync.ChannelReader2;
import com.alibaba.middleware.race.sync.Constants;
import com.alibaba.middleware.race.sync.channel.MuiltFileReadChannelSplitor;
import com.alibaba.middleware.race.sync.channel.ReadChannel;
import com.alibaba.middleware.race.sync.model.RecordLog;

/**
 * @author wangkai
 */
public class TestRecordLogCodec {
	
	private static ReadChannel initChannels2() throws IOException {
		File root = new File(Constants.DATA_HOME);
		return MuiltFileReadChannelSplitor.newChannel(root.getAbsolutePath() + "/", 1, 10,
				1024 * 256);
	}

	public static void main(String[] args) throws Exception {

		ReadChannel channel = initChannels2();

		ChannelReader2 reader = ChannelReader2.get();

		byte[] cs = "middleware3|student".getBytes();

		int all = 0;
		
		long old = System.currentTimeMillis();
		
		RecordLog r = new RecordLog();
		r.newColumns(10);
		
		for (; channel.hasBufRemaining();) {
			
			reader.read(channel, cs,r);
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
