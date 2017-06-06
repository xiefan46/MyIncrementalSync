package test.sync;

import java.io.File;
import java.io.RandomAccessFile;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.middleware.race.sync.ChannelReader;
import com.alibaba.middleware.race.sync.RAFInputStream;
import com.alibaba.middleware.race.sync.ReadChannel;
import com.alibaba.middleware.race.sync.SimpleReadChannel;
import com.alibaba.middleware.race.sync.model.Record;

/**
 * @author wangkai
 */
public class TestRecordLogCodec {

	public static void main(String[] args) throws Exception {

		RandomAccessFile file = new RandomAccessFile(new File("D:/GIT/MyIncrementalSync/01.txt"),
				"r");

		RAFInputStream inputStream = new RAFInputStream(file);

		ReadChannel channel = new SimpleReadChannel(inputStream, 128);

		ChannelReader reader = ChannelReader.get();

		byte[] cs = "test|user".getBytes();

		for (; channel.hasRemaining();) {

			Record r = reader.read(channel, cs, 0, 9999);
			if (r == null) {
				continue;
			}
			System.out.println(JSONObject.toJSONString(r));
		}

	}
}
