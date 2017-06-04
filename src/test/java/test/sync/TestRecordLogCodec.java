package test.sync;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.middleware.race.sync.codec.RecordLogCodec;
import com.alibaba.middleware.race.sync.model.Record;

/**
 * @author wangkai
 *
 */
public class TestRecordLogCodec {

	public static void main(String[] args) {
		RecordLogCodec codec = new RecordLogCodec();
		String rs = "000001:106|1489133349000|test|user|I|id:1:1||102|name:2:0||ljh|score:1:0||98|\n"
				+ "000001:106|1489133349000|test|user|U|id:1:1|102|102|score:1:0|98|95|\n"
				+ "000001:106|1489133349000|test|user|D|id:1:1|2147483747||name:2:0|ljh||score:1:0|98||";

		byte[] data = rs.getBytes();

		String tableSchema = "test|user";
		
		Record rI = codec.decode(data, 26 + tableSchema.length(), 77,0,1000);
		Record rU = codec.decode(data, 78 + 26 + tableSchema.length(), 146,0,1000);
		Record rD = codec.decode(data, 147 + 26 + tableSchema.length(), 254,0,1000);

		System.out.println(JSONObject.toJSONString(rI));
		System.out.println(JSONObject.toJSONString(rU));
		System.out.println(JSONObject.toJSONString(rD));

		long v = (long) rD.getPrimaryColumn().getValue();

		long l = v & 0xffffffffl;

		System.out.println(l);

		System.out.println(Integer.MAX_VALUE + 100l);

	}
}
