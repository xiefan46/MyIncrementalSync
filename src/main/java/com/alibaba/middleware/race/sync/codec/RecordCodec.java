package com.alibaba.middleware.race.sync.codec;

import com.alibaba.middleware.race.sync.model.Record;
import com.alibaba.middleware.race.sync.util.MathUtil;

/**
 * Created by Rudy Steiner on 2017/6/12.
 */
public class RecordCodec {
    /*
    *  columns: such as 4 columns record, 1 byte
    *  column index:                    , 1 byte
    *  column length:                   , 4 byte
    *  column content:                  , real length byte
    *  we suppose a record will not out of 512 byte
    * */
    private final	byte []				array			= new byte[512];
    public int encode(Record record){
        int offset=0;
        byte[][] columnsContent=record.getColumns();
        byte columns=(byte)columnsContent.length;
        array[offset++]=columns;
        for(byte i=0;i<columns;i++){
            if(columnsContent[i]==null||columnsContent[i].length==0)
                continue;
            int len= columnsContent[i].length;
            if(len>0){
                array[offset++]=i;
                MathUtil.int2Byte(array,len,offset);
                offset+=4;
                System.arraycopy(columnsContent[i],0,array,offset,len);
                offset+=len;
            }
        }
        return offset;
    }
   public Record decode(byte[] array){
       if(array==null||array.length==0)
           return null;
       int len=array.length;
       int offset=0;
       int columns=array[offset];
       Record record=new Record(columns);
       offset++;
       while(offset<len){
            int columnIndex=array[offset++];
            int columnLen=MathUtil.byte2Int(array,offset);
            offset+=4;
            byte[] columnContent=new byte[columnLen];
            System.arraycopy(array,offset,columnContent,0,columnLen);
            record.setColum(columnIndex,columnContent);
            offset+=columnLen;
       }
       return  record;
   }
    public byte[] getArray() {
        return array;
    }
}
