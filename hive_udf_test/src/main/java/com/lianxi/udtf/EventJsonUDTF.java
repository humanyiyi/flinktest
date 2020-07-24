package com.lianxi.udtf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONException;

import java.util.ArrayList;
import java.util.List;

public class EventJsonUDTF extends GenericUDTF {
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        List<String> eventName = new ArrayList<>(2);
        List<ObjectInspector> eventType = new ArrayList<>(2);

        eventName.add("event_name");
        eventName.add("event_json");
        eventType.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        eventType.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(eventName, eventType);
    }

    @Override
    //[{"ett":"1583705574227","en":"display","kv":{"goodsid":"0","action":"1","extend1":"1","place":"0","category":"63"}},{"ett":"1583760986259","en":"loading","kv":{"extend2":"","loading_time":"4","action":"3","extend1":"","type":"3","type1":"","loading_way":"1 "}},{"ett":"1583746639124","en":"ad","kv":{"activityId":"1","displa yMills":"111839","entry":"1","action":"5","contentType":"0"}},{"ett ":"1583758016208","en":"notification","kv":{"ap_time":"1583694079866","action":"1","type":"3","content":""}},{"ett":"1583699890760","en":"favorites","kv":{"course_id":4,"id":0,"add_time":"1583730648134","u serid":7}}]
    public void process(Object[] objects) throws HiveException {
        String input = objects[0].toString();
        String[] result = new String[2];
        if (StringUtils.isBlank(input)){
            return;
        }else {
            try {
                JSONArray jsonArray = new JSONArray(input);
                if (jsonArray == null) {
                    return;
                }
                for (int i = 0; i < jsonArray.length(); i++){
                    try {
                        result[0] = jsonArray.getJSONObject(i).getString("en");
                        result[1] = jsonArray.getString(i);
                    }catch (JSONException e){
                        continue;
                    }

                    forward(result);
                }
            }catch (JSONException e){
                e.printStackTrace();
            }
        }

    }

    @Override
    public void close() throws HiveException {

    }
}
