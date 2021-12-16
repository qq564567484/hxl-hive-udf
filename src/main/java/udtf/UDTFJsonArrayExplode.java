package udtf;


import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.LinkedList;

@Description(name = "udtf_json_array_explode",
        value = "_FUNC_(json_array_string) - explode json_array and return json and it's index ")
public class UDTFJsonArrayExplode extends GenericUDTF {

    private String[] struct = new String[2];

    private JsonParser jsonParser = new JsonParser();

    private JsonArray jsonArray = null;

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {

        LinkedList<String> colName = Lists.newLinkedList();
        colName.add("json");
        colName.add("index");

        LinkedList<ObjectInspector> resType = Lists.newLinkedList();
        resType.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        resType.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(colName,resType);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        if(args.length != 1 || null == args[0]){
            return;
        }

        String str = args[0].toString();

        try {
            jsonArray = jsonParser.parse(str).getAsJsonArray();
        }catch (Exception e){
            return;
        }

        int index = jsonArray.size();

        for (int i = 0; i < index; i++) {
            String jsonString = jsonArray.get(i).getAsJsonObject().toString();
            struct[0] = jsonString;
            struct[1] = String.valueOf(i+1);
            forward(struct);
        }
    }

    @Override
    public void close() throws HiveException {}
}
