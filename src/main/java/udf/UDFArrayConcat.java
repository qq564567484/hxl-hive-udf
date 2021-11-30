package udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;

import java.util.ArrayList;
import java.util.List;


/**
 * array对象的拼接,按第一个数组的数值类型来返回,忽略NULL值,不去重复
 */
@Description(name = "udf_array_concat",
             value = "_FUNC_(array1, array2, array3, .....) - Concatenates the array arguments")

public class UDFArrayConcat extends GenericUDF {
  private transient ListObjectInspector arrayOI = null;
  private transient ObjectInspectorConverters.Converter converters[];

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    converters = new ObjectInspectorConverters.Converter[arguments.length];

    for (int i = 0; i < arguments.length; ++i) {
      if (i == 0) {
        arrayOI = (ListObjectInspector)ObjectInspectorUtils
            .getStandardObjectInspector(arguments[i]);
      }
      converters[i] = ObjectInspectorConverters.getConverter(arguments[i], arrayOI);
    }
    return arrayOI;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    ArrayList<Object> resultArray = null;
    for (int i = 0; i < arguments.length; ++i) {
      List<?> array = (List<?>)converters[i].convert(arguments[i].get());
      if (array == null) {
        continue;
      }

      if (resultArray == null) {
        resultArray = new ArrayList<Object>(array);
      } else {
        resultArray.addAll(array);
      }
    }
    return resultArray;
  }

    @Override
    public String getDisplayString(String[] children) {
	  return "";
    }
}
