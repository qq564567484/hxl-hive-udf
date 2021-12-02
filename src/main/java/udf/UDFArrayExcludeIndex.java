package udf;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;


/**
 * 2个入参,array_1,array2,按array2里的下标(从0开始)删除第一个array1中数据并返回一个array
 */
@Description(name = "udf_array_exclude_index",
             value = "_FUNC_(values, indices) - Removes elements of 'values' whose indices are in 'indices'.")
public class UDFArrayExcludeIndex extends GenericUDF {
  private transient ListObjectInspector valueArrayOI;
  private transient ListObjectInspector indexArrayOI;
  private transient ObjectInspectorConverters.Converter converter;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments)
    throws UDFArgumentException {

    if(arguments.length != 2){
      throw  new UDFArgumentLengthException("require 2 params ( array(value),array(index) ) , now param length is " + arguments.length);
    }

    valueArrayOI = (ListObjectInspector) ObjectInspectorUtils
            .getStandardObjectInspector(arguments[0]);

    indexArrayOI = (ListObjectInspector) ObjectInspectorUtils
            .getStandardObjectInspector(arguments[1]);

    ObjectInspector elementOI = indexArrayOI.getListElementObjectInspector();

    converter = ObjectInspectorConverters.getConverter(elementOI,PrimitiveObjectInspectorFactory.javaIntObjectInspector);

    return valueArrayOI;

  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    List<?> valueArray = valueArrayOI.getList(arguments[0].get());

    List<?> indexArray = indexArrayOI.getList(arguments[1].get());

    if (valueArray == null) {
      return null;
    }

    int size = valueArray.size();
    // Avoid using addAll or the constructor here because that will cause
    // an unchecked cast warning on List<?>.
    HashSet<Integer> indices = Sets.newHashSet();
    for (Object o : indexArray) {
      if (null != o && (Integer) converter.convert(o) <= size - 1) {
        indices.add((Integer) converter.convert(o));
      }
    }

    ArrayList<Object> arrayList = Lists.newArrayList();
    for (int i = 0; i < size; i++) {
      if(!indices.contains(i)){
        arrayList.add(valueArray.get(i));
      }
    }

    return arrayList;
  }

  @Override
  public String getDisplayString(String[] input) {
	  return "";
  }
}
