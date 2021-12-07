package udf;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;


/**
 * 2个入参,array_1,array2,返回两个数组的交集
 */
@Description(name = "udf_array_intersect",
             value = "_FUNC_(values, indices) - return two array's intersec result.")
public class UDFArrayIntersect extends GenericUDF {
  private transient ListObjectInspector firstArrayOI;
  private transient ListObjectInspector secondArrayOI;
  private transient ObjectInspectorConverters.Converter[] converter;


  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments)
    throws UDFArgumentException {

    if(arguments.length != 2){
      throw  new UDFArgumentLengthException("require 2 params ( array(value),array(index) ) , now param length is " + arguments.length);
    }

    if(!arguments[0].getCategory().equals(ObjectInspector.Category.LIST)
            || !arguments[1].getCategory().equals(ObjectInspector.Category.LIST)
    ){
      throw new UDFArgumentException("require 2 params ( array(value),array(index) ) , now is ( " +
              arguments[0].getTypeName() + " , " + arguments[1].getTypeName()
      );
    }

    firstArrayOI = (ListObjectInspector) ObjectInspectorUtils
            .getStandardObjectInspector(arguments[0]);

    secondArrayOI = (ListObjectInspector) ObjectInspectorUtils
            .getStandardObjectInspector(arguments[1]);

    converter = new ObjectInspectorConverters.Converter[arguments.length];

    for (int i = 0; i < arguments.length; ++i) {
      converter[i] = ObjectInspectorConverters.getConverter(arguments[i], firstArrayOI);
    }

    return firstArrayOI;

  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    List<?> firstArray = (List<?>)converter[0].convert(firstArrayOI.getList(arguments[0].get()));

    List<?> secondArray = (List<?>)converter[1].convert(secondArrayOI.getList(arguments[1].get()));

    if (firstArray == null || secondArray == null) {
      return null;
    }


    Sets.SetView<?> view = Sets.intersection(Sets.newHashSet(firstArray), Sets.newHashSet(secondArray));

    ArrayList<Object> intersectList = new ArrayList<>();

    intersectList.addAll(Sets.newHashSet(view));

    return intersectList;
  }

  @Override
  public String getDisplayString(String[] input) {
	  return "";
  }
}
