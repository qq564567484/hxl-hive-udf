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

import java.util.*;


/**
 * array_1,array2,array3 ... 返回数组之间的交集
 */
@Description(name = "udf_array_intersect",
             value = "_FUNC_(array1, array2, array3, ...) - return array's intersec result.")
public class UDFArrayIntersect extends GenericUDF {
  private transient ListObjectInspector[] argumentListOI;
  private transient ObjectInspectorConverters.Converter[] converter;
  private transient ListObjectInspector commonOI;


  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments)
    throws UDFArgumentException {

    if(arguments.length == 0){
      throw  new UDFArgumentLengthException("require params ( array(value),array(value), ... ) , now param length is " + arguments.length);
    }

    for (ObjectInspector oi : arguments) {
      if(!oi.getCategory().equals(ObjectInspector.Category.LIST)){
        throw new UDFArgumentException("require params ( array(value),array(value), ... ) , now is ( " +
                oi.getTypeName() + " , " + oi.getTypeName()
        );
      }
    }

    argumentListOI = new ListObjectInspector[arguments.length];
    converter = new ObjectInspectorConverters.Converter[arguments.length];

    for (int i = 0; i < arguments.length; i++) {
      argumentListOI[i] = (ListObjectInspector) ObjectInspectorUtils
              .getStandardObjectInspector(arguments[i]);

      if(null == commonOI){
        commonOI = (ListObjectInspector) ObjectInspectorUtils
                .getStandardObjectInspector(arguments[0]);
      }

      converter[i] = ObjectInspectorConverters.getConverter(arguments[i], commonOI);
    }

    return argumentListOI[0];

  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    ArrayList<Set<?>> arrayList = new ArrayList<>(arguments.length);

    for (int i = 0; i < arguments.length; i++) {

      List<?> array = (List<?>)converter[i].convert(commonOI.getList(arguments[i].get()));

      if(null != array){
        arrayList.add(Sets.newHashSet(array.iterator()));
      }
    }

    if(arrayList.size() == 0){
      return null;
    }

    Set<?> intersectionSet = Sets.newHashSet();

    for (Set<?> set: arrayList){
       if(intersectionSet.isEmpty()){
         intersectionSet = set;
       }else{
         Sets.SetView<?> view = Sets.intersection(intersectionSet, set);
         intersectionSet = Sets.newHashSet(view);
       }
    }

    return  Lists.newArrayList(intersectionSet);
  }

  @Override
  public String getDisplayString(String[] input) {
	  return "";
  }
}
