package udf;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

/**
 * _FUNC_(array1,array2)
 * 按array2的值在array1中删除,例如 _FUNC_(array(1,2,3),array(1,2)) = array(3)
 */
@Description(name = "udf_array_exclude_value",
             value = "_FUNC_(array1,array2) - exclude the item in array2 and also in array1.Note that ordering will be lost.")

public class UDFArrayExcludeValue extends GenericUDF {

  private transient ListObjectInspector sourceArrayOI;
  private transient ListObjectInspector excludeArrayOI;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

    if(arguments.length != 2){
      throw  new UDFArgumentLengthException("require 2 params ( array(value),array(index) ) , now param length is " + arguments.length);
    }


    sourceArrayOI = (ListObjectInspector) ObjectInspectorUtils
            .getStandardObjectInspector(arguments[0]);

    excludeArrayOI = (ListObjectInspector) ObjectInspectorUtils
            .getStandardObjectInspector(arguments[1]);




    //只支持原始数据类型且两个集合元素类型要一致
    if(sourceArrayOI.getCategory().equals(ObjectInspector.Category.LIST)
      && excludeArrayOI.getCategory().equals(ObjectInspector.Category.LIST)
      && sourceArrayOI.getListElementObjectInspector().getCategory().equals(ObjectInspector.Category.PRIMITIVE)
      && excludeArrayOI.getListElementObjectInspector().getCategory().equals(ObjectInspector.Category.PRIMITIVE)
      && sourceArrayOI.getListElementObjectInspector().getTypeName().equals(excludeArrayOI.getListElementObjectInspector().getTypeName())
    ){
      return sourceArrayOI;
    }else{
      throw new UDFArgumentException("check input params, only support list with primitive category , but now is "
              + " sourceArray is " + sourceArrayOI.getTypeName()
              + " excludeArray is " + excludeArrayOI.getTypeName()
      );
    }
  }

  @Override
  public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
    List<?> sourceArray = (List<?>) deferredObjects[0].get();
    List<?> excludeArray = (List<?>) deferredObjects[1].get();

    if(null == sourceArray){
      return null;
    }else if(null == excludeArray){
      return sourceArray;
    }

    HashSet<?> set1 = Sets.newHashSet(sourceArray.iterator());
    HashSet<?> set2 = Sets.newHashSet(excludeArray.iterator());

    Sets.SetView<?> difference = Sets.difference(set1, set2);
    HashSet<?> resultSet = Sets.newHashSet(difference.iterator());

    return new ArrayList<>(resultSet);
  }

  @Override
  public String getDisplayString(String[] strings) {
    return "";
  }


}
