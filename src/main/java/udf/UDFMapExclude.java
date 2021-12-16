package udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;


/**
 * Removes elements of the map (first argument) whose keys are present in the
 * array (second argument).  If either input is NULL then NULL is returned.
 * Any NULLs in the array are ignored. 
 */
@Description(name = "udf_map_exclude",
             value = "_FUNC_(values, indices) - Removes elements of 'values'" +
                     " whose keys are in 'indices'.")
public class UDFMapExclude extends GenericUDF {
  private transient ObjectInspector arrayOI;
  private transient MapObjectInspector mapOI;
  private transient ObjectInspectorConverters.Converter converter;
  private transient ObjectInspector oi;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments)
      throws UDFArgumentException {

    mapOI = (MapObjectInspector)arguments[0];
    arrayOI = (ListObjectInspector)arguments[1];

    if (arguments.length != 2) {
      throw new UDFArgumentLengthException(getFuncName() + " requires 2 argument, got "
              + arguments.length);
    }

    if (arguments[0].getCategory() != ObjectInspector.Category.MAP) {
      throw new UDFArgumentException(getFuncName() + " only takes map types, got "
              + arguments[0].getTypeName());
    }

    mapOI = (MapObjectInspector) ObjectInspectorUtils
            .getStandardObjectInspector(arguments[0]);

    ObjectInspector mapItemOI = mapOI.getMapKeyObjectInspector();

    StandardListObjectInspector listObjectInspector = ObjectInspectorFactory.getStandardListObjectInspector(mapItemOI);
    converter = ObjectInspectorConverters.getConverter(arguments[1], listObjectInspector);

    arrayOI = ObjectInspectorUtils.getStandardObjectInspector(arguments[1]);

    return mapOI;

  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {

    Map<?, ?> valueMap = mapOI.getMap(arguments[0].get());
    List<?> indiciesArray = (List<?>) converter.convert(arguments[1].get());

    if (null == valueMap) {
        return null;
    }else if (null == indiciesArray){
        return valueMap;
    }else{
        for (Object key : indiciesArray) {
          valueMap.remove(key);
        }
    }
    return valueMap;

  }

  @Override
  public String getDisplayString(String[] input) {
	  return "";
  }
}
