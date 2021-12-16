package udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;

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
  private transient ListObjectInspector arrayOI;
  private transient MapObjectInspector mapOI;
  private transient ObjectInspectorConverters.Converter converter;

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

    if (arguments[1].getCategory() != ObjectInspector.Category.LIST) {
      throw new UDFArgumentException(getFuncName() + " only takes list types, got "
              + arguments[0].getTypeName());
    }


    mapOI = (MapObjectInspector) ObjectInspectorUtils
            .getStandardObjectInspector(arguments[0]);

    arrayOI = (ListObjectInspector) ObjectInspectorUtils
            .getStandardObjectInspector(arguments[1]);


    ObjectInspector mapItemOI = mapOI.getMapKeyObjectInspector();

    ObjectInspector listItemOI = arrayOI.getListElementObjectInspector();

    if (!ObjectInspectorUtils.compareTypes(mapItemOI, listItemOI)) {
      throw new UDFArgumentException("Map key type (" + mapItemOI + ") must match " + 
                                     "list element type (" + listItemOI + ").");
    }

    converter = ObjectInspectorConverters.getConverter(listItemOI,mapItemOI);

    return mapOI;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {

    Map<?, ?> valueMap = mapOI.getMap(arguments[0].get());
    List<?> indiciesArray = arrayOI.getList(arguments[1].get());

    Object[] formattedList = indiciesArray.stream().map(i -> converter.convert(i)).toArray();



    if (null == valueMap) {
      return null;
    }else if (null == formattedList){
      return valueMap;
    }else{
      for (Object key : formattedList) {
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
