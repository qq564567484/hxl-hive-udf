package udf;

import org.apache.hadoop.hive.ql.exec.*;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

/**
 * 返回参数内的最大值,会自动忽略NULL
 */
@Description(name = "udf_args_min",
        value = "_FUNC_(param1, parm2, ...) - Find the smallest value")

public class UDFArgsMin extends GenericUDF {


  private transient ObjectInspector[] argumentOIs;
  private transient ObjectInspectorConverters.Converter[] converters;
  private transient ObjectInspector resultOI;

  /**
   * 1.initialize方法只调用一次,并且在evaluate方法调用前调用,该方法接受一个argument数组
   * 2.此方法检查接受正确的参数类型和参数个数
   * 3.定义输出类型
   * @param arguments
   * @return
   * @throws UDFArgumentException
   */
  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

    //判断入参的长度情况
    if (arguments.length < 2) {
      throw new UDFArgumentLengthException(getFuncName() + " requires at least 2 arguments, got "
              + arguments.length);
    }

    //判断入参的类型是不是PRIMITIVE类型,不是抛出异常
    if (arguments[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentException(getFuncName() + " only takes primitive types, got "
              + arguments[0].getTypeName());
    }

    argumentOIs = arguments;

    //设定不同objectinspector的类型转换器
    converters = new ObjectInspectorConverters.Converter[arguments.length];

    //根据入参的第一个objectinspector获得他的TypeInfo对象
    TypeInfo commonInfo = TypeInfoUtils.getTypeInfoFromObjectInspector(arguments[0]);

    for (int i = 1; i < arguments.length; i++) {
      TypeInfo currInfo = TypeInfoUtils.getTypeInfoFromObjectInspector(arguments[i]);

      //获取一个每个元素都能进行转化的class
      //找不到就为null
      commonInfo = FunctionRegistry.getCommonClassForComparison(currInfo, commonInfo);
    }

    //根据上面得到的TypeInfo对象设定返回值的类型
    resultOI = TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(
            (commonInfo == null) ?
                    TypeInfoFactory.doubleTypeInfo : commonInfo);

    //根据返回值类型和每个参数的ObjectInspector类型,获取相应的类型转换器
    for (int i = 0; i < arguments.length; i++) {
      converters[i] = ObjectInspectorConverters.getConverter(arguments[i], resultOI);
    }

    return resultOI;
  }

  /**
   * 这个方法类似UDF的evaluate()方法。它处理真实的参数，并返回最终结果.
   * @param arguments
   * @return
   * @throws HiveException
   */
  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    //设定初始值
    Object minV = null;

    for (int i = 0; i < arguments.length; i++) {
      Object ai = arguments[i].get();
      if (ai == null) { //NULL if any of the args are nulls
        continue;
      }

      if (minV == null) { //First non-null item.
        minV = converters[i].convert(ai);
        continue;
      }

      Object converted = converters[i].convert(ai);
      if (converted == null) {
        continue;
      }

      //ObjectInspectorUtils.compare 比较两个对象大小
      int result = ObjectInspectorUtils.compare(
              converted, resultOI,
              minV, resultOI);
      if (result < 0) {
        minV = converted;
      }
    }
    return minV;
  }

  @Override
  public String getDisplayString(String[] strings) {
    return "";
  }
}
