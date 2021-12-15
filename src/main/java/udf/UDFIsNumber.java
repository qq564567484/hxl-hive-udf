package udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaBooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * Returns TRUE if this is a real number (not INF, not NAN), NULL if null.
 */
@Description(name = "udf_is_number",
             value = "_FUNC_(num) - Return TRUE if num is finite and a number.")

public class UDFIsNumber extends GenericUDF{

    private transient ObjectInspectorConverters.Converter converter;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

        if (arguments.length != 1) {
            throw new UDFArgumentLengthException(getFuncName() + " requires 1 argument, got "
                    + arguments.length);
        }

        if (arguments[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentException(getFuncName() + " only takes primitive types, got "
                    + arguments[0].getTypeName());
        }

        JavaBooleanObjectInspector resultOI = PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;

        converter = ObjectInspectorConverters.getConverter(arguments[0], PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);


        return resultOI;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {

        if(null == deferredObjects[0]){
            return null;
        }else{
            Object convert = converter.convert(deferredObjects[0].get());
            if(null == convert){
                return false;
            }else{
                Double o = (double) converter.convert(deferredObjects[0].get());
                return !o.isNaN() && !o.isInfinite();
            }
        }
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "";
    }
}
