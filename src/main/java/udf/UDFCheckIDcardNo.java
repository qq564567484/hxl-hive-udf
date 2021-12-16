package udf;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;


/**
 * check input idCardNo is valid or not
 */
@Description(name = "udf_check_idcard_no",
             value = "_FUNC_(idcard_no) - check input idCardNo is valid or not,return true or false ")
public class UDFCheckIDcardNo extends GenericUDF {
  private transient ObjectInspectorConverters.Converter converter;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments)
      throws UDFArgumentException {


    if (arguments.length != 1) {
      throw new UDFArgumentLengthException(getFuncName() + " requires 1 argument, got "
              + arguments.length);
    }

    converter = ObjectInspectorConverters.getConverter(arguments[0], PrimitiveObjectInspectorFactory.javaStringObjectInspector);

    return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;

  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {

    String idCardNo = (String) converter.convert(arguments[0].get());

    if(StringUtils.isBlank(idCardNo)){
      return false;
    }else if( idCardNo.length() == 18 ){
      boolean idCardflag = false;
      // 1.将身份证号码前面的17位数分别乘以不同的系数。
      int[] coefficientArr = { 7, 9, 10, 5, 8, 4, 2, 1, 6, 3, 7, 9, 10, 5, 8, 4, 2 };
      int sum = 0;
      for (int i = 0; i < coefficientArr.length; i++) {
        // Character.digit 在指定的基数返回字符ch的数值。如果基数是不在范围内MIN_RADIX≤基数≤MAX_RADIX或如果该值的通道是不是一个有效的数字在指定的基数-1，则返回。
        // ch - the character to be converted(要转换的字符)
        // ch - int类型，是字符的ASCII码，数字的ASCII码是48-57
        // radix - the radix(基数) ----也就是进制数
        sum += Character.digit(idCardNo.charAt(i), 10) * coefficientArr[i];
      }
      // 余数数组
      int[] remainderArr = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };

      // 身份证号码第18位数组

      int[] lastArr = { 1, 0, 'X', 9, 8, 7, 6, 5, 4, 3, 2 };
      String matchDigit = "";
      for (int i = 0; i < remainderArr.length; i++) {
        int j = remainderArr[i];
        if (j == sum % 11) {
          matchDigit = String.valueOf(lastArr[i]);
          if (lastArr[i] > 57) {
            matchDigit = String.valueOf((char) lastArr[i]);
          }
        }

        if (matchDigit.equals(idCardNo.substring(idCardNo.length() - 1))) {
          idCardflag =  true;
        }
      }
      return idCardflag;
    }else if( idCardNo.length() == 15){

      String pattern = "^(\\d{6}(18|19|20)\\d{2}(0[1-9]|1[12])(0[1-9]|[12]\\d|3[01])\\d{3}(\\d|X|x))|(\\d{8}(0[1-9]|1[12])(0[1-9]|[12]\\d|3[01])\\d{3})$";
      return Pattern.matches(pattern,idCardNo);

    }else{
      return false;
    }
  }

  @Override
  public String getDisplayString(String[] input) {
	  return "";
  }
}
