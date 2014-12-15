package com.ganqiang.datatunnel.util;

import java.math.BigDecimal;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class ArithUtil
{

  // 默认除法运算精度
  private static final int DEF_DIV_SCALE = 10;

  // 这个类不能实例化
  private ArithUtil()
  {
  }
  
  public static boolean isNumeric(String str){ 
    Pattern p = Pattern.compile("^(-?\\d+)(\\.\\d+)?$");  
    Matcher m = p.matcher(str);  
    boolean isNumber = m.matches();  
    return isNumber;    
  }

  public static String parseExp(String expression)
  {
    // String numberReg="^((?!0)\\d+(\\.\\d+(?<!0))?)|(0\\.\\d+(?<!0))$";
    expression = expression.replaceAll("\\s+", "").replaceAll("^\\((.+)\\)$",
        "$1");
//    String checkExp = "\\d";
    String minExp = "^((\\d+(\\.\\d+)?)|(\\[\\-\\d+(\\.\\d+)?\\]))[\\+\\-\\*\\/]((\\d+(\\.\\d+)?)|(\\[\\-\\d+(\\.\\d+)?\\]))$";
    // 最小表达式计算
    if (expression.matches(minExp)) {
      String result = calculate(expression);

      return Double.parseDouble(result) >= 0 ? result : "[" + result + "]";
    }
    // 计算不带括号的四则运算
    String noParentheses = "^[^\\(\\)]+$";
    String priorOperatorExp = "(((\\d+(\\.\\d+)?)|(\\[\\-\\d+(\\.\\d+)?\\]))[\\*\\/]((\\d+(\\.\\d+)?)|(\\[\\-\\d+(\\.\\d+)?\\])))";
    String operatorExp = "(((\\d+(\\.\\d+)?)|(\\[\\-\\d+(\\.\\d+)?\\]))[\\+\\-]((\\d+(\\.\\d+)?)|(\\[\\-\\d+(\\.\\d+)?\\])))";
    if (expression.matches(noParentheses)) {
      Pattern patt = Pattern.compile(priorOperatorExp);
      Matcher mat = patt.matcher(expression);
      if (mat.find()) {
        String tempMinExp = mat.group();
        expression = expression.replaceFirst(priorOperatorExp,
            parseExp(tempMinExp));
      } else {
        patt = Pattern.compile(operatorExp);
        mat = patt.matcher(expression);

        if (mat.find()) {
          String tempMinExp = mat.group();
          expression = expression.replaceFirst(operatorExp,
              parseExp(tempMinExp));
        }
      }
      return parseExp(expression);
    }
    // 计算带括号的四则运算
    String minParentheses = "\\([^\\(\\)]+\\)";
    Pattern patt = Pattern.compile(minParentheses);
    Matcher mat = patt.matcher(expression);
    if (mat.find()) {
      String tempMinExp = mat.group();
      expression = expression
          .replaceFirst(minParentheses, parseExp(tempMinExp));
    }
    return parseExp(expression);
  }

  public static String calculate(String exp)
  {
    exp = exp.replaceAll("[\\[\\]]", "");
    String number[] = exp.replaceFirst("(\\d)[\\+\\-\\*\\/]", "$1,").split(",");
    BigDecimal number1 = new BigDecimal(number[0]);
    BigDecimal number2 = new BigDecimal(number[1]);
    BigDecimal result = null;

    String operator = exp.replaceFirst("^.*\\d([\\+\\-\\*\\/]).+$", "$1");
    if ("+".equals(operator)) {
      result = number1.add(number2);
    } else if ("-".equals(operator)) {
      result = number1.subtract(number2);
    } else if ("*".equals(operator)) {
      result = number1.multiply(number2);
    } else if ("/".equals(operator)) {
      result = number1.divide(number2);
    }

    return result != null ? result.toString() : null;
  }

  /**
   * 提供精确的加法运算。
   * 
   * @param v1
   *          被加数
   * @param v2
   *          加数
   * @return 两个参数的和
   */

  public static double add(double v1, double v2)
  {
    BigDecimal b1 = new BigDecimal(Double.toString(v1));
    BigDecimal b2 = new BigDecimal(Double.toString(v2));
    return b1.add(b2).doubleValue();
  }

  /**
   * 提供精确的减法运算。
   * 
   * @param v1
   *          被减数
   * @param v2
   *          减数
   * @return 两个参数的差
   */

  public static double sub(double v1, double v2)
  {
    BigDecimal b1 = new BigDecimal(Double.toString(v1));
    BigDecimal b2 = new BigDecimal(Double.toString(v2));
    return b1.subtract(b2).doubleValue();
  }

  /**
   * 提供精确的乘法运算。
   * 
   * @param v1
   *          被乘数
   * @param v2
   *          乘数
   * @return 两个参数的积
   */

  public static double mul(double v1, double v2)
  {
    BigDecimal b1 = new BigDecimal(Double.toString(v1));
    BigDecimal b2 = new BigDecimal(Double.toString(v2));
    return b1.multiply(b2).doubleValue();
  }

  /**
   * 提供（相对）精确的除法运算，当发生除不尽的情况时，精确到 小数点以后10位，以后的数字四舍五入。
   * 
   * @param v1
   *          被除数
   * @param v2
   *          除数
   * @return 两个参数的商
   */

  public static double div(double v1, double v2)
  {
    return div(v1, v2, DEF_DIV_SCALE);
  }

  public static String divRate(double v1, double v2)
  {
    double d = div(v1, v2, DEF_DIV_SCALE);
    double r = mul(d, 100);
    return r + "%";
  }

  public static String divRate(double v1, double v2, int scale)
  {
    double d = div(v1, v2, scale);
    double r = mul(d, 100);
    return r + "%";
  }

  /**
   * 提供（相对）精确的除法运算。当发生除不尽的情况时，由scale参数指 定精度，以后的数字四舍五入。
   * 
   * @param v1
   *          被除数
   * @param v2
   *          除数
   * @param scale
   *          表示表示需要精确到小数点以后几位。
   * @return 两个参数的商
   */

  public static double div(double v1, double v2, int scale)
  {
    if (scale < 0) {
      throw new IllegalArgumentException(
          "The   scale   must   be   a   positive   integer   or   zero");
    }
    BigDecimal b1 = new BigDecimal(Double.toString(v1));
    BigDecimal b2 = new BigDecimal(Double.toString(v2));
    return b1.divide(b2, scale, BigDecimal.ROUND_HALF_UP).doubleValue();
  }

  /**
   * 提供精确的小数位四舍五入处理。
   * 
   * @param v
   *          需要四舍五入的数字
   * @param scale
   *          小数点后保留几位
   * @return 四舍五入后的结果
   */

  public static double round(double v, int scale)
  {
    if (scale < 0) {
      throw new IllegalArgumentException(
          "The   scale   must   be   a   positive   integer   or   zero");
    }
    BigDecimal b = new BigDecimal(Double.toString(v));
    BigDecimal one = new BigDecimal("1");
    return b.divide(one, scale, BigDecimal.ROUND_HALF_UP).doubleValue();
  }
}
