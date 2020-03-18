public enum FieldEnum {
  EMP_ID(0, 8, "empId"),
  DATE(8, 18, "date"),
  GENDER(43, 44, "gender");
  String name;
  int startIndex, endIndex;

  FieldEnum(int startIndex, int endIndex, String name) {
    this.startIndex = startIndex;
    this.endIndex = endIndex;
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public int getFieldLength() {
    return endIndex - startIndex;
  }

  public String getValue(String line) {
    return line.substring(startIndex, endIndex);
  }
}
