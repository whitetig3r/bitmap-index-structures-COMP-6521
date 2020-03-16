public enum FieldEnum {
  EMP_ID(0, 18),
  DATE(8, 18),
  GENDER(43, 44);
  int startIndex, endIndex;

  FieldEnum(int startIndex, int endIndex) {
    this.startIndex = startIndex;
    this.endIndex = endIndex;
  }

  public String getValue(String line) {
    return line.substring(startIndex, endIndex);
  }
}
