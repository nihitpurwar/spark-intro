package lms.learning;

public class AudienceEventBean {
  private String id;
  private String transactionId;
  private String productId;
  private int userId;
  private double spent;

  public AudienceEventBean(int userId, int spent) {
    this.userId = userId;
    this.spent = spent;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getTransactionId() {
    return transactionId;
  }

  public void setTransactionId(String transactionId) {
    this.transactionId = transactionId;
  }

  public String getProductId() {
    return productId;
  }

  public void setProductId(String productId) {
    this.productId = productId;
  }

  public int getUserId() {
    return userId;
  }

  public void setUserId(int userId) {
    this.userId = userId;
  }

  public double getSpent() {
    return spent;
  }

  public void setSpent(double spent) {
    this.spent = spent;
  }
}
