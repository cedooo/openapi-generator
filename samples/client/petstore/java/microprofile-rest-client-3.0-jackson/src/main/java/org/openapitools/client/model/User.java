/**
 * OpenAPI Petstore
 * This is a sample server Petstore server. For this sample, you can use the api key `special-key` to test the authorization filters.
 *
 * The version of the OpenAPI document: 1.0.0
 * 
 *
 * NOTE: This class is auto generated by OpenAPI Generator (https://openapi-generator.tech).
 * https://openapi-generator.tech
 * Do not edit the class manually.
 */

package org.openapitools.client.model;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import java.util.Objects;
import java.util.Arrays;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonPropertyOrder({
  User.JSON_PROPERTY_ID,
  User.JSON_PROPERTY_USERNAME,
  User.JSON_PROPERTY_FIRST_NAME,
  User.JSON_PROPERTY_LAST_NAME,
  User.JSON_PROPERTY_EMAIL,
  User.JSON_PROPERTY_PASSWORD,
  User.JSON_PROPERTY_PHONE,
  User.JSON_PROPERTY_USER_STATUS
})
/**
  * A User who is purchasing from the pet store
  */

public class User  {
  
  public static final String JSON_PROPERTY_ID = "id";
  
  private Long id;

  public static final String JSON_PROPERTY_USERNAME = "username";
  
  private String username;

  public static final String JSON_PROPERTY_FIRST_NAME = "firstName";
  
  private String firstName;

  public static final String JSON_PROPERTY_LAST_NAME = "lastName";
  
  private String lastName;

  public static final String JSON_PROPERTY_EMAIL = "email";
  
  private String email;

  public static final String JSON_PROPERTY_PASSWORD = "password";
  
  private String password;

  public static final String JSON_PROPERTY_PHONE = "phone";
  
  private String phone;

  public static final String JSON_PROPERTY_USER_STATUS = "userStatus";
 /**
  * User Status
  */
  
  private Integer userStatus;

  /**
   * Get id
   * @return id
   **/
  @JsonProperty(JSON_PROPERTY_ID)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public Long getId() {
    return id;
  }

  /**
   * Set id
   */
  @JsonProperty(JSON_PROPERTY_ID)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setId(Long id) {
    this.id = id;
  }

  public User id(Long id) {
    this.id = id;
    return this;
  }

  /**
   * Get username
   * @return username
   **/
  @JsonProperty(JSON_PROPERTY_USERNAME)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public String getUsername() {
    return username;
  }

  /**
   * Set username
   */
  @JsonProperty(JSON_PROPERTY_USERNAME)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setUsername(String username) {
    this.username = username;
  }

  public User username(String username) {
    this.username = username;
    return this;
  }

  /**
   * Get firstName
   * @return firstName
   **/
  @JsonProperty(JSON_PROPERTY_FIRST_NAME)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public String getFirstName() {
    return firstName;
  }

  /**
   * Set firstName
   */
  @JsonProperty(JSON_PROPERTY_FIRST_NAME)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setFirstName(String firstName) {
    this.firstName = firstName;
  }

  public User firstName(String firstName) {
    this.firstName = firstName;
    return this;
  }

  /**
   * Get lastName
   * @return lastName
   **/
  @JsonProperty(JSON_PROPERTY_LAST_NAME)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public String getLastName() {
    return lastName;
  }

  /**
   * Set lastName
   */
  @JsonProperty(JSON_PROPERTY_LAST_NAME)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setLastName(String lastName) {
    this.lastName = lastName;
  }

  public User lastName(String lastName) {
    this.lastName = lastName;
    return this;
  }

  /**
   * Get email
   * @return email
   **/
  @JsonProperty(JSON_PROPERTY_EMAIL)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public String getEmail() {
    return email;
  }

  /**
   * Set email
   */
  @JsonProperty(JSON_PROPERTY_EMAIL)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setEmail(String email) {
    this.email = email;
  }

  public User email(String email) {
    this.email = email;
    return this;
  }

  /**
   * Get password
   * @return password
   **/
  @JsonProperty(JSON_PROPERTY_PASSWORD)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public String getPassword() {
    return password;
  }

  /**
   * Set password
   */
  @JsonProperty(JSON_PROPERTY_PASSWORD)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setPassword(String password) {
    this.password = password;
  }

  public User password(String password) {
    this.password = password;
    return this;
  }

  /**
   * Get phone
   * @return phone
   **/
  @JsonProperty(JSON_PROPERTY_PHONE)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public String getPhone() {
    return phone;
  }

  /**
   * Set phone
   */
  @JsonProperty(JSON_PROPERTY_PHONE)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setPhone(String phone) {
    this.phone = phone;
  }

  public User phone(String phone) {
    this.phone = phone;
    return this;
  }

  /**
   * User Status
   * @return userStatus
   **/
  @JsonProperty(JSON_PROPERTY_USER_STATUS)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public Integer getUserStatus() {
    return userStatus;
  }

  /**
   * Set userStatus
   */
  @JsonProperty(JSON_PROPERTY_USER_STATUS)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setUserStatus(Integer userStatus) {
    this.userStatus = userStatus;
  }

  public User userStatus(Integer userStatus) {
    this.userStatus = userStatus;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    return EqualsBuilder.reflectionEquals(this, o, false, null, true);
  }

  @Override
  public int hashCode() {
    return HashCodeBuilder.reflectionHashCode(this);
  }

  /**
   * Create a string representation of this pojo.
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class User {\n");
    
    sb.append("    id: ").append(toIndentedString(id)).append("\n");
    sb.append("    username: ").append(toIndentedString(username)).append("\n");
    sb.append("    firstName: ").append(toIndentedString(firstName)).append("\n");
    sb.append("    lastName: ").append(toIndentedString(lastName)).append("\n");
    sb.append("    email: ").append(toIndentedString(email)).append("\n");
    sb.append("    password: ").append(toIndentedString(password)).append("\n");
    sb.append("    phone: ").append(toIndentedString(phone)).append("\n");
    sb.append("    userStatus: ").append(toIndentedString(userStatus)).append("\n");
    sb.append("}");
    return sb.toString();
  }

  /**
   * Convert the given object to string with each line indented by 4 spaces
   * (except the first line).
   */
  private static String toIndentedString(Object o) {
    if (o == null) {
      return "null";
    }
    return o.toString().replace("\n", "\n    ");
  }
}

