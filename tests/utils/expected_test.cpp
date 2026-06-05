/**
 * @file expected_test.cpp
 * @brief Unit tests for Expected<T, E> class
 */

#include "utils/expected.h"

#include <gtest/gtest.h>

#include <string>

#include "utils/error.h"

using namespace mygramdb::utils;

// ========== Test Expected<T, E> with value ==========

TEST(ExpectedTest, DefaultConstructor) {
  Expected<int, Error> result;
  EXPECT_TRUE(result.has_value());
  EXPECT_EQ(*result, 0);  // Default-constructed int is 0
}

TEST(ExpectedTest, ValueConstructor) {
  Expected<int, Error> result(42);
  EXPECT_TRUE(result.has_value());
  EXPECT_EQ(*result, 42);
  EXPECT_EQ(result.value(), 42);
}

TEST(ExpectedTest, ErrorConstructor) {
  auto error = MakeError(ErrorCode::kInvalidArgument, "Test error");
  Expected<int, Error> result(MakeUnexpected(error));
  EXPECT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), ErrorCode::kInvalidArgument);
  EXPECT_EQ(result.error().message(), "Test error");
}

TEST(ExpectedTest, BoolConversion) {
  Expected<int, Error> success(42);
  Expected<int, Error> failure(MakeUnexpected(MakeError(ErrorCode::kUnknown)));

  EXPECT_TRUE(static_cast<bool>(success));
  EXPECT_FALSE(static_cast<bool>(failure));
}

TEST(ExpectedTest, ValueAccess) {
  Expected<std::string, Error> result("Hello");
  EXPECT_EQ(result.value(), "Hello");
  EXPECT_EQ(*result, "Hello");
  EXPECT_EQ(result->length(), 5);
}

TEST(ExpectedTest, ValueAccessThrows) {
  Expected<int, Error> result(MakeUnexpected(MakeError(ErrorCode::kNotFound)));
  EXPECT_THROW({ (void)result.value(); }, BadExpectedAccess<Error>);
}

TEST(ExpectedTest, ErrorAccess) {
  auto error = MakeError(ErrorCode::kTimeout, "Operation timed out");
  Expected<int, Error> result(MakeUnexpected(error));

  EXPECT_EQ(result.error().code(), ErrorCode::kTimeout);
  EXPECT_EQ(result.error().message(), "Operation timed out");
}

TEST(ExpectedTest, AssignUnexpectedReplacesValueWithError) {
  Expected<int, Error> result(42);

  result = MakeUnexpected(MakeError(ErrorCode::kTimeout, "Timed out"));

  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), ErrorCode::kTimeout);
  EXPECT_EQ(result.error().message(), "Timed out");
}

TEST(ExpectedTest, MoveAssignUnexpectedReplacesValueWithError) {
  Expected<std::string, Error> result("value");

  result = MakeUnexpected(MakeError(ErrorCode::kInvalidArgument, "Invalid"));

  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), ErrorCode::kInvalidArgument);
  EXPECT_EQ(result.error().message(), "Invalid");
}

TEST(ExpectedTest, ValueOr) {
  Expected<int, Error> success(42);
  Expected<int, Error> failure(MakeUnexpected(MakeError(ErrorCode::kUnknown)));

  EXPECT_EQ(success.value_or(0), 42);
  EXPECT_EQ(failure.value_or(99), 99);
}

TEST(ExpectedTest, ValueOrMove) {
  Expected<std::string, Error> success("Hello");
  Expected<std::string, Error> failure(MakeUnexpected(MakeError(ErrorCode::kUnknown)));

  EXPECT_EQ(std::move(success).value_or("Default"), "Hello");
  EXPECT_EQ(std::move(failure).value_or("Default"), "Default");
}

// ========== Test Expected<void, E> ==========

TEST(ExpectedVoidTest, DefaultConstructor) {
  Expected<void, Error> result;
  EXPECT_TRUE(result.has_value());
}

TEST(ExpectedVoidTest, ErrorConstructor) {
  auto error = MakeError(ErrorCode::kInvalidArgument);
  Expected<void, Error> result(MakeUnexpected(error));
  EXPECT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), ErrorCode::kInvalidArgument);
}

TEST(ExpectedVoidTest, ValueAccess) {
  Expected<void, Error> success;
  EXPECT_NO_THROW(success.value());

  Expected<void, Error> failure(MakeUnexpected(MakeError(ErrorCode::kUnknown)));
  EXPECT_THROW(failure.value(), BadExpectedAccess<Error>);
}

/**
 * @brief Test that calling error() on a successful Expected<void, E> aborts
 *
 * The error() method terminates the program in all build modes (debug and
 * release) when called on an Expected that contains a value.
 */
TEST(ExpectedVoidDeathTest, ErrorAccessorOnSuccessAborts) {
  Expected<void, Error> success;
  EXPECT_TRUE(success.has_value());

  EXPECT_DEATH({ (void)success.error(); }, "error\\(\\) called on a value");
}

/**
 * @brief Test error accessor behavior on Expected<void, E> with error
 *
 * This test verifies that accessing error() when Expected contains an error
 * returns the correct error value.
 */
TEST(ExpectedVoidTest, ErrorAccessorOnFailure) {
  auto error = MakeError(ErrorCode::kTimeout, "Operation timed out");
  Expected<void, Error> failure(MakeUnexpected(error));

  EXPECT_FALSE(failure.has_value());
  EXPECT_EQ(failure.error().code(), ErrorCode::kTimeout);
  EXPECT_EQ(failure.error().message(), "Operation timed out");
}

/**
 * @brief Test Expected<void, E> copy and move with error
 *
 * Verifies that the std::optional<E> implementation correctly handles
 * copy and move operations.
 */
TEST(ExpectedVoidTest, CopyAndMoveWithError) {
  auto error = MakeError(ErrorCode::kNotFound, "Resource not found");
  Expected<void, Error> original(MakeUnexpected(error));

  // Test copy constructor
  Expected<void, Error> copied(original);
  EXPECT_FALSE(copied.has_value());
  EXPECT_EQ(copied.error().code(), ErrorCode::kNotFound);
  EXPECT_EQ(original.error().code(), ErrorCode::kNotFound);  // Original unchanged

  // Test move constructor
  Expected<void, Error> moved(std::move(original));
  EXPECT_FALSE(moved.has_value());
  EXPECT_EQ(moved.error().code(), ErrorCode::kNotFound);
}

/**
 * @brief Test Expected<void, E> assignment with success and error
 */
TEST(ExpectedVoidTest, AssignmentBetweenSuccessAndError) {
  Expected<void, Error> success;
  Expected<void, Error> failure(MakeUnexpected(MakeError(ErrorCode::kTimeout)));

  // Assign error to success
  success = failure;
  EXPECT_FALSE(success.has_value());
  EXPECT_EQ(success.error().code(), ErrorCode::kTimeout);

  // Assign success to error
  Expected<void, Error> new_success;
  failure = new_success;
  EXPECT_TRUE(failure.has_value());
}

// ========== Test monadic operations ==========

TEST(ExpectedTest, Transform) {
  Expected<int, Error> result(42);

  auto doubled = result.transform([](int x) { return x * 2; });
  EXPECT_TRUE(doubled.has_value());
  EXPECT_EQ(*doubled, 84);

  Expected<int, Error> error(MakeUnexpected(MakeError(ErrorCode::kUnknown)));
  auto transformed = error.transform([](int x) { return x * 2; });
  EXPECT_FALSE(transformed.has_value());
}

TEST(ExpectedTest, TransformToString) {
  Expected<int, Error> result(42);

  auto str = result.transform([](int x) { return std::to_string(x); });
  EXPECT_TRUE(str.has_value());
  EXPECT_EQ(*str, "42");
}

TEST(ExpectedTest, AndThen) {
  auto divide = [](int a, int b) -> Expected<int, Error> {
    if (b == 0) {
      return MakeUnexpected(MakeError(ErrorCode::kInvalidArgument, "Division by zero"));
    }
    return a / b;
  };

  Expected<int, Error> numerator(10);

  auto result = numerator.and_then([&](int a) { return divide(a, 2); });
  EXPECT_TRUE(result.has_value());
  EXPECT_EQ(*result, 5);

  auto error_result = numerator.and_then([&](int a) { return divide(a, 0); });
  EXPECT_FALSE(error_result.has_value());
  EXPECT_EQ(error_result.error().code(), ErrorCode::kInvalidArgument);
}

TEST(ExpectedTest, OrElse) {
  auto recover = [](const Error& err) -> Expected<int, Error> {
    if (err.code() == ErrorCode::kNotFound) {
      return 0;  // Return default value
    }
    return MakeUnexpected(err);  // Propagate other errors
  };

  Expected<int, Error> not_found(MakeUnexpected(MakeError(ErrorCode::kNotFound)));
  auto recovered = not_found.or_else(recover);
  EXPECT_TRUE(recovered.has_value());
  EXPECT_EQ(*recovered, 0);

  Expected<int, Error> other_error(MakeUnexpected(MakeError(ErrorCode::kTimeout)));
  auto not_recovered = other_error.or_else(recover);
  EXPECT_FALSE(not_recovered.has_value());
  EXPECT_EQ(not_recovered.error().code(), ErrorCode::kTimeout);
}

TEST(ExpectedTest, TransformError) {
  auto add_context = [](const Error& err) { return MakeError(err.code(), err.message(), "Additional context"); };

  Expected<int, Error> error(MakeUnexpected(MakeError(ErrorCode::kTimeout, "Operation timed out")));
  auto with_context = error.transform_error(add_context);

  EXPECT_FALSE(with_context.has_value());
  EXPECT_EQ(with_context.error().code(), ErrorCode::kTimeout);
  EXPECT_EQ(with_context.error().context(), "Additional context");
}

// ========== Test copy and move semantics ==========

TEST(ExpectedTest, CopyConstructor) {
  Expected<std::string, Error> original("Hello");
  Expected<std::string, Error> copy(original);

  EXPECT_TRUE(copy.has_value());
  EXPECT_EQ(*copy, "Hello");
  EXPECT_EQ(*original, "Hello");  // Original unchanged
}

TEST(ExpectedTest, MoveConstructor) {
  Expected<std::string, Error> original("Hello");
  Expected<std::string, Error> moved(std::move(original));

  EXPECT_TRUE(moved.has_value());
  EXPECT_EQ(*moved, "Hello");
}

TEST(ExpectedTest, CopyAssignment) {
  Expected<int, Error> original(42);
  Expected<int, Error> copy(0);
  copy = original;

  EXPECT_TRUE(copy.has_value());
  EXPECT_EQ(*copy, 42);
}

TEST(ExpectedTest, MoveAssignment) {
  Expected<std::string, Error> original("Hello");
  Expected<std::string, Error> moved("World");
  moved = std::move(original);

  EXPECT_TRUE(moved.has_value());
  EXPECT_EQ(*moved, "Hello");
}

// ========== Test with custom types ==========

struct CustomData {
  int id;
  std::string name;

  bool operator==(const CustomData& other) const { return id == other.id && name == other.name; }
};

TEST(ExpectedTest, CustomType) {
  CustomData data{1, "Test"};
  Expected<CustomData, Error> result(data);

  EXPECT_TRUE(result.has_value());
  EXPECT_EQ(result->id, 1);
  EXPECT_EQ(result->name, "Test");
  EXPECT_EQ(*result, data);
}

// ========== Test practical use cases ==========

// Simulated file reading function
Expected<std::string, Error> ReadFile(const std::string& path) {
  if (path.empty()) {
    return MakeUnexpected(MakeError(ErrorCode::kInvalidArgument, "Empty path"));
  }
  if (path == "/nonexistent") {
    return MakeUnexpected(MakeError(ErrorCode::kNotFound, "File not found"));
  }
  return std::string("File contents");
}

TEST(ExpectedTest, FileReadingExample) {
  auto contents = ReadFile("/etc/config");
  EXPECT_TRUE(contents.has_value());
  EXPECT_EQ(*contents, "File contents");

  auto not_found = ReadFile("/nonexistent");
  EXPECT_FALSE(not_found.has_value());
  EXPECT_EQ(not_found.error().code(), ErrorCode::kNotFound);

  auto invalid = ReadFile("");
  EXPECT_FALSE(invalid.has_value());
  EXPECT_EQ(invalid.error().code(), ErrorCode::kInvalidArgument);
}

// Simulated database query
Expected<int, Error> GetUserID(const std::string& username) {
  if (username.empty()) {
    return MakeUnexpected(MakeError(ErrorCode::kInvalidArgument, "Empty username"));
  }
  if (username == "admin") {
    return 1;
  }
  return MakeUnexpected(MakeError(ErrorCode::kNotFound, "User not found"));
}

TEST(ExpectedTest, DatabaseQueryExample) {
  auto admin_id = GetUserID("admin");
  EXPECT_TRUE(admin_id.has_value());
  EXPECT_EQ(*admin_id, 1);

  auto unknown_user = GetUserID("unknown");
  EXPECT_FALSE(unknown_user.has_value());
  EXPECT_EQ(unknown_user.error().code(), ErrorCode::kNotFound);
}

// Chaining operations
Expected<std::string, Error> FormatUserInfo(const std::string& username) {
  return GetUserID(username).transform([&](int id) { return "User " + username + " has ID " + std::to_string(id); });
}

TEST(ExpectedTest, ChainingExample) {
  auto info = FormatUserInfo("admin");
  EXPECT_TRUE(info.has_value());
  EXPECT_EQ(*info, "User admin has ID 1");

  auto error = FormatUserInfo("unknown");
  EXPECT_FALSE(error.has_value());
  EXPECT_EQ(error.error().code(), ErrorCode::kNotFound);
}

// ========== Test error handling patterns ==========

TEST(ExpectedTest, MultipleErrorHandling) {
  auto process = [](int value) -> Expected<int, Error> {
    if (value < 0) {
      return MakeUnexpected(MakeError(ErrorCode::kInvalidArgument, "Negative value"));
    }
    if (value > 100) {
      return MakeUnexpected(MakeError(ErrorCode::kOutOfRange, "Value too large"));
    }
    return value * 2;
  };

  auto success = process(50);
  EXPECT_TRUE(success.has_value());
  EXPECT_EQ(*success, 100);

  auto negative = process(-1);
  EXPECT_FALSE(negative.has_value());
  EXPECT_EQ(negative.error().code(), ErrorCode::kInvalidArgument);

  auto too_large = process(200);
  EXPECT_FALSE(too_large.has_value());
  EXPECT_EQ(too_large.error().code(), ErrorCode::kOutOfRange);
}

// ========== Test BadExpectedAccess exception ==========

TEST(ExpectedTest, BadExpectedAccessException) {
  Expected<int, Error> error(MakeUnexpected(MakeError(ErrorCode::kTimeout, "Timed out")));

  try {
    int value = error.value();
    FAIL() << "Expected BadExpectedAccess exception, got value: " << value;
  } catch (const BadExpectedAccess<Error>& e) {
    EXPECT_EQ(e.error().code(), ErrorCode::kTimeout);
    EXPECT_STREQ(e.what(), "Bad Expected access: contains error");
  }
}

// ========== Death tests for misuse of value()/error() ==========

/**
 * @brief Test that calling error() on a success Expected<T, E> aborts
 */
TEST(ExpectedDeathTest, ErrorOnSuccessAborts) {
  Expected<int, Error> success(42);
  EXPECT_DEATH({ (void)success.error(); }, "error\\(\\) called on a value");
}

/**
 * @brief Test that calling error() on a success Expected<T, E> via rvalue aborts
 */
TEST(ExpectedDeathTest, ErrorOnSuccessRvalueAborts) {
  Expected<int, Error> success(42);
  EXPECT_DEATH({ (void)std::move(success).error(); }, "error\\(\\) called on a value");
}

/**
 * @brief Test that calling error() on a success Expected<void, E> via rvalue aborts
 */
TEST(ExpectedVoidDeathTest, ErrorOnSuccessRvalueAborts) {
  Expected<void, Error> success;
  EXPECT_DEATH({ (void)std::move(success).error(); }, "error\\(\\) called on a value");
}

/**
 * @brief Test that value() works correctly on success Expected<T, E>
 */
TEST(ExpectedTest, ValueOnSuccessWorks) {
  Expected<int, Error> success(42);
  EXPECT_EQ(success.value(), 42);

  const Expected<int, Error> const_success(99);
  EXPECT_EQ(const_success.value(), 99);

  Expected<int, Error> movable(7);
  EXPECT_EQ(std::move(movable).value(), 7);
}

/**
 * @brief Test that error() works correctly on error Expected<T, E>
 */
TEST(ExpectedTest, ErrorOnFailureWorks) {
  Expected<int, Error> failure(MakeUnexpected(MakeError(ErrorCode::kTimeout, "Timed out")));
  EXPECT_EQ(failure.error().code(), ErrorCode::kTimeout);
  EXPECT_EQ(failure.error().message(), "Timed out");

  const Expected<int, Error> const_failure(MakeUnexpected(MakeError(ErrorCode::kNotFound, "Not found")));
  EXPECT_EQ(const_failure.error().code(), ErrorCode::kNotFound);
}

/**
 * @brief Test that error() works correctly on error Expected<void, E>
 */
TEST(ExpectedVoidTest, ErrorOnFailureWorks) {
  Expected<void, Error> failure(MakeUnexpected(MakeError(ErrorCode::kTimeout, "Timed out")));
  EXPECT_EQ(failure.error().code(), ErrorCode::kTimeout);
  EXPECT_EQ(failure.error().message(), "Timed out");

  const Expected<void, Error> const_failure(MakeUnexpected(MakeError(ErrorCode::kNotFound, "Not found")));
  EXPECT_EQ(const_failure.error().code(), ErrorCode::kNotFound);
}
