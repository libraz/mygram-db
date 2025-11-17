/**
 * @file expected.h
 * @brief C++17-compatible implementation of std::expected (C++23)
 *
 * This provides a type-safe error handling mechanism that represents either
 * a successful value (T) or an error (E). It is compatible with C++23's
 * std::expected and can be migrated to std::expected once C++23 is adopted.
 *
 * Usage:
 * @code
 * Expected<int, Error> Divide(int a, int b) {
 *   if (b == 0) {
 *     return Unexpected(Error(ErrorCode::kDivisionByZero, "Division by zero"));
 *   }
 *   return a / b;
 * }
 *
 * auto result = Divide(10, 2);
 * if (result) {
 *   std::cout << "Result: " << *result << std::endl;
 * } else {
 *   std::cerr << "Error: " << result.error().message << std::endl;
 * }
 * @endcode
 */

#pragma once

#include <stdexcept>
#include <type_traits>
#include <utility>
#include <variant>

namespace mygram::utils {

/**
 * @brief Tag type for constructing Expected with an error value
 */
template <typename E>
struct Unexpected {
  E error;

  explicit Unexpected(E err) : error(std::move(err)) {}
};

/**
 * @brief Helper function to create Unexpected values
 */
template <typename E>
Unexpected<std::decay_t<E>> MakeUnexpected(E&& error) {
  return Unexpected<std::decay_t<E>>(std::forward<E>(error));
}

/**
 * @brief Exception thrown when accessing value of Expected containing error
 */
template <typename E>
class BadExpectedAccess : public std::exception {
 public:
  explicit BadExpectedAccess(E error) : error_(std::move(error)) {}

  const char* what() const noexcept override { return "Bad Expected access: contains error"; }

  const E& error() const& { return error_; }
  E& error() & { return error_; }
  const E&& error() const&& { return std::move(error_); }
  E&& error() && { return std::move(error_); }

 private:
  E error_;
};

/**
 * @brief Expected<T, E> represents either a value of type T or an error of type E
 *
 * This is a C++17-compatible implementation of C++23's std::expected.
 * It provides type-safe error handling without exceptions.
 *
 * @tparam T The type of the expected value
 * @tparam E The type of the error
 */
template <typename T, typename E>
class Expected {
 public:
  using value_type = T;
  using error_type = E;
  using unexpected_type = Unexpected<E>;

  // ========== Constructors ==========

  /**
   * @brief Default constructor - constructs with default value
   */
  Expected() : storage_(std::in_place_index<0>, T{}) {}

  /**
   * @brief Construct with a value
   */
  Expected(const T& value) : storage_(std::in_place_index<0>, value) {}

  /**
   * @brief Construct with a value (move)
   */
  Expected(T&& value) : storage_(std::in_place_index<0>, std::move(value)) {}

  /**
   * @brief Construct with an error
   */
  Expected(const Unexpected<E>& unexpected) : storage_(std::in_place_index<1>, unexpected.error) {}

  /**
   * @brief Construct with an error (move)
   */
  Expected(Unexpected<E>&& unexpected) noexcept(std::is_nothrow_move_constructible_v<E>)
      : storage_(std::in_place_index<1>, std::move(unexpected).error) {}

  /**
   * @brief Copy constructor
   */
  Expected(const Expected& other) = default;

  /**
   * @brief Move constructor
   */
  Expected(Expected&& other) noexcept = default;

  /**
   * @brief Copy assignment
   */
  Expected& operator=(const Expected& other) = default;

  /**
   * @brief Move assignment
   */
  Expected& operator=(Expected&& other) noexcept = default;

  /**
   * @brief Destructor
   */
  ~Expected() = default;

  /**
   * @brief Assign a value
   */
  Expected& operator=(const T& value) {
    storage_ = value;
    return *this;
  }

  /**
   * @brief Assign a value (move)
   */
  Expected& operator=(T&& value) {
    storage_ = std::move(value);
    return *this;
  }

  /**
   * @brief Assign an error
   */
  Expected& operator=(const Unexpected<E>& unexpected) {
    storage_ = unexpected.error;
    return *this;
  }

  /**
   * @brief Assign an error (move)
   */
  Expected& operator=(Unexpected<E>&& unexpected) noexcept(std::is_nothrow_move_assignable_v<E>) {
    storage_ = std::move(unexpected).error;
    return *this;
  }

  // ========== Observers ==========

  /**
   * @brief Check if Expected contains a value (not an error)
   */
  [[nodiscard]] bool has_value() const noexcept { return storage_.index() == 0; }

  /**
   * @brief Check if Expected contains a value (implicit conversion to bool)
   */
  [[nodiscard]] explicit operator bool() const noexcept { return has_value(); }

  // ========== Value accessors ==========

  /**
   * @brief Access the contained value (const lvalue reference)
   * @throws BadExpectedAccess if Expected contains an error
   */
  [[nodiscard]] const T& value() const& {
    if (!has_value()) {
      throw BadExpectedAccess<E>(std::get<1>(storage_));
    }
    return std::get<0>(storage_);
  }

  /**
   * @brief Access the contained value (lvalue reference)
   * @throws BadExpectedAccess if Expected contains an error
   */
  [[nodiscard]] T& value() & {
    if (!has_value()) {
      throw BadExpectedAccess<E>(std::get<1>(storage_));
    }
    return std::get<0>(storage_);
  }

  /**
   * @brief Access the contained value (const rvalue reference)
   * @throws BadExpectedAccess if Expected contains an error
   */
  [[nodiscard]] const T&& value() const&& {
    if (!has_value()) {
      throw BadExpectedAccess<E>(std::get<1>(storage_));
    }
    return std::move(std::get<0>(storage_));
  }

  /**
   * @brief Access the contained value (rvalue reference)
   * @throws BadExpectedAccess if Expected contains an error
   */
  [[nodiscard]] T&& value() && {
    if (!has_value()) {
      throw BadExpectedAccess<E>(std::get<1>(storage_));
    }
    return std::move(std::get<0>(storage_));
  }

  /**
   * @brief Access the contained value (unchecked, const lvalue)
   * @warning Undefined behavior if Expected contains an error
   */
  [[nodiscard]] const T& operator*() const& { return std::get<0>(storage_); }

  /**
   * @brief Access the contained value (unchecked, lvalue)
   * @warning Undefined behavior if Expected contains an error
   */
  [[nodiscard]] T& operator*() & { return std::get<0>(storage_); }

  /**
   * @brief Access the contained value (unchecked, const rvalue)
   * @warning Undefined behavior if Expected contains an error
   */
  [[nodiscard]] const T&& operator*() const&& { return std::move(std::get<0>(storage_)); }

  /**
   * @brief Access the contained value (unchecked, rvalue)
   * @warning Undefined behavior if Expected contains an error
   */
  [[nodiscard]] T&& operator*() && { return std::move(std::get<0>(storage_)); }

  /**
   * @brief Access the contained value via pointer (unchecked, const)
   * @warning Undefined behavior if Expected contains an error
   */
  [[nodiscard]] const T* operator->() const { return &std::get<0>(storage_); }

  /**
   * @brief Access the contained value via pointer (unchecked)
   * @warning Undefined behavior if Expected contains an error
   */
  [[nodiscard]] T* operator->() { return &std::get<0>(storage_); }

  /**
   * @brief Get the value or a default if error (const lvalue)
   */
  template <typename U>
  [[nodiscard]] T value_or(U&& default_value) const& {
    return has_value() ? std::get<0>(storage_) : static_cast<T>(std::forward<U>(default_value));
  }

  /**
   * @brief Get the value or a default if error (rvalue)
   */
  template <typename U>
  [[nodiscard]] T value_or(U&& default_value) && {
    return has_value() ? std::move(std::get<0>(storage_)) : static_cast<T>(std::forward<U>(default_value));
  }

  // ========== Error accessors ==========

  /**
   * @brief Access the contained error (const lvalue reference)
   * @warning Undefined behavior if Expected contains a value
   */
  [[nodiscard]] const E& error() const& { return std::get<1>(storage_); }

  /**
   * @brief Access the contained error (lvalue reference)
   * @warning Undefined behavior if Expected contains a value
   */
  [[nodiscard]] E& error() & { return std::get<1>(storage_); }

  /**
   * @brief Access the contained error (const rvalue reference)
   * @warning Undefined behavior if Expected contains a value
   */
  [[nodiscard]] const E&& error() const&& { return std::move(std::get<1>(storage_)); }

  /**
   * @brief Access the contained error (rvalue reference)
   * @warning Undefined behavior if Expected contains a value
   */
  [[nodiscard]] E&& error() && { return std::move(std::get<1>(storage_)); }

  // ========== Monadic operations (C++23 compatibility) ==========

  /**
   * @brief Transform the value if present, otherwise propagate the error
   *
   * @tparam F Callable type that takes T and returns Expected<U, E>
   * @param func Function to apply to the value
   * @return Expected<U, E> with transformed value or original error
   */
  template <typename F>
  auto and_then(F&& func) & -> decltype(func(std::declval<T&>())) {
    using U = decltype(func(std::declval<T&>()));
    static_assert(std::is_same_v<typename U::error_type, E>, "Error type must be the same");
    if (has_value()) {
      return std::forward<F>(func)(std::get<0>(storage_));
    }
    return U(MakeUnexpected(std::get<1>(storage_)));
  }

  /**
   * @brief Transform the value if present (const version)
   */
  template <typename F>
  auto and_then(F&& func) const& -> decltype(func(std::declval<const T&>())) {
    using U = decltype(func(std::declval<const T&>()));
    static_assert(std::is_same_v<typename U::error_type, E>, "Error type must be the same");
    if (has_value()) {
      return std::forward<F>(func)(std::get<0>(storage_));
    }
    return U(MakeUnexpected(std::get<1>(storage_)));
  }

  /**
   * @brief Transform the value if present (rvalue version)
   */
  template <typename F>
  auto and_then(F&& func) && -> decltype(func(std::declval<T&&>())) {
    using U = decltype(func(std::declval<T&&>()));
    static_assert(std::is_same_v<typename U::error_type, E>, "Error type must be the same");
    if (has_value()) {
      return std::forward<F>(func)(std::move(std::get<0>(storage_)));
    }
    return U(MakeUnexpected(std::move(std::get<1>(storage_))));
  }

  /**
   * @brief Transform the value if present (map operation)
   *
   * @tparam F Callable type that takes T and returns U
   * @param func Function to apply to the value
   * @return Expected<U, E> with transformed value or original error
   */
  template <typename F>
  auto transform(F&& func) & {
    using U = std::decay_t<decltype(func(std::declval<T&>()))>;
    if (has_value()) {
      return Expected<U, E>(std::forward<F>(func)(std::get<0>(storage_)));
    }
    return Expected<U, E>(MakeUnexpected(std::get<1>(storage_)));
  }

  /**
   * @brief Transform the value if present (const version)
   */
  template <typename F>
  auto transform(F&& func) const& {
    using U = std::decay_t<decltype(func(std::declval<const T&>()))>;
    if (has_value()) {
      return Expected<U, E>(std::forward<F>(func)(std::get<0>(storage_)));
    }
    return Expected<U, E>(MakeUnexpected(std::get<1>(storage_)));
  }

  /**
   * @brief Transform the value if present (rvalue version)
   */
  template <typename F>
  auto transform(F&& func) && {
    using U = std::decay_t<decltype(func(std::declval<T&&>()))>;
    if (has_value()) {
      return Expected<U, E>(std::forward<F>(func)(std::move(std::get<0>(storage_))));
    }
    return Expected<U, E>(MakeUnexpected(std::move(std::get<1>(storage_))));
  }

  /**
   * @brief Transform the error if present
   *
   * @tparam F Callable type that takes E and returns G
   * @param func Function to apply to the error
   * @return Expected<T, G> with original value or transformed error
   */
  template <typename F>
  auto transform_error(F&& func) & {
    using G = std::decay_t<decltype(func(std::declval<E&>()))>;
    if (has_value()) {
      return Expected<T, G>(std::get<0>(storage_));
    }
    return Expected<T, G>(MakeUnexpected(std::forward<F>(func)(std::get<1>(storage_))));
  }

  /**
   * @brief Transform the error if present (const version)
   */
  template <typename F>
  auto transform_error(F&& func) const& {
    using G = std::decay_t<decltype(func(std::declval<const E&>()))>;
    if (has_value()) {
      return Expected<T, G>(std::get<0>(storage_));
    }
    return Expected<T, G>(MakeUnexpected(std::forward<F>(func)(std::get<1>(storage_))));
  }

  /**
   * @brief Transform the error if present (rvalue version)
   */
  template <typename F>
  auto transform_error(F&& func) && {
    using G = std::decay_t<decltype(func(std::declval<E&&>()))>;
    if (has_value()) {
      return Expected<T, G>(std::move(std::get<0>(storage_)));
    }
    return Expected<T, G>(MakeUnexpected(std::forward<F>(func)(std::move(std::get<1>(storage_)))));
  }

  /**
   * @brief Handle the error case (or_else operation)
   *
   * @tparam F Callable type that takes E and returns Expected<T, G>
   * @param func Function to apply to the error
   * @return Expected<T, G> with original value or result of func
   */
  template <typename F>
  auto or_else(F&& func) & -> decltype(func(std::declval<E&>())) {
    using G = decltype(func(std::declval<E&>()));
    static_assert(std::is_same_v<typename G::value_type, T>, "Value type must be the same");
    if (has_value()) {
      return G(std::get<0>(storage_));
    }
    return std::forward<F>(func)(std::get<1>(storage_));
  }

  /**
   * @brief Handle the error case (const version)
   */
  template <typename F>
  auto or_else(F&& func) const& -> decltype(func(std::declval<const E&>())) {
    using G = decltype(func(std::declval<const E&>()));
    static_assert(std::is_same_v<typename G::value_type, T>, "Value type must be the same");
    if (has_value()) {
      return G(std::get<0>(storage_));
    }
    return std::forward<F>(func)(std::get<1>(storage_));
  }

  /**
   * @brief Handle the error case (rvalue version)
   */
  template <typename F>
  auto or_else(F&& func) && -> decltype(func(std::declval<E&&>())) {
    using G = decltype(func(std::declval<E&&>()));
    static_assert(std::is_same_v<typename G::value_type, T>, "Value type must be the same");
    if (has_value()) {
      return G(std::move(std::get<0>(storage_)));
    }
    return std::forward<F>(func)(std::move(std::get<1>(storage_)));
  }

 private:
  std::variant<T, E> storage_;
};

/**
 * @brief Specialization of Expected<void, E> for operations that don't return a value
 *
 * This allows representing success/failure without a value payload.
 */
template <typename E>
class Expected<void, E> {
 public:
  using value_type = void;
  using error_type = E;
  using unexpected_type = Unexpected<E>;

  // ========== Constructors ==========

  /**
   * @brief Default constructor - constructs with success (no value)
   */
  Expected() : has_value_(true) {}

  /**
   * @brief Construct with an error
   */
  Expected(const Unexpected<E>& unexpected) : has_value_(false), error_(unexpected.error) {}

  /**
   * @brief Construct with an error (move)
   */
  Expected(Unexpected<E>&& unexpected) noexcept(std::is_nothrow_move_constructible_v<E>)
      : has_value_(false), error_(std::move(unexpected).error) {}

  /**
   * @brief Copy constructor
   */
  Expected(const Expected& other) = default;

  /**
   * @brief Move constructor
   */
  Expected(Expected&& other) noexcept = default;

  /**
   * @brief Copy assignment
   */
  Expected& operator=(const Expected& other) = default;

  /**
   * @brief Move assignment
   */
  Expected& operator=(Expected&& other) noexcept = default;

  /**
   * @brief Destructor
   */
  ~Expected() = default;

  // ========== Observers ==========

  /**
   * @brief Check if Expected represents success (not an error)
   */
  [[nodiscard]] bool has_value() const noexcept { return has_value_; }

  /**
   * @brief Check if Expected represents success (implicit conversion to bool)
   */
  [[nodiscard]] explicit operator bool() const noexcept { return has_value_; }

  /**
   * @brief Verify the Expected contains a value (throws if error)
   * @throws BadExpectedAccess if Expected contains an error
   */
  void value() const {
    if (!has_value_) {
      throw BadExpectedAccess<E>(error_);
    }
  }

  // ========== Error accessors ==========

  /**
   * @brief Access the contained error (const lvalue reference)
   * @warning Undefined behavior if Expected contains a value
   */
  [[nodiscard]] const E& error() const& { return error_; }

  /**
   * @brief Access the contained error (lvalue reference)
   * @warning Undefined behavior if Expected contains a value
   */
  [[nodiscard]] E& error() & { return error_; }

  /**
   * @brief Access the contained error (const rvalue reference)
   * @warning Undefined behavior if Expected contains a value
   */
  [[nodiscard]] const E&& error() const&& { return std::move(error_); }

  /**
   * @brief Access the contained error (rvalue reference)
   * @warning Undefined behavior if Expected contains a value
   */
  [[nodiscard]] E&& error() && { return std::move(error_); }

 private:
  bool has_value_;
  E error_;
};

}  // namespace mygram::utils
