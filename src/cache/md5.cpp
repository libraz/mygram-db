/**
 * @file md5.cpp
 * @brief MD5 implementation
 *
 * Based on RFC 1321 - The MD5 Message-Digest Algorithm
 *
 * NOLINTBEGIN - Low-level cryptographic implementation following RFC 1321
 * Using standard MD5 algorithm conventions (short parameter names, magic numbers)
 */

#include "cache/md5.h"

namespace mygramdb::cache {

// Constants for MD5 Transform routine
static constexpr uint32_t S11 = 7;
static constexpr uint32_t S12 = 12;
static constexpr uint32_t S13 = 17;
static constexpr uint32_t S14 = 22;
static constexpr uint32_t S21 = 5;
static constexpr uint32_t S22 = 9;
static constexpr uint32_t S23 = 14;
static constexpr uint32_t S24 = 20;
static constexpr uint32_t S31 = 4;
static constexpr uint32_t S32 = 11;
static constexpr uint32_t S33 = 16;
static constexpr uint32_t S34 = 23;
static constexpr uint32_t S41 = 6;
static constexpr uint32_t S42 = 10;
static constexpr uint32_t S43 = 15;
static constexpr uint32_t S44 = 21;

// F, G, H and I are basic MD5 functions
static inline uint32_t F(uint32_t x, uint32_t y, uint32_t z) {
  return (x & y) | (~x & z);
}
static inline uint32_t G(uint32_t x, uint32_t y, uint32_t z) {
  return (x & z) | (y & ~z);
}
static inline uint32_t H(uint32_t x, uint32_t y, uint32_t z) {
  return x ^ y ^ z;
}
static inline uint32_t I(uint32_t x, uint32_t y, uint32_t z) {
  return y ^ (x | ~z);
}

// ROTATE_LEFT rotates x left n bits
static inline uint32_t RotateLeft(uint32_t x, uint32_t n) {
  return (x << n) | (x >> (32 - n));
}

// FF, GG, HH, and II transformations for rounds 1, 2, 3, and 4
static inline void FF(uint32_t& a, uint32_t b, uint32_t c, uint32_t d, uint32_t x, uint32_t s, uint32_t ac) {
  a += F(b, c, d) + x + ac;
  a = RotateLeft(a, s);
  a += b;
}

static inline void GG(uint32_t& a, uint32_t b, uint32_t c, uint32_t d, uint32_t x, uint32_t s, uint32_t ac) {
  a += G(b, c, d) + x + ac;
  a = RotateLeft(a, s);
  a += b;
}

static inline void HH(uint32_t& a, uint32_t b, uint32_t c, uint32_t d, uint32_t x, uint32_t s, uint32_t ac) {
  a += H(b, c, d) + x + ac;
  a = RotateLeft(a, s);
  a += b;
}

static inline void II(uint32_t& a, uint32_t b, uint32_t c, uint32_t d, uint32_t x, uint32_t s, uint32_t ac) {
  a += I(b, c, d) + x + ac;
  a = RotateLeft(a, s);
  a += b;
}

MD5::MD5() {
  // MD5 initialization constants
  state_[0] = 0x67452301;
  state_[1] = 0xefcdab89;
  state_[2] = 0x98badcfe;
  state_[3] = 0x10325476;

  count_[0] = 0;
  count_[1] = 0;
}

void MD5::Update(const uint8_t* data, size_t len) {
  // Compute number of bytes mod 64
  const uint32_t index = (count_[0] >> 3) & 0x3F;

  // Update number of bits
  count_[0] += static_cast<uint32_t>(len << 3);
  if (count_[0] < static_cast<uint32_t>(len << 3)) {
    count_[1]++;
  }
  count_[1] += static_cast<uint32_t>(len >> 29);

  const uint32_t part_len = 64 - index;

  // Transform as many times as possible
  uint32_t i = 0;
  if (len >= part_len) {
    std::memcpy(&buffer_[index], data, part_len);
    Transform(buffer_);

    for (i = part_len; i + 63 < len; i += 64) {
      Transform(&data[i]);
    }

    // Buffer remaining input (start from beginning after transform)
    std::memcpy(buffer_, &data[i], len - i);
  } else {
    // Buffer remaining input (continue from current index)
    std::memcpy(&buffer_[index], &data[i], len - i);
  }
}

void MD5::Finalize(uint8_t digest[16]) {
  static const uint8_t padding[64] = {0x80};

  // Save number of bits
  uint8_t bits[8];
  for (int i = 0; i < 8; i++) {
    bits[i] = static_cast<uint8_t>((count_[i >> 2] >> ((i & 0x3) << 3)) & 0xff);
  }

  // Pad out to 56 mod 64
  const uint32_t index = (count_[0] >> 3) & 0x3f;
  const uint32_t pad_len = (index < 56) ? (56 - index) : (120 - index);
  Update(padding, pad_len);

  // Append length (before padding)
  Update(bits, 8);

  // Store state in digest
  for (int i = 0; i < 16; i++) {
    digest[i] = static_cast<uint8_t>((state_[i >> 2] >> ((i & 0x3) << 3)) & 0xff);
  }
}

void MD5::Transform(const uint8_t block[64]) {
  uint32_t a = state_[0];
  uint32_t b = state_[1];
  uint32_t c = state_[2];
  uint32_t d = state_[3];

  uint32_t x[16];
  for (int i = 0; i < 16; i++) {
    x[i] = static_cast<uint32_t>(block[i * 4]) | (static_cast<uint32_t>(block[i * 4 + 1]) << 8) |
           (static_cast<uint32_t>(block[i * 4 + 2]) << 16) | (static_cast<uint32_t>(block[i * 4 + 3]) << 24);
  }

  // Round 1
  FF(a, b, c, d, x[0], S11, 0xd76aa478);
  FF(d, a, b, c, x[1], S12, 0xe8c7b756);
  FF(c, d, a, b, x[2], S13, 0x242070db);
  FF(b, c, d, a, x[3], S14, 0xc1bdceee);
  FF(a, b, c, d, x[4], S11, 0xf57c0faf);
  FF(d, a, b, c, x[5], S12, 0x4787c62a);
  FF(c, d, a, b, x[6], S13, 0xa8304613);
  FF(b, c, d, a, x[7], S14, 0xfd469501);
  FF(a, b, c, d, x[8], S11, 0x698098d8);
  FF(d, a, b, c, x[9], S12, 0x8b44f7af);
  FF(c, d, a, b, x[10], S13, 0xffff5bb1);
  FF(b, c, d, a, x[11], S14, 0x895cd7be);
  FF(a, b, c, d, x[12], S11, 0x6b901122);
  FF(d, a, b, c, x[13], S12, 0xfd987193);
  FF(c, d, a, b, x[14], S13, 0xa679438e);
  FF(b, c, d, a, x[15], S14, 0x49b40821);

  // Round 2
  GG(a, b, c, d, x[1], S21, 0xf61e2562);
  GG(d, a, b, c, x[6], S22, 0xc040b340);
  GG(c, d, a, b, x[11], S23, 0x265e5a51);
  GG(b, c, d, a, x[0], S24, 0xe9b6c7aa);
  GG(a, b, c, d, x[5], S21, 0xd62f105d);
  GG(d, a, b, c, x[10], S22, 0x2441453);
  GG(c, d, a, b, x[15], S23, 0xd8a1e681);
  GG(b, c, d, a, x[4], S24, 0xe7d3fbc8);
  GG(a, b, c, d, x[9], S21, 0x21e1cde6);
  GG(d, a, b, c, x[14], S22, 0xc33707d6);
  GG(c, d, a, b, x[3], S23, 0xf4d50d87);
  GG(b, c, d, a, x[8], S24, 0x455a14ed);
  GG(a, b, c, d, x[13], S21, 0xa9e3e905);
  GG(d, a, b, c, x[2], S22, 0xfcefa3f8);
  GG(c, d, a, b, x[7], S23, 0x676f02d9);
  GG(b, c, d, a, x[12], S24, 0x8d2a4c8a);

  // Round 3
  HH(a, b, c, d, x[5], S31, 0xfffa3942);
  HH(d, a, b, c, x[8], S32, 0x8771f681);
  HH(c, d, a, b, x[11], S33, 0x6d9d6122);
  HH(b, c, d, a, x[14], S34, 0xfde5380c);
  HH(a, b, c, d, x[1], S31, 0xa4beea44);
  HH(d, a, b, c, x[4], S32, 0x4bdecfa9);
  HH(c, d, a, b, x[7], S33, 0xf6bb4b60);
  HH(b, c, d, a, x[10], S34, 0xbebfbc70);
  HH(a, b, c, d, x[13], S31, 0x289b7ec6);
  HH(d, a, b, c, x[0], S32, 0xeaa127fa);
  HH(c, d, a, b, x[3], S33, 0xd4ef3085);
  HH(b, c, d, a, x[6], S34, 0x4881d05);
  HH(a, b, c, d, x[9], S31, 0xd9d4d039);
  HH(d, a, b, c, x[12], S32, 0xe6db99e5);
  HH(c, d, a, b, x[15], S33, 0x1fa27cf8);
  HH(b, c, d, a, x[2], S34, 0xc4ac5665);

  // Round 4
  II(a, b, c, d, x[0], S41, 0xf4292244);
  II(d, a, b, c, x[7], S42, 0x432aff97);
  II(c, d, a, b, x[14], S43, 0xab9423a7);
  II(b, c, d, a, x[5], S44, 0xfc93a039);
  II(a, b, c, d, x[12], S41, 0x655b59c3);
  II(d, a, b, c, x[3], S42, 0x8f0ccc92);
  II(c, d, a, b, x[10], S43, 0xffeff47d);
  II(b, c, d, a, x[1], S44, 0x85845dd1);
  II(a, b, c, d, x[8], S41, 0x6fa87e4f);
  II(d, a, b, c, x[15], S42, 0xfe2ce6e0);
  II(c, d, a, b, x[6], S43, 0xa3014314);
  II(b, c, d, a, x[13], S44, 0x4e0811a1);
  II(a, b, c, d, x[4], S41, 0xf7537e82);
  II(d, a, b, c, x[11], S42, 0xbd3af235);
  II(c, d, a, b, x[2], S43, 0x2ad7d2bb);
  II(b, c, d, a, x[9], S44, 0xeb86d391);

  state_[0] += a;
  state_[1] += b;
  state_[2] += c;
  state_[3] += d;
}

void MD5::Hash(const std::string& input, uint8_t digest[16]) {
  MD5 md5;
  md5.Update(input);
  md5.Finalize(digest);
}

}  // namespace mygramdb::cache

// NOLINTEND
