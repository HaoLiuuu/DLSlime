#pragma once
#include <array>

#include "../region/host_region.h"

namespace dlslime::tcp::region_wire {
constexpr uint16_t version = 2;
constexpr uint16_t read = 1, write = 2, response = 0x8000;
constexpr uint64_t max_payload = uint64_t{1} << 30;
enum Status : uint32_t {
    ok        = 0,
    stale     = 1,
    denied    = 2,
    bounds    = 3,
    closed    = 4,
    malformed = 5
};
using Bytes = std::array<unsigned char, 72>;
struct Header {
    uint16_t    opcode{};
    uint32_t    status{};
    uint64_t    request{};
    region::Key key;
    uint64_t    offset{}, length{};
    bool        operator==(const Header&) const = default;
};
inline void put(Bytes& b, size_t at, uint64_t value, size_t width)
{
    for (size_t i = 0; i < width; ++i)
        b[at + width - 1 - i] = static_cast<unsigned char>(value >> (i * 8));
}
inline uint64_t get(const Bytes& b, size_t at, size_t width)
{
    uint64_t value = 0;
    for (size_t i = 0; i < width; ++i)
        value = (value << 8) | b[at + i];
    return value;
}
inline Bytes encode(const Header& h)
{
    Bytes b{};
    put(b, 0, 0x444c5332, 4);
    put(b, 4, version, 2);
    put(b, 6, h.opcode, 2);
    put(b, 8, h.status, 4);  // bytes 12..15 are reserved and must be zero
    put(b, 16, h.request, 8);
    put(b, 24, h.key.scope_hi, 8);
    put(b, 32, h.key.scope_lo, 8);
    put(b, 40, h.key.id, 8);
    put(b, 48, h.key.generation, 8);
    put(b, 56, h.offset, 8);
    put(b, 64, h.length, 8);
    return b;
}
inline Header decode(const Bytes& b)
{
    if (get(b, 0, 4) != 0x444c5332 || get(b, 4, 2) != version || get(b, 12, 4))
        throw std::runtime_error("Invalid Region TCP magic, version or reserved bits");
    return {static_cast<uint16_t>(get(b, 6, 2)),
            static_cast<uint32_t>(get(b, 8, 4)),
            get(b, 16, 8),
            {get(b, 24, 8), get(b, 32, 8), get(b, 40, 8), get(b, 48, 8)},
            get(b, 56, 8),
            get(b, 64, 8)};
}
}  // namespace dlslime::tcp::region_wire
