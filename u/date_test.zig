const std = @import("std");
const testing = std.testing;
const date = @import("utils").date;

test "daysFromCivil: known days and both leap rules" {
    try testing.expectEqual(@as(i64, 0), date.daysFromCivil(1970, 1, 1));
    try testing.expectEqual(@as(i64, -1), date.daysFromCivil(1969, 12, 31));
    try testing.expectEqual(@as(i64, 10_957), date.daysFromCivil(2000, 1, 1));
    // 2000 is a leap year (400 rule), 1900 is not (100 rule).
    try testing.expectEqual(@as(i64, 10_957 + 59), date.daysFromCivil(2000, 2, 29));
    try testing.expectEqual(@as(i64, -25_567), date.daysFromCivil(1900, 1, 1));
    try testing.expectEqual(@as(i64, -25_567 + 59), date.daysFromCivil(1900, 3, 1));
    try testing.expectEqual(@as(i64, 20_714), date.daysFromCivil(2026, 9, 18));
}

test "civilFromDays inverts daysFromCivil across eras" {
    var days: i64 = date.daysFromCivil(1600, 1, 1);
    const last = date.daysFromCivil(2400, 12, 31);
    var expect = date.Civil{ .year = 1600, .month = 1, .day = 1 };
    while (days <= last) : (days += 1) {
        try testing.expectEqual(expect, date.civilFromDays(days));
        try testing.expectEqual(days, date.daysFromCivil(expect.year, expect.month, expect.day));
        // Walk the calendar by hand so the test does not trust the formula.
        expect.day += 1;
        if (expect.day > date.daysInMonth(expect.year, expect.month)) {
            expect.day = 1;
            expect.month += 1;
            if (expect.month > 12) {
                expect.month = 1;
                expect.year += 1;
            }
        }
    }
}

test "daysInMonth follows the leap rules" {
    try testing.expectEqual(@as(i64, 29), date.daysInMonth(2024, 2));
    try testing.expectEqual(@as(i64, 28), date.daysInMonth(2023, 2));
    try testing.expectEqual(@as(i64, 28), date.daysInMonth(1900, 2));
    try testing.expectEqual(@as(i64, 29), date.daysInMonth(2000, 2));
    try testing.expectEqual(@as(i64, 31), date.daysInMonth(2025, 12));
    try testing.expectEqual(@as(i64, 30), date.daysInMonth(2025, 11));
}

test "parseTimestamp reads S3 LastModified stamps: T separator, Z, ms fraction" {
    const us_per_s: i64 = 1_000_000;
    try testing.expectEqual(@as(i64, 0), try date.parseTimestamp("1970-01-01T00:00:00.000Z", true));
    try testing.expectEqual(us_per_s, try date.parseTimestamp("1970-01-01T00:00:01Z", true));
    // 2026-09-18T00:00:00Z = 1789689600 s since the epoch.
    try testing.expectEqual(1_789_689_600 * us_per_s, try date.parseTimestamp("2026-09-18T00:00:00.000Z", true));
    // A leap day, and a sub-second fraction shorter than three digits.
    try testing.expectEqual(1_709_164_800 * us_per_s + 100_000, try date.parseTimestamp("2024-02-29T00:00:00.1Z", true));
    try testing.expectError(error.InvalidTimestamp, date.parseTimestamp("nope", true));
}
