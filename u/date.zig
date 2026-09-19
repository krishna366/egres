//! Dates and timestamps: proleptic-Gregorian calendar arithmetic (any
//! year, no tables) and the ISO-8601 text codecs built on it, `YYYY-MM-DD`
//! and `YYYY-MM-DD[T| ]HH:MM:SS[.ffffff][Z|±HH[:MM]]`. SQL date and
//! timestamp values, S3 `LastModified` stamps and SigV4 `x-amz-date` all
//! go through here; storage is int days or int Unix µs, always UTC.

const std = @import("std");
const String = @import("utils.zig").String;

pub fn isLeapYear(y: i64) bool {
    return @rem(y, 4) == 0 and (@rem(y, 100) != 0 or @rem(y, 400) == 0);
}

pub fn daysInMonth(y: i64, m: i64) i64 {
    return switch (m) {
        1, 3, 5, 7, 8, 10, 12 => 31,
        4, 6, 9, 11 => 30,
        else => if (isLeapYear(y)) 29 else 28,
    };
}

/// Proleptic-Gregorian date → days since 1970-01-01, Howard Hinnant's
/// `days_from_civil`. Closed form, no loops, no tables; it and its
/// inverse below are the whole calendar (std.time.epoch goes only from
/// epoch to civil, and only for unsigned years from 1970 on).
///
/// Two ideas make it a formula:
///
/// 1. The year is counted from March 1, not January 1, so February is
///    the LAST month and the leap day is the last day of the year. Then
///    every month's day-of-year is a fixed number that does not depend
///    on whether the year is leap. `y` is the March-based year: a date
///    in January or February belongs to the previous one.
/// 2. The Gregorian calendar repeats every 400 years exactly: 400 * 365
///    + 97 leap days = 146,097 days. An `era` is one such block; every
///    era is the same length, so the day count is `era * 146097` plus
///    the day within the era.
///
/// Symbols (all integers, days unless said otherwise):
///   y     March-based year: `year`, or `year - 1` for January/February.
///   era   Which 400-year block `y` is in, numbered from 0000-03-01:
///         `y / 400`, rounding DOWN also for negative years (hence the
///         `- 399` before a truncating divide). 2026 → era 5.
///   yoe   Year of era, `y - era * 400`, in 0..399. 2026 → 26.
///   mp    Month index in the March-based year, in 0..11: March is 0,
///         February is 11. `month - 3`, or `month + 9` for Jan/Feb.
///   doy   Day of the March-based year, in 0..365. `(153 * mp + 2) / 5`
///         is the day month `mp` starts on. March..July run 31,30,31,
///         30,31 = 153 days and August..December 31,31,30,31,30 = 153,
///         so month starts advance by 30.6 days on average, and
///         `floor(30.6 * mp + 0.4)` lands on every start exactly (0, 31,
///         61, 92, ... 306 for January, 337 for February); the integer
///         form is that expression scaled by 5. Then `+ day - 1` walks
///         into the month.
///   doe   Day of era, in 0..146096: `yoe * 365` plus one leap day per
///         four years, minus one per hundred (the 100-year rule); the
///         400-year leap day needs no term because `yoe` never reaches
///         400, and the era boundary itself accounts for it.
///   719468  Days from 0000-03-01 (day 0 of era 0) to 1970-01-01, the
///         shift onto the Unix epoch.
pub fn daysFromCivil(year: i64, month: i64, day: i64) i64 {
    const y = if (month <= 2) year - 1 else year;
    const era = @divTrunc(if (y >= 0) y else y - 399, 400);
    const yoe = y - era * 400;
    const mp = if (month > 2) month - 3 else month + 9;
    const doy = @divTrunc(153 * mp + 2, 5) + day - 1;
    const doe = yoe * 365 + @divTrunc(yoe, 4) - @divTrunc(yoe, 100) + doy;
    return era * 146_097 + doe - 719_468;
}

pub const Civil = struct { year: i64, month: i64, day: i64 };

/// The inverse of daysFromCivil, Hinnant's `civil_from_days`: the same
/// symbols, recovered in the opposite order. Undo the epoch shift, split
/// off the era, then the year within it, then the month, then the day.
///
///   z     Days since 0000-03-01: `days + 719468`.
///   era   `z / 146097`, rounding down for negative `z` (the `- 146096`
///         before the truncating divide does that).
///   doe   Day of era, `z - era * 146097`, in 0..146096.
///   leaps The leap days that fall before `doe` within the era, so they
///         can be removed before dividing by 365: one per 1460 days
///         (four years), take back one per 36524 days (a century), add
///         back the one at day 146096 (the era's final, 400th-year leap
///         day, which the century rule wrongly removed).
///   yoe   Year of era: `(doe - leaps) / 365`, in 0..399.
///   doy   Day of the March-based year: `doe` minus the days of the
///         `yoe` complete years before it (the same `365 * yoe + yoe/4
///         - yoe/100` as in the forward direction).
///   mp    Month index from the day of year, `(5 * doy + 2) / 153`: the
///         forward formula `(153 * mp + 2) / 5` inverted.
///   day   `doy` minus the start day of month `mp`, plus 1 to be 1-based.
///   month mp back to a calendar month: `mp + 3` for March..December,
///         `mp - 9` for January and February.
///   year  `yoe + era * 400`, plus 1 when the month is January or
///         February, since those belong to the NEXT calendar year.
pub fn civilFromDays(days: i64) Civil {
    const z = days + 719_468;
    const era = @divTrunc(if (z >= 0) z else z - 146_096, 146_097);
    const doe = z - era * 146_097;
    const leaps = @divTrunc(doe, 1460) - @divTrunc(doe, 36_524) +
        @divTrunc(doe, 146_096);
    const yoe = @divTrunc(doe - leaps, 365);
    const doy = doe - (365 * yoe + @divTrunc(yoe, 4) - @divTrunc(yoe, 100));
    const mp = @divTrunc(5 * doy + 2, 153);
    const day = doy - @divTrunc(153 * mp + 2, 5) + 1;
    const month = if (mp < 10) mp + 3 else mp - 9;
    const year = yoe + era * 400 + @as(i64, if (month <= 2) 1 else 0);
    return .{ .year = year, .month = month, .day = day };
}

// Text codecs: Postgres text form ↔ int4 days / int8 Unix µs, UTC.

const us_per_sec: i64 = 1_000_000;
const us_per_day: i64 = 86_400 * us_per_sec;

/// Parse a date literal into days since 1970-01-01. Postgres's date
/// input takes anything its timestamp input does and keeps the day, so
/// this is parseTimestamp with the time of day dropped: `YYYY-MM-DD`,
/// or a full timestamp truncated to its date (a zone offset is ignored,
/// as for a plain timestamp). Anything else is `error.InvalidTimestamp`
/// (→ 22P02).
pub fn parseDate(text: String) !i32 {
    const us = try parseTimestamp(text, false);
    return @intCast(@divFloor(us, us_per_day));
}

/// Render days since 1970-01-01 as `YYYY-MM-DD`, exactly what Postgres
/// prints in the default (ISO) DateStyle.
pub fn formatDate(w: *std.Io.Writer, days: i32) !void {
    const c = civilFromDays(days);
    // Unsigned on purpose (see formatTimestamp).
    try w.print("{d:0>4}-{d:0>2}-{d:0>2}", .{
        @as(u64, @intCast(c.year)),
        @as(u64, @intCast(c.month)),
        @as(u64, @intCast(c.day)),
    });
}

/// Parse `YYYY-MM-DD[ HH:MM[:SS[.ffffff]]][Z|±HH[:MM]]` (a `T` may
/// separate date and time) into Unix µs. Only four-digit years.
/// A trailing zone offset moves a timestamptz to UTC and is ignored
/// for a plain timestamp (Postgres does the same). Fraction digits
/// past six are dropped. Anything else is `error.InvalidTimestamp`
/// (→ 22P02).
pub fn parseTimestamp(text: String, with_tz: bool) !i64 {
    var p = TsParser{ .s = std.mem.trim(u8, text, " ") };
    const year = try p.digits(4);
    try p.expect('-');
    const month = try p.digits(2);
    try p.expect('-');
    const day = try p.digits(2);
    if (month < 1 or month > 12 or day < 1 or day > daysInMonth(year, month))
        return error.InvalidTimestamp;

    var hour: i64 = 0;
    var minute: i64 = 0;
    var second: i64 = 0;
    var frac: i64 = 0;
    if (p.peekIs(' ') or p.peekIs('T') or p.peekIs('t')) {
        p.i += 1;
        hour = try p.digits(2);
        try p.expect(':');
        minute = try p.digits(2);
        if (p.peekIs(':')) {
            p.i += 1;
            second = try p.digits(2);
        }
        if (p.peekIs('.')) {
            p.i += 1;
            frac = try p.fraction();
        }
    }
    if (hour > 23 or minute > 59 or second > 59) return error.InvalidTimestamp;

    // The zone: `Z`, or a signed hour with an optional `:MM` / `MM`.
    var offset_us: i64 = 0;
    while (p.peekIs(' ')) p.i += 1;
    if (p.peekIs('Z') or p.peekIs('z')) {
        p.i += 1;
    } else if (p.peekIs('+') or p.peekIs('-')) {
        const negative = p.peekIs('-');
        p.i += 1;
        var minutes = 60 * try p.digits(2);
        if (p.peekIs(':')) p.i += 1;
        if (p.peekIsDigit()) minutes += try p.digits(2);
        offset_us = minutes * 60 * us_per_sec;
        if (negative) offset_us = -offset_us;
    }
    if (p.i != p.s.len) return error.InvalidTimestamp;

    const days = daysFromCivil(year, month, day);
    const secs = hour * 3600 + minute * 60 + second;
    var us = days * us_per_day + secs * us_per_sec + frac;
    // `12:00+02` is 10:00 UTC: local time minus its offset.
    if (with_tz) us -= offset_us;
    return us;
}

/// Render Unix µs as `YYYY-MM-DD HH:MM:SS[.ffffff]`, the fraction
/// without trailing zeros, plus `+00` for a timestamptz — exactly what
/// Postgres prints in a UTC session.
pub fn formatTimestamp(w: *std.Io.Writer, unix_us: i64, with_tz: bool) !void {
    const c = civilFromDays(@divFloor(unix_us, us_per_day));
    const day_us = @mod(unix_us, us_per_day);
    const secs = @divFloor(day_us, us_per_sec);
    const frac = @mod(day_us, us_per_sec);
    // Unsigned on purpose: a `0` fill on a signed integer prints an
    // explicit `+` sign (`0000+5`). Every field here is non-negative.
    try w.print("{d:0>4}-{d:0>2}-{d:0>2} {d:0>2}:{d:0>2}:{d:0>2}", .{
        @as(u64, @intCast(c.year)),
        @as(u64, @intCast(c.month)),
        @as(u64, @intCast(c.day)),
        @as(u64, @intCast(@divFloor(secs, 3600))),
        @as(u64, @intCast(@mod(@divFloor(secs, 60), 60))),
        @as(u64, @intCast(@mod(secs, 60))),
    });
    if (frac != 0) {
        var buf: [6]u8 = undefined;
        const digits = try std.fmt.bufPrint(&buf, "{d:0>6}", .{@as(u64, @intCast(frac))});
        try w.writeByte('.');
        try w.writeAll(std.mem.trimEnd(u8, digits, "0"));
    }
    if (with_tz) try w.writeAll("+00");
}

/// A cursor over the timestamp text: fixed-width digit fields and
/// one-byte separators.
const TsParser = struct {
    s: String,
    i: usize = 0,

    fn peekIs(self: *const TsParser, c: u8) bool {
        return self.i < self.s.len and self.s[self.i] == c;
    }

    fn peekIsDigit(self: *const TsParser) bool {
        return self.i < self.s.len and std.ascii.isDigit(self.s[self.i]);
    }

    fn expect(self: *TsParser, c: u8) !void {
        if (!self.peekIs(c)) return error.InvalidTimestamp;
        self.i += 1;
    }

    /// Exactly `n` decimal digits, as a number.
    fn digits(self: *TsParser, n: usize) !i64 {
        if (self.i + n > self.s.len) return error.InvalidTimestamp;
        var v: i64 = 0;
        for (self.s[self.i..][0..n]) |c| {
            if (!std.ascii.isDigit(c)) return error.InvalidTimestamp;
            v = v * 10 + (c - '0');
        }
        self.i += n;
        return v;
    }

    /// The digits after the point, scaled to µs: `.5` is 500000,
    /// digits past the sixth are consumed and dropped.
    fn fraction(self: *TsParser) !i64 {
        var us: i64 = 0;
        var n: usize = 0;
        while (self.peekIsDigit()) : (self.i += 1) {
            if (n < 6) us = us * 10 + (self.s[self.i] - '0');
            n += 1;
        }
        if (n == 0) return error.InvalidTimestamp;
        while (n < 6) : (n += 1) us *= 10;
        return us;
    }
};
