//! AWS Signature Version 4 (SigV4) request signing — the auth scheme every
//! S3-compatible store speaks (AWS, GCS interop, SeaweedFS, proxies). Pure
//! stdlib crypto (Sha256 + HmacSha256); no I/O. The S3 client signs exactly
//! three headers — host, x-amz-content-sha256, x-amz-date — which SigV4
//! requires at minimum (host + every x-amz-*); anything else we send
//! (Range, Content-Length, Connection) legally rides unsigned.

const std = @import("std");
const Sha256 = std.crypto.hash.sha2.Sha256;
const HmacSha256 = std.crypto.auth.hmac.sha2.HmacSha256;

pub const hash_hex_len = Sha256.digest_length * 2;

/// Hex sha256 of a payload. The well-known empty-payload digest
/// (e3b0c442...) falls out of data="".
pub fn hashHex(data: []const u8) [hash_hex_len]u8 {
    var digest: [Sha256.digest_length]u8 = undefined;
    Sha256.hash(data, &digest, .{});
    var out: [hash_hex_len]u8 = undefined;
    _ = std.fmt.bufPrint(&out, "{x}", .{&digest}) catch unreachable;
    return out;
}

/// UTC timestamps in the two SigV4 formats, derived from epoch seconds.
pub const AmzDate = struct {
    /// ISO8601 basic: YYYYMMDD'T'HHMMSS'Z' (x-amz-date header).
    stamp: [16]u8,

    pub fn fromEpochSeconds(secs: u64) AmzDate {
        const es = std.time.epoch.EpochSeconds{ .secs = secs };
        const day = es.getEpochDay();
        const year_day = day.calculateYearDay();
        const month_day = year_day.calculateMonthDay();
        const day_secs = es.getDaySeconds();
        var self: AmzDate = undefined;
        _ = std.fmt.bufPrint(&self.stamp, "{d:0>4}{d:0>2}{d:0>2}T{d:0>2}{d:0>2}{d:0>2}Z", .{
            year_day.year,
            month_day.month.numeric(),
            month_day.day_index + 1,
            day_secs.getHoursIntoDay(),
            day_secs.getMinutesIntoHour(),
            day_secs.getSecondsIntoMinute(),
        }) catch unreachable;
        return self;
    }

    /// The YYYYMMDD prefix (credential scope date).
    pub fn date(self: *const AmzDate) []const u8 {
        return self.stamp[0..8];
    }
};

/// Build the Authorization header value for one request. `path` is the
/// canonical URI (absolute, already URI-safe — our object keys are hex
/// names and prefixes are restricted to unreserved characters, so no
/// percent-encoding pass is needed). No query string support (none used).
/// Caller owns the returned slice.
pub fn authorization(
    a: std.mem.Allocator,
    access_key: []const u8,
    secret_key: []const u8,
    region: []const u8,
    method: []const u8,
    path: []const u8,
    host: []const u8,
    payload_hash: []const u8,
    amz: *const AmzDate,
) ![]u8 {
    const canonical = try std.fmt.allocPrint(a,
        "{s}\n{s}\n\nhost:{s}\nx-amz-content-sha256:{s}\nx-amz-date:{s}\n\n" ++
            signed_headers ++ "\n{s}", .{
        method, path, host, payload_hash, &amz.stamp, payload_hash,
    });
    defer a.free(canonical);

    const canonical_hash = hashHex(canonical);
    const sts = try std.fmt.allocPrint(a,
        "AWS4-HMAC-SHA256\n{s}\n{s}/{s}/" ++ service ++ "/aws4_request\n{s}", .{
        &amz.stamp, amz.date(), region, &canonical_hash,
    });
    defer a.free(sts);

    const sig = signature(secret_key, amz.date(), region, service, sts);
    return std.fmt.allocPrint(a,
        "AWS4-HMAC-SHA256 Credential={s}/{s}/{s}/" ++ service ++
            "/aws4_request, SignedHeaders=" ++ signed_headers ++ ", Signature={s}", .{
        access_key, amz.date(), region, &sig,
    });
}

const service = "s3";
const signed_headers = "host;x-amz-content-sha256;x-amz-date";

/// The HMAC signing chain over `string_to_sign`, hex-encoded. `svc` is a
/// parameter (not the s3 const) so tests can run AWS's published
/// test-suite vectors, which use service name "service".
pub fn signature(secret_key: []const u8, date: []const u8, region: []const u8, svc: []const u8, string_to_sign: []const u8) [64]u8 {
    var key: [HmacSha256.mac_length]u8 = undefined;
    {
        var buf: [4 + 64]u8 = undefined; // "AWS4" ++ secret (secrets are ≤ 40B)
        const seed = std.fmt.bufPrint(&buf, "AWS4{s}", .{secret_key}) catch unreachable;
        HmacSha256.create(&key, date, seed);
    }
    HmacSha256.create(&key, region, &key);
    HmacSha256.create(&key, svc, &key);
    HmacSha256.create(&key, "aws4_request", &key);
    HmacSha256.create(&key, string_to_sign, &key);
    var out: [64]u8 = undefined;
    _ = std.fmt.bufPrint(&out, "{x}", .{&key}) catch unreachable;
    return out;
}
