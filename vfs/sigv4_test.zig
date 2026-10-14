// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const testing = std.testing;
const sigv4 = @import("vfs").sigv4;

// The well-known sha256 of the empty payload — sent as
// x-amz-content-sha256 on body-less requests (GET/HEAD/DELETE).
test "sigv4: empty-payload hash" {
    const h = sigv4.hashHex("");
    try testing.expectEqualStrings(
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        &h,
    );
}

// AWS SigV4 test suite, "get-vanilla" vector: the published credentials,
// scope, and string-to-sign must produce the published signature.
// https://docs.aws.amazon.com/general/latest/gr/signature-v4-test-suite.html
test "sigv4: aws test-suite get-vanilla signature" {
    const sts =
        "AWS4-HMAC-SHA256\n" ++
        "20150830T123600Z\n" ++
        "20150830/us-east-1/service/aws4_request\n" ++
        "bb579772317eb040ac9ed261061d46c1f17a8133879d6129b6e1c25292927e63";
    const sig = sigv4.signature(
        "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
        "20150830",
        "us-east-1",
        "service",
        sts,
    );
    try testing.expectEqualStrings(
        "5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31",
        &sig,
    );
}

// The string-to-sign hash above is itself sha256 of the suite's canonical
// request — pin that too, so both halves of the pipeline are covered.
test "sigv4: aws test-suite get-vanilla canonical request hash" {
    const canonical =
        "GET\n" ++
        "/\n" ++
        "\n" ++
        "host:example.amazonaws.com\n" ++
        "x-amz-date:20150830T123600Z\n" ++
        "\n" ++
        "host;x-amz-date\n" ++
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    const h = sigv4.hashHex(canonical);
    try testing.expectEqualStrings(
        "bb579772317eb040ac9ed261061d46c1f17a8133879d6129b6e1c25292927e63",
        &h,
    );
}

test "sigv4: amz date formatting" {
    // 2015-08-30T12:36:00Z
    const amz = sigv4.AmzDate.fromEpochSeconds(1440938160);
    try testing.expectEqualStrings("20150830T123600Z", &amz.stamp);
    try testing.expectEqualStrings("20150830", amz.date());
}

test "sigv4: authorization header shape" {
    const a = testing.allocator;
    const amz = sigv4.AmzDate.fromEpochSeconds(1440938160);
    const payload_hash = sigv4.hashHex("");
    const auth = try sigv4.authorization(
        a,
        "AKIDEXAMPLE",
        "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
        "us-east-1",
        "GET",
        "/bucket/0001_000000000001.sst",
        "",
        "127.0.0.1:8333",
        &payload_hash,
        &amz,
        null,
    );
    defer a.free(auth);
    try testing.expect(std.mem.startsWith(u8, auth, "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/s3/aws4_request, " ++
        "SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature="));
    // Signature is 64 hex chars.
    try testing.expectEqual(@as(usize, 64), auth.len - (std.mem.indexOf(u8, auth, "Signature=").? + "Signature=".len));
}

test "sigv4: session token joins the signed headers" {
    const a = testing.allocator;
    const amz = sigv4.AmzDate.fromEpochSeconds(1440938160);
    const payload_hash = sigv4.hashHex("");
    const args = .{
        "AKIDEXAMPLE",
        "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
        "us-east-1",
        "GET",
        "/bucket/0001_000000000001.sst",
        "",
        "127.0.0.1:8333",
        &payload_hash,
        &amz,
    };
    const with = try @call(.auto, sigv4.authorization, .{a} ++ args ++
        .{@as(?[]const u8, "FwoGZXIvYXdzEBYaDEXAMPLETOKEN")});
    defer a.free(with);
    const without = try @call(.auto, sigv4.authorization, .{a} ++ args ++
        .{@as(?[]const u8, null)});
    defer a.free(without);
    try testing.expect(std.mem.indexOf(u8, with, "SignedHeaders=host;" ++
        "x-amz-content-sha256;x-amz-date;x-amz-security-token, ") != null);
    // The token changes the canonical request, so the signature too.
    const sig_at = std.mem.indexOf(u8, with, "Signature=").?;
    try testing.expect(!std.mem.eql(u8, with[sig_at..], without[sig_at..]));
}
