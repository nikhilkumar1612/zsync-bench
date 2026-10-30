const std = @import("std");

const magic = "ZSYNCLOG";
const header_len = 16;
const frame_header_len = 12;
const digest_len = 32;
const max_event_bytes = 16 * 1024 * 1024;

pub fn main() !void {
    var gpa: std.heap.GeneralPurposeAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);
    if (args.len != 2) {
        std.debug.print("usage: zsync-dataset-zig <dataset-directory>\n", .{});
        return error.InvalidArguments;
    }

    const path = try std.fs.path.join(allocator, &.{ args[1], "events.bin" });
    defer allocator.free(path);
    const file = try std.fs.cwd().openFile(path, .{});
    defer file.close();
    var file_header: [header_len]u8 = undefined;
    if (try file.readAll(&file_header) != header_len) return error.TruncatedHeader;
    if (!std.mem.eql(u8, file_header[0..8], magic)) return error.InvalidMagic;
    if (readU16(file_header[8..10]) != 1 or readU16(file_header[10..12]) != 0) return error.UnsupportedVersion;
    if (readU32(file_header[12..16]) != 0) return error.UnsupportedFlags;

    var stdout_buffer: [4096]u8 = undefined;
    var stdout_writer = std.fs.File.stdout().writer(&stdout_buffer);
    const out = &stdout_writer.interface;
    defer out.flush() catch {};

    while (true) {
        var frame_header: [frame_header_len]u8 = undefined;
        const header_bytes = try file.readAll(&frame_header);
        if (header_bytes == 0) break;
        if (header_bytes != frame_header_len) return error.TruncatedFrameHeader;
        const payload_len: usize = readU32(frame_header[0..4]);
        const frame_sequence = readU64(frame_header[4..12]);
        if (payload_len > max_event_bytes) return error.EventTooLarge;
        const body = try allocator.alloc(u8, payload_len + digest_len);
        defer allocator.free(body);
        if (try file.readAll(body) != body.len) return error.TruncatedFrame;
        const payload = body[0..payload_len];
        const expected_digest = body[payload_len..];
        var hasher = std.crypto.hash.sha2.Sha256.init(.{});
        hasher.update(&frame_header);
        hasher.update(payload);
        var actual_digest: [digest_len]u8 = undefined;
        hasher.final(&actual_digest);
        if (!std.mem.eql(u8, expected_digest, &actual_digest)) return error.FrameDigestMismatch;

        const parsed = try std.json.parseFromSlice(std.json.Value, allocator, payload, .{});
        defer parsed.deinit();
        if (parsed.value != .object) return error.InvalidEnvelope;
        const object = parsed.value.object;
        const sequence_value = object.get("sequence") orelse return error.MissingSequence;
        if (sequence_value != .integer or sequence_value.integer < 0) return error.InvalidSequence;
        const sequence: u64 = @intCast(sequence_value.integer);
        if (sequence != frame_sequence) return error.SequenceMismatch;
        const kind_value = object.get("kind") orelse return error.MissingKind;
        if (kind_value != .string) return error.InvalidKind;
        const digest_value = object.get("payload_sha256") orelse return error.MissingPayloadDigest;
        if (digest_value != .string or digest_value.string.len != 64) return error.InvalidPayloadDigest;
        try out.print("{d} {s} {s}\n", .{ sequence, kind_value.string, digest_value.string });
    }
    try out.flush();
}

fn readU16(bytes: []const u8) u16 {
    return @as(u16, bytes[0]) | (@as(u16, bytes[1]) << 8);
}
fn readU32(bytes: []const u8) u32 {
    return @as(u32, bytes[0]) | (@as(u32, bytes[1]) << 8) | (@as(u32, bytes[2]) << 16) | (@as(u32, bytes[3]) << 24);
}
fn readU64(bytes: []const u8) u64 {
    var value: u64 = 0;
    for (bytes[0..8], 0..) |byte, index| value |= @as(u64, byte) << @intCast(index * 8);
    return value;
}
