/**
 * Chunked transfer encoding utilities for ClickHouse TCP protocol.
 * Protocol Revision 54470+ supports optional chunked framing.
 */

/**
 * Wrap data in a chunk frame: [UInt32 LE length][payload][UInt32 LE 0 terminator]
 */
export function wrapChunk(data: Uint8Array): Uint8Array {
  const result = new Uint8Array(4 + data.length + 4);
  const view = new DataView(result.buffer);
  view.setUint32(0, data.length, true);
  result.set(data, 4);
  // Trailing 4 bytes are already 0 (terminator)
  return result;
}

/**
 * State machine for reading chunked data.
 * Accumulates incoming bytes and extracts complete chunk payloads.
 */
export class ChunkedReadState {
  private buffer: Uint8Array = new Uint8Array(0);
  private pendingLength: number | null = null;

  /**
   * Feed incoming bytes and extract any complete chunk payloads.
   * Returns accumulated payload data (may be empty if waiting for more bytes).
   */
  feed(input: Uint8Array): Uint8Array {
    // Append to buffer
    if (this.buffer.length === 0) {
      this.buffer = input;
    } else {
      const newBuf = new Uint8Array(this.buffer.length + input.length);
      newBuf.set(this.buffer, 0);
      newBuf.set(input, this.buffer.length);
      this.buffer = newBuf;
    }

    const payloads: Uint8Array[] = [];
    let offset = 0;

    while (offset < this.buffer.length) {
      // Need length prefix?
      if (this.pendingLength === null) {
        if (this.buffer.length - offset < 4) break; // Need more data
        const view = new DataView(this.buffer.buffer, this.buffer.byteOffset + offset, 4);
        this.pendingLength = view.getUint32(0, true);
        offset += 4;

        // Zero-length chunk = end marker, skip it
        if (this.pendingLength === 0) {
          this.pendingLength = null;
          continue;
        }
      }

      // Have length, need payload
      if (this.buffer.length - offset < this.pendingLength) break; // Need more data

      payloads.push(this.buffer.subarray(offset, offset + this.pendingLength));
      offset += this.pendingLength;
      this.pendingLength = null;
    }

    // Compact buffer
    if (offset > 0) {
      this.buffer = this.buffer.subarray(offset);
    }

    // Concatenate payloads
    if (payloads.length === 0) return new Uint8Array(0);
    if (payloads.length === 1) return payloads[0];

    const totalLen = payloads.reduce((sum, p) => sum + p.length, 0);
    const result = new Uint8Array(totalLen);
    let pos = 0;
    for (const p of payloads) {
      result.set(p, pos);
      pos += p.length;
    }
    return result;
  }

  reset(): void {
    this.buffer = new Uint8Array(0);
    this.pendingLength = null;
  }
}
