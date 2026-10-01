/*
 * Copyright (c) 2026 Contributors to the Eclipse Foundation
 *
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Eclipse Public License v2.0
 *  and Eclipse Distribution License v1.0 which accompany this distribution.
 *
 * The Eclipse Public License is available at
 *    https://www.eclipse.org/legal/epl-2.0/
 *  and the Eclipse Distribution License is available at
 *    http://www.eclipse.org/org/documents/edl-v10.php.
 *
 *  SPDX-License-Identifier: EPL-2.0 OR BSD-3-Clause
 */

package packets

import (
	"bytes"
	"testing"
)

// BenchmarkReadPacketPublish measures the cost of decoding a small PUBLISH of the
// kind used for request/response: a topic, Response Topic and Correlation Data
// properties, and a payload of a few bytes.
func BenchmarkReadPacketPublish(b *testing.B) {
	for name, payload := range map[string][]byte{
		"empty-payload": {},
		"12B-payload":   make([]byte, 12),
		"1KiB-payload":  make([]byte, 1024),
	} {
		b.Run(name, func(b *testing.B) {
			p := &Publish{
				Topic:   "rpc/request/1234",
				Payload: payload,
				Properties: &Properties{
					ResponseTopic:   "rpc/response/1234",
					CorrelationData: make([]byte, 12),
				},
			}
			var wire bytes.Buffer
			if _, err := p.WriteTo(&wire); err != nil {
				b.Fatal(err)
			}
			raw := wire.Bytes()

			b.ReportAllocs()
			b.SetBytes(int64(len(raw)))
			r := bytes.NewReader(raw)
			for b.Loop() {
				r.Reset(raw)
				if _, err := ReadPacket(r); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
