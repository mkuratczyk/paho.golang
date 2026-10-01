/*
 * Copyright (c) 2024 Contributors to the Eclipse Foundation
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

// TestPublishPackUnpack packs and unpacks PUBLISH messages at each QOS level and confirms no loss of data
// Trigger was issue #149 which highlighted a difference between the way PUBLISH packets were encoded depending
// upon whether *ControlPacket.WriteTo or *Publish.WriteTo was called (these should have identical results)
func TestPublishPackUnpack(t *testing.T) {
	const controlPacketWriteTo = "ControlPacket.WriteTo"
	const publishWriteTo = "Publish.WriteTo"

	for _, wt := range []string{controlPacketWriteTo, publishWriteTo} {
		for qos := byte(0); qos < 3; qos++ {
			srcCp := NewControlPacket(PUBLISH)
			srcP := srcCp.Content.(*Publish)
			srcP.PacketID = 1
			srcP.QoS = qos
			srcP.Topic = "Test"
			srcP.Payload = []byte("Test")

			var b bytes.Buffer
			var err error

			switch wt {
			case controlPacketWriteTo:
				_, err = srcCp.WriteTo(&b)
			case publishWriteTo:
				_, err = srcP.WriteTo(&b)
			default:
				panic("bug in test")
			}
			if err != nil {
				t.Errorf("%s failed to Write PUBLISH: %s", wt, err)
			}

			dstCp, err := ReadPacket(bytes.NewReader(b.Bytes()))
			if err != nil {
				t.Errorf("%s failed to Read PUBLISH: %s", wt, err)
			}
			dstP, ok := dstCp.Content.(*Publish)
			if !ok {
				t.Fatalf("%s readPacket did not return expected type (got %T)", wt, dstCp.Content)
			}

			if (qos == 0 && dstP.PacketID != 0) || (qos == 1 && dstP.PacketID != 1) {
				t.Errorf("%s QOS %d unexpected Packet ID: %d", wt, qos, dstP.PacketID)
			}
			if dstP.QoS != qos {
				t.Errorf("%s QOS %d unexpected QOS in decoded packet: %d", wt, qos, dstP.QoS)
			}
			if dstP.Topic != "Test" {
				t.Errorf("%s QOS %d unexpected topic:%s", wt, qos, dstP.Topic)
			}
			if bytes.Compare(dstP.Payload, []byte("Test")) != 0 {
				t.Errorf("%s QOS %d unexpected body: %s", wt, qos, dstP.Payload)
			}
		}
	}
}

// TestPublishUnpackPayloadIsolated checks that the decoded payload is an exactly
// sized copy: it must not alias the buffer it was decoded from, and appending to
// it must not touch anything else.
func TestPublishUnpackPayloadIsolated(t *testing.T) {
	for _, size := range []int{0, 1, 12, 600, 70000} {
		want := bytes.Repeat([]byte{0xAB}, size)
		src := &Publish{Topic: "t", Payload: want, Properties: &Properties{}}
		var wire bytes.Buffer
		if _, err := src.WriteTo(&wire); err != nil {
			t.Fatalf("size %d: %v", size, err)
		}
		raw := wire.Bytes()

		cp, err := ReadPacket(bytes.NewReader(raw))
		if err != nil {
			t.Fatalf("size %d: %v", size, err)
		}
		got := cp.Content.(*Publish).Payload
		if got == nil {
			t.Fatalf("size %d: payload is nil, want an empty non-nil slice", size)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("size %d: payload mismatch", size)
		}
		if cap(got) != len(got) {
			t.Errorf("size %d: cap %d, want %d", size, cap(got), len(got))
		}
		for i := range raw {
			raw[i] = 0
		}
		if !bytes.Equal(got, want) {
			t.Errorf("size %d: payload changed when the source buffer was overwritten", size)
		}
	}
}
