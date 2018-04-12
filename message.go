/*
 * Copyright (c) 2013 IBM Corp.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * Contributors:
 *    Seth Hoenig
 *    Allan Stockdill-Mander
 *    Mike Robertson
 */

package mqtt

import (
	"sync"

	"github.com/eclipse/paho.mqtt.golang/packets"
)

// Message defines the externals that a message implementation must support
// these are received messages that are passed to the callbacks, not internal
// messages
type Message interface {
	Duplicate() bool
	Qos() byte
	Retained() bool
	Topic() string
	MessageID() uint16
	Payload() []byte
	Ack() error
}

type message struct {
	duplicate bool
	qos       byte
	retained  bool
	topic     string
	messageID uint16
	payload   []byte
	ackOnce   sync.Once
	ackCB     func() error
}

func (m *message) Duplicate() bool {
	return m.duplicate
}

func (m *message) Qos() byte {
	return m.qos
}

func (m *message) Retained() bool {
	return m.retained
}

func (m *message) Topic() string {
	return m.topic
}

func (m *message) MessageID() uint16 {
	return m.messageID
}

func (m *message) Payload() []byte {
	return m.payload
}

// Ack acknowledges that the message has been processed.
//
// If the client has not been configured to enable unordered message processing,
// then delays calling this acknowledgement function will delay acknowledgement
// processing of all subsequent messages.
//
// Only the first call to this function will send an acknowledgement. Subsequent
// calls will do nothing.
func (m *message) Ack() (e error) {
	m.ackOnce.Do(func() {
		if m.ackCB != nil {
			e = m.ackCB()
		}
	})
	return
}

func messageFromPublish(p *packets.PublishPacket) Message {
	return &message{
		duplicate: p.Dup,
		qos:       p.Qos,
		retained:  p.Retain,
		topic:     p.TopicName,
		messageID: p.MessageID,
		payload:   p.Payload,
		ackCB:     p.AckCB,
	}
}

func newConnectMsgFromOptions(options *ClientOptions) *packets.ConnectPacket {
	m := packets.NewControlPacket(packets.Connect).(*packets.ConnectPacket)

	m.CleanSession = options.CleanSession
	m.WillFlag = options.WillEnabled
	m.WillRetain = options.WillRetained
	m.ClientIdentifier = options.ClientID

	if options.WillEnabled {
		m.WillQos = options.WillQos
		m.WillTopic = options.WillTopic
		m.WillMessage = options.WillPayload
	}

	username := options.Username
	password := options.Password
	if options.CredentialsProvider != nil {
		username, password = options.CredentialsProvider()
	}

	if username != "" {
		m.UsernameFlag = true
		m.Username = username
		//mustn't have password without user as well
		if password != "" {
			m.PasswordFlag = true
			m.Password = []byte(password)
		}
	}

	m.Keepalive = uint16(options.KeepAlive)

	return m
}
