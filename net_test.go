package mqtt

import (
	"context"
	"errors"
	"testing"

	"time"

	"reflect"

	"sync"

	"bytes"

	"sync/atomic"

	"math/rand"

	"fmt"

	"github.com/eclipse/paho.mqtt.golang/packets"
)

func TestSend(t *testing.T) {
	setup := func(sendErr error, ordered, deferred bool) (*client, packets.ControlPacket, <-chan error, *sync.WaitGroup) {
		co := NewClientOptions()
		co.SetOrderMatters(ordered)
		co.SetDeferredAck(deferred)
		c := NewClient(co).(*client)
		c.oboundP = make(chan *PacketAndToken)
		c.stop = make(chan struct{})
		c.workers.Add(1)

		pa := packets.NewControlPacket(packets.Puback).(*packets.PubackPacket)
		pa.MessageID = 1

		errC := make(chan error)

		wg := sync.WaitGroup{}

		go func() {
			select {
			case pkt := <-c.oboundP:
				if !reflect.DeepEqual(pa, pkt.p) {
					errC <- errors.New("packet does not match")
				} else {
					wg.Wait()
					if pkt.sentCB != nil {
						pkt.sentCB(sendErr)
					}
					errC <- nil
				}
			case <-time.After(2 * time.Second):
				errC <- errors.New("timeout getting packet")
			}
		}()
		return c, pa, errC, &wg
	}

	t.Run("No callback error", func(t *testing.T) {
		t.Parallel()
		c, cp, errC, _ := setup(nil, false, true)

		cbErrC := make(chan error)
		go func() { cbErrC <- sendPacketDeferred(c, cp)() }()
		select {
		case cbErr := <-cbErrC:
			if cbErr != nil {
				t.Fatalf("got unexpected callback error: %+v", cbErr)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timeout getting send value")
		}

		if err := <-errC; err != nil {
			t.Fatalf("error getting packet: %+v", err)
		}
	})

	t.Run("Some callback error", func(t *testing.T) {
		t.Parallel()
		c, cp, errC, _ := setup(errors.New("test error"), false, true)

		cbErrC := make(chan error)
		go func() { cbErrC <- sendPacketDeferred(c, cp)() }()
		select {
		case cbErr := <-cbErrC:
			if cbErr.Error() != "test error" {
				t.Fatalf("got unexpected callback error: %+v", cbErr)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timeout getting send value")
		}

		if err := <-errC; err != nil {
			t.Fatalf("error getting packet: %+v", err)
		}
	})

	t.Run("Error no ack", func(t *testing.T) {
		t.Parallel()
		c, cp, _, _ := setup(errors.New("test error"), false, true)
		close(c.stop)

		cbErrC := make(chan error)
		go func() { cbErrC <- sendPacketDeferred(c, cp)() }()
		select {
		case cbErr := <-cbErrC:
			if cbErr.Error() != ErrAckNotSent.Error() {
				t.Fatalf("got unexpected callback error: %+v", cbErr)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timeout getting send value")
		}
	})

	raceTest := func(ordered bool) func(t *testing.T) {
		return func(t *testing.T) {
			for i := 0; i < 100; i++ {
				t.Run("Error sending message", func(t *testing.T) {
					t.Parallel()
					c, cp, _, wg := setup(errors.New("test error"), ordered, true)
					wg.Add(1)

					cbErrC := make(chan error)
					go func() { cbErrC <- sendPacketDeferred(c, cp)() }()
					go func() {
						// Wait for the packet to be enqueued.
						time.Sleep(50 * time.Millisecond)
						close(c.stop)
						wg.Done()
					}()
					select {
					case cbErr := <-cbErrC:
						if cbErr.Error() != "test error" {
							t.Fatalf("got unexpected callback error: %+v", cbErr)
						}
					case <-time.After(5 * time.Second):
						t.Fatal("timeout getting send value")
					}
				})
			}
		}
	}

	t.Run("Racy tests no ordering", raceTest(false))

	t.Run("Racy tests with ordering", raceTest(true))

	t.Run("Send no deferred", func(t *testing.T) {
		t.Parallel()
		c, cp, errC, _ := setup(nil, false, false)

		cbErrC := make(chan error)
		go func() { cbErrC <- sendPacketDeferred(c, cp)() }()
		select {
		case cbErr := <-cbErrC:
			if cbErr != nil {
				t.Fatalf("got unexpected callback error: %+v", cbErr)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timeout getting send value")
		}

		if err := <-errC; err != nil {
			t.Fatalf("error getting packet: %+v", err)
		}
	})
}

func TestSendOrdered(t *testing.T) {
	// Need to also add the corresponding values to the lists for this to work.
	setup := func(sendErr error, deferred bool) *client {
		co := NewClientOptions()
		co.SetOrderMatters(true)
		co.SetDeferredAck(deferred)
		c := NewClient(co).(*client)
		c.oboundP = make(chan *PacketAndToken)
		c.ibound = make(chan packets.ControlPacket)
		c.incomingPubChan = make(chan *packets.PublishPacket)
		c.stop = make(chan struct{})
		c.persist = NewMemoryStore()
		c.workers.Add(1)
		go alllogic(c)

		return c
	}

	id := uint32(1)

	nonDeferred := func(qos uint8) func(t *testing.T) {
		return func(t *testing.T) {
			t.Parallel()

			c := setup(nil, false)
			pubPacket := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
			pubPacket.Qos = qos
			newID := atomic.AddUint32(&id, 1)
			pubPacket.MessageID = uint16(newID)
			pubPacket.Payload = []byte("Hello")

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			// Client send PUBLISH.
			select {
			case c.ibound <- pubPacket:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}

			// Handle PUBLISH channel.
			select {
			case p := <-c.incomingPubChan:
				if p.MessageID != pubPacket.MessageID {
					t.Fatal("Incorrect message ID")
				}
				if !bytes.Equal(p.Payload, pubPacket.Payload) {
					t.Fatal("Incorrect payload")
				}
				if p.Qos != pubPacket.Qos {
					t.Fatal("Incorrect QOS")
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}

			// Server send PUBACK/PUBREC.
			select {
			case pt := <-c.oboundP:
				if qos == 1 {
					if p, ok := pt.p.(*packets.PubackPacket); !ok {
						t.Fatal("Got unexpected packet")
					} else {
						if p.MessageID != pubPacket.MessageID {
							t.Fatal("incorrect message ID")
						}
					}
				}
				if qos == 2 {
					if p, ok := pt.p.(*packets.PubrecPacket); !ok {
						t.Fatal("Got unexpected packet")
					} else {
						if p.MessageID != pubPacket.MessageID {
							t.Fatal("incorrect message ID")
						}
					}
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}

			if qos == 2 {
				// Client send PUBREL.
				pubrel := packets.NewControlPacket(packets.Pubrel).(*packets.PubrelPacket)
				pubrel.MessageID = pubPacket.MessageID
				select {
				case c.ibound <- pubrel:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}

				// Server send PUBCOMP.
				select {
				case pt := <-c.oboundP:
					if p, ok := pt.p.(*packets.PubcompPacket); !ok {
						t.Fatal("Got unexpected packet")
					} else {
						if p.MessageID != pubPacket.MessageID {
							t.Fatal("incorrect message ID")
						}
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
		}
	}

	t.Run("Non-deferred", func(t *testing.T) {
		t.Run("QOS 1", func(t *testing.T) {
			t.Parallel()
			for i := 0; i < 100; i++ {
				t.Run("Works", nonDeferred(1))
			}

		})
		t.Run("QOS 2", func(t *testing.T) {
			t.Parallel()
			for i := 0; i < 100; i++ {
				t.Run("Works", nonDeferred(2))
			}
		})
	})

	deferred := func(qos uint8) func(t *testing.T) {
		return func(t *testing.T) {
			t.Parallel()
			c := setup(nil, true)
			pubPacket := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
			pubPacket.Qos = qos
			newID := atomic.AddUint32(&id, 1)
			pubPacket.MessageID = uint16(newID)
			pubPacket.Payload = []byte("Hello")

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			// Client send PUBLISH.
			select {
			case c.ibound <- pubPacket:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}

			var p *packets.PublishPacket
			select {
			case p = <-c.incomingPubChan:
				if p.MessageID != pubPacket.MessageID {
					t.Fatal("Incorrect message ID")
				}
				if !bytes.Equal(p.Payload, pubPacket.Payload) {
					t.Fatal("Incorrect payload")
				}
				if p.Qos != pubPacket.Qos {
					t.Fatal("Incorrect QOS")
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}

			// Should not get acknowledgement since it is deferred.
			select {
			case <-c.oboundP:
				t.Fatal("Got PUBACK without acknowledgement")
			case <-time.After(100 * time.Millisecond):
			}

			msg := messageFromPublish(p)
			go msg.Ack()

			// Server send PUBACK/PUBREC.
			select {
			case pt := <-c.oboundP:
				if qos == 1 {
					if p, ok := pt.p.(*packets.PubackPacket); !ok {
						t.Fatal("Got unexpected packet")
					} else {
						if p.MessageID != pubPacket.MessageID {
							t.Fatal("incorrect message ID")
						}
					}
				}
				if qos == 2 {
					if p, ok := pt.p.(*packets.PubrecPacket); !ok {
						t.Fatal("Got unexpected packet")
					} else {
						if p.MessageID != pubPacket.MessageID {
							t.Fatal("incorrect message ID")
						}
					}
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}

			if qos == 2 {
				// Client send PUBREL.
				pubrel := packets.NewControlPacket(packets.Pubrel).(*packets.PubrelPacket)
				pubrel.MessageID = pubPacket.MessageID
				select {
				case c.ibound <- pubrel:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}

				// Server send PUBCOMP.
				select {
				case pt := <-c.oboundP:
					if p, ok := pt.p.(*packets.PubcompPacket); !ok {
						t.Fatal("Got unexpected packet")
					} else {
						if p.MessageID != pubPacket.MessageID {
							t.Fatal("incorrect message ID")
						}
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}

		}
	}

	t.Run("Deferred", func(t *testing.T) {
		t.Run("QOS 1", func(t *testing.T) {
			t.Parallel()
			for i := 0; i < 100; i++ {
				t.Run("Works", deferred(1))
			}

		})
		t.Run("QOS 2", func(t *testing.T) {
			t.Parallel()
			for i := 0; i < 100; i++ {
				t.Run("Works", deferred(2))
			}
		})
	})
}

func Test1MillionDuplicates(t *testing.T) {
	DEBUG = NOOPLogger{}
	WARN = NOOPLogger{}
	CRITICAL = NOOPLogger{}
	ERROR = NOOPLogger{}

	rand.Seed(time.Now().UnixNano())

	setup := func(sendErr error, deferred bool) *client {
		co := NewClientOptions()
		co.SetOrderMatters(true)
		co.SetDeferredAck(deferred)
		c := NewClient(co).(*client)
		c.oboundP = make(chan *PacketAndToken)
		c.ibound = make(chan packets.ControlPacket)
		c.incomingPubChan = make(chan *packets.PublishPacket)
		c.stop = make(chan struct{})
		c.persist = NewMemoryStore()
		c.workers.Add(1)
		go alllogic(c)

		return c
	}

	var wg sync.WaitGroup
	id := uint32(1)
	c := setup(nil, true)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	errC := make(chan error)

	sent := make(map[uint16]uint8, 100000)
	var sentMu sync.Mutex

	c.msgRouter.addRoute("#", func(_ Client, m Message) {
		time.AfterFunc(time.Duration(rand.Intn(500)+500)*time.Millisecond, func() {
			// fmt.Println("GOT MESSAGE:", m.MessageID(), m.Duplicate())
			sentMu.Lock()
			sent[m.MessageID()]++
			sentMu.Unlock()

			if err := m.Ack(); err != nil {
				errC <- err
			}
		})
	})
	c.msgRouter.matchAndDispatch(c.incomingPubChan, c.options.Order, c)

	// Handle package acknowledgements.
	// Ensure they should exist.
	go func() {
		var count uint64
		for {
			select {
			case p := <-c.oboundP:
				if pub, ok := p.p.(*packets.PubackPacket); ok {
					func() {
						sentMu.Lock()
						defer sentMu.Unlock()

						if _, idOk := sent[pub.MessageID]; !idOk {
							t.Logf("Unknown message ID: %d, %t", pub.MessageID, pub.Dup)
							errC <- fmt.Errorf("got unknown message ID: %d", pub.MessageID)
						} else {
							sent[pub.MessageID]--
							if sent[pub.MessageID] == 0 {
								delete(sent, pub.MessageID)
							}
							count++
							if count%10000 == 0 {
								t.Logf("Got %d acknowledgements", count)
							}
							wg.Done()
						}
					}()
				} else {
					t.Log("Unknown packet type received")
					errC <- errors.New("unknown packet type received")
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	sendN := func(n int) {
		for i := 0; i < n; i++ {
			pubPacket := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
			pubPacket.TopicName = "test"
			pubPacket.Qos = 1
			newID := atomic.AddUint32(&id, 1)
			pubPacket.MessageID = uint16(newID)
			pubPacket.Payload = []byte("Hello")
			select {
			case c.ibound <- pubPacket:
			case <-ctx.Done():
				t.Log(ctx.Err())
				errC <- ctx.Err()
			}

			p2 := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
			p2.TopicName = "test"
			p2.Qos = 1
			p2.MessageID = uint16(newID)
			p2.Payload = []byte("Hello")
			p2.Dup = true
			select {
			case c.ibound <- p2:
			case <-ctx.Done():
				t.Log(ctx.Err())
				errC <- ctx.Err()
			}
		}
	}

	const perGR = 100000

	// Send the messages.
	for i := 0; i < 10; i++ {
		wg.Add(perGR * 2)
		go sendN(perGR)
	}

	doneC := make(chan struct{})
	go func() {
		wg.Wait()
		close(doneC)
	}()

	for {
		select {
		case <-doneC:
			return
		case err := <-errC:
			t.Fatalf("Error: %+v", err)
		}
	}
}
