package mqtt

import (
	"errors"
	"testing"

	"time"

	"reflect"

	"sync"

	"fmt"

	"github.com/eclipse/paho.mqtt.golang/packets"
)

func TestSendUnordered(t *testing.T) {
	setup := func(sendErr error) (*client, packets.ControlPacket, <-chan error, *sync.WaitGroup) {
		co := NewClientOptions()
		co.SetOrderMatters(false)
		co.SetDeferredAck(true)
		c := NewClient(co).(*client)
		c.oboundP = make(chan *PacketAndToken)
		c.stop = make(chan struct{})

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
					pkt.sentCB(sendErr)
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
		c, cp, errC, _ := setup(nil)

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
		c, cp, errC, _ := setup(errors.New("test error"))

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
		c, cp, _, _ := setup(errors.New("test error"))
		close(c.stop)

		cbErrC := make(chan error)
		go func() { cbErrC <- sendPacketDeferred(c, cp)() }()
		select {
		case cbErr := <-cbErrC:
			if cbErr.Error() != packets.ErrAckNotSent.Error() {
				t.Fatalf("got unexpected callback error: %+v", cbErr)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timeout getting send value")
		}
	})

	t.Run("Racy tests", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			t.Run("Error sending message", func(t *testing.T) {
				t.Parallel()
				c, cp, _, wg := setup(errors.New("test error"))
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
	})
}

func TestSendOrdered(t *testing.T) {
	// Need to also add the corresponding values to the lists for this to work.
	setup := func(sendErr error, qos uint8) (*client, <-chan error, *sync.WaitGroup) {
		co := NewClientOptions()
		co.SetOrderMatters(true)
		co.SetDeferredAck(true)
		c := NewClient(co).(*client)
		c.oboundP = make(chan *PacketAndToken)
		c.ibound = make(chan packets.ControlPacket)
		c.incomingPubChan = make(chan *packets.PublishPacket)
		c.responseC = make(chan respData)
		c.stop = make(chan struct{})
		c.persist = NewMemoryStore()
		c.workers.Add(1)
		go alllogic(c)
		go c.respHandler()

		// pa := packets.NewControlPacket(packets.Puback).(*packets.PubackPacket)
		// pa.MessageID = 1
		// pa.Qos = qos

		errC := make(chan error)

		wg := sync.WaitGroup{}

		go func() {
			select {
			case pkt := <-c.oboundP:
				wg.Wait()
				pkt.sentCB(sendErr)
				errC <- nil
			case <-time.After(2 * time.Second):
				errC <- errors.New("timeout getting packet")
			}
		}()
		return c, errC, &wg
	}

	t.Run("No callback error", func(t *testing.T) {
		t.Parallel()
		c, _, _ := setup(nil, 1)
		defer func() {
			time.Sleep(1 * time.Second)
			close(c.stop)
		}()

		pubPacket := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
		pubPacket.Qos = 1
		pubPacket.MessageID = 5
		select {
		case c.ibound <- pubPacket:
		case <-time.After(2 * time.Second):
			t.Fatal("timeout publishing message")
		}
		fmt.Println("PAST FIRST SELECT")

		select {
		case p := <-c.incomingPubChan:
			errC := make(chan error)
			go func() {
				select {
				case errC <- messageFromPublish(p).Ack():
				case <-time.After(2 * time.Second):
					fmt.Println("ACK TIMEOUT")
				}
			}()
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout getting message from publish packet channel")
		}

		// cbErrC := make(chan error)
		// go func() { cbErrC <- sendPacketDeferred(c, cp)() }()
		// select {
		// case cbErr := <-cbErrC:
		// 	if cbErr != nil {
		// 		t.Fatalf("got unexpected callback error: %+v", cbErr)
		// 	}
		// case <-time.After(2 * time.Second):
		// 	t.Fatal("timeout getting send value")
		// }
		//
		// if err := <-errC; err != nil {
		// 	t.Fatalf("error getting packet: %+v", err)
		// }
	})
	//
	// t.Run("Some callback error", func(t *testing.T) {
	// 	t.Parallel()
	// 	c, cp, errC, _ := setup(errors.New("test error"))
	//
	// 	cbErrC := make(chan error)
	// 	go func() { cbErrC <- sendPacketDeferred(c, cp)() }()
	// 	select {
	// 	case cbErr := <-cbErrC:
	// 		if cbErr.Error() != "test error" {
	// 			t.Fatalf("got unexpected callback error: %+v", cbErr)
	// 		}
	// 	case <-time.After(2 * time.Second):
	// 		t.Fatal("timeout getting send value")
	// 	}
	//
	// 	if err := <-errC; err != nil {
	// 		t.Fatalf("error getting packet: %+v", err)
	// 	}
	// })
	//
	// t.Run("Error no ack", func(t *testing.T) {
	// 	t.Parallel()
	// 	c, cp, _, _ := setup(errors.New("test error"))
	// 	close(c.stop)
	//
	// 	cbErrC := make(chan error)
	// 	go func() { cbErrC <- sendPacketDeferred(c, cp)() }()
	// 	select {
	// 	case cbErr := <-cbErrC:
	// 		if cbErr.Error() != packets.ErrAckNotSent.Error() {
	// 			t.Fatalf("got unexpected callback error: %+v", cbErr)
	// 		}
	// 	case <-time.After(2 * time.Second):
	// 		t.Fatal("timeout getting send value")
	// 	}
	// })
	//
	// t.Run("Racy tests", func(t *testing.T) {
	// 	for i := 0; i < 100; i++ {
	// 		t.Run("Error sending message", func(t *testing.T) {
	// 			t.Parallel()
	// 			c, cp, _, wg := setup(errors.New("test error"))
	// 			wg.Add(1)
	//
	// 			cbErrC := make(chan error)
	// 			go func() { cbErrC <- sendPacketDeferred(c, cp)() }()
	// 			go func() {
	// 				// Wait for the packet to be enqueued.
	// 				time.Sleep(50 * time.Millisecond)
	// 				close(c.stop)
	// 				wg.Done()
	// 			}()
	// 			select {
	// 			case cbErr := <-cbErrC:
	// 				if cbErr.Error() != "test error" {
	// 					t.Fatalf("got unexpected callback error: %+v", cbErr)
	// 				}
	// 			case <-time.After(5 * time.Second):
	// 				t.Fatal("timeout getting send value")
	// 			}
	// 		})
	// 	}
	// })
}
