module not-a-client

go 1.14

require (
	github.com/libp2p/go-libp2p v0.5.1
	github.com/libp2p/go-libp2p-connmgr v0.2.1
	github.com/libp2p/go-libp2p-core v0.3.1
	github.com/libp2p/go-libp2p-mplex v0.2.1
	github.com/libp2p/go-libp2p-peerstore v0.1.4
	github.com/libp2p/go-libp2p-secio v0.2.1
	github.com/libp2p/go-libp2p-yamux v0.2.1
	github.com/libp2p/go-tcp-transport v0.1.1
	github.com/multiformats/go-multiaddr v0.2.0
	github.com/protolambda/rumor v0.0.0-20200408173933-468bcc1774f1
	github.com/protolambda/zrnt v0.11.0
	github.com/protolambda/zssz v0.1.4
	github.com/protolambda/ztyp v0.0.0
	github.com/sirupsen/logrus v1.4.2
)

replace (
	github.com/protolambda/rumor => ../rumor
	github.com/protolambda/zrnt => ../zrnt
	github.com/protolambda/zssz => ../zssz
	github.com/protolambda/ztyp => ../ztyp
)
