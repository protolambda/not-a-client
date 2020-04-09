package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"github.com/libp2p/go-libp2p"
	connmgr "github.com/libp2p/go-libp2p-connmgr"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	mplex "github.com/libp2p/go-libp2p-mplex"
	"github.com/libp2p/go-libp2p-peerstore/pstoremem"
	secio "github.com/libp2p/go-libp2p-secio"
	yamux "github.com/libp2p/go-libp2p-yamux"
	"github.com/libp2p/go-tcp-transport"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/protolambda/rumor/addrutil"
	"github.com/protolambda/rumor/rpc/methods"
	"github.com/protolambda/rumor/rpc/reqresp"
	"github.com/protolambda/zrnt/eth2/beacon"
	"github.com/protolambda/zrnt/eth2/util/hashing"
	"github.com/protolambda/zssz"
	"github.com/protolambda/zssz/htr"
	"github.com/protolambda/ztyp/tree"
	"github.com/sirupsen/logrus"
	"os"
	"time"
)

func main()  {

	check := func(err error) {
		if err != nil {
			panic(err)
		}
	}

	log := logrus.New()
	log.SetOutput(os.Stdout)
	log.SetLevel(logrus.InfoLevel)
	log.SetFormatter(&logrus.TextFormatter{ForceColors: true, DisableTimestamp: true})

	// load genesis
	var genesisState *beacon.BeaconStateView
	{
		genesisFile := "genesis.ssz"
		fSt, err := os.Stat(genesisFile)
		check(err)
		f, err := os.Open(genesisFile)
		check(err)
		genesisState, err = beacon.AsBeaconStateView(beacon.BeaconStateType.Deserialize(f, uint64(fSt.Size())))
		check(err)
		check(f.Close())
		log.Infoln("loaded genesis state")
	}

	hostCtx, closeHost := context.WithCancel(context.Background())

	// load libp2p
	var h host.Host
	{
		priv, pub, err := crypto.GenerateKeyPairWithReader(crypto.Secp256k1, -1, rand.Reader)
		check(err)
		privBytes, err := priv.Bytes()
		check(err)
		pubBytes, err := pub.Bytes()
		check(err)
		log.WithFields(logrus.Fields{
			"priv": hex.EncodeToString(privBytes),
			"pub": hex.EncodeToString(pubBytes),
		}).Info("made keypair")

		loPeers := 15
		hiPeers := 20

		hostOptions := []libp2p.Option{
			libp2p.Transport(tcp.NewTCPTransport),
			libp2p.Muxer("/yamux/1.0.0", yamux.DefaultTransport),
			libp2p.Muxer("/mplex/6.7.0", mplex.DefaultTransport),
			libp2p.NATPortMap(),
			libp2p.Identity(priv),
			libp2p.Security(secio.ID, secio.New),
			libp2p.Peerstore(pstoremem.NewPeerstore()),
			libp2p.ConnectionManager(connmgr.NewConnManager(loPeers, hiPeers, time.Second*15)),
		}

		h, err = libp2p.New(hostCtx, hostOptions...)
		check(err)
		log.WithField("peer_id", h.ID().Pretty()).Infoln("loaded libp2p host")
	}

	// bind to network interface
	{
		mAddr, err := ma.NewMultiaddr("/ip4/0.0.0.0/tcp/9000")
		check(err)
		check(h.Network().Listen(mAddr))
		log.Infoln("libp2p UP")
		for _, a := range h.Addrs() {
			log.Infof("Listening on: %s", a.String())
		}
	}

	// connect to bootnode
	var bootID peer.ID
	{
		bootnodeEnr := "enr:-Iu4QGuiaVXBEoi4kcLbsoPYX7GTK9ExOODTuqYBp9CyHN_PSDtnLMCIL91ydxUDRPZ-jem-o0WotK6JoZjPQWhTfEsTgmlkgnY0gmlwhDbOLfeJc2VjcDI1NmsxoQLVqNEoCVTC74VmUx25USyFe7lL0TgpXHaCX9CDy9H6boN0Y3CCIyiDdWRwgiMo"
		enrAddr, err := addrutil.ParseEnrOrEnode(bootnodeEnr)
		check(err)
		muAddr, err := addrutil.EnodeToMultiAddr(enrAddr)
		check(err)
		addrInfo, err := peer.AddrInfoFromP2pAddr(muAddr)
		check(err)
		{
			ctx, _ := context.WithTimeout(context.Background(), time.Second*10)
			check(h.Connect(ctx, *addrInfo))
			h.ConnManager().Protect(addrInfo.ID, "bootnode")
		}
		bootID = addrInfo.ID
		log.WithField("bootnode", bootID.Pretty()).Info("connected to bootnode")
	}

	// Modify the RPC response codec to hook it up to ZRNT types
	methods.BlocksByRangeRPCv1.ResponseChunkCodec = reqresp.NewSSZCodec((*beacon.SignedBeaconBlock)(nil))

	// Helper to fetch blocks from bootnode
	getBlocksBatch := func(state *beacon.BeaconStateView, count uint64) ([]*beacon.SignedBeaconBlock, error) {
		slot, err := state.Slot()
		if err != nil {
			return nil, err
		}
		// starting from next slot
		slot += 1
		req := reqresp.RequestSSZInput{
			Obj: &methods.BlocksByRangeReqV1{
				HeadBlockRoot: methods.Root{}, // can be ignored
				StartSlot:     methods.Slot(slot),
				Count:         count,
				Step:          1,
			},
		}
		log.Infof("Requesting slots %d - %d", slot, uint64(slot) + count)

		blocks := make([]*beacon.SignedBeaconBlock, 0, count)

		reqCtx, _ := context.WithTimeout(hostCtx, time.Second*10)
		err = methods.BlocksByRangeRPCv1.RunRequest(reqCtx, h.NewStream, bootID, nil, &req, count,
			func(chunk reqresp.ChunkedResponseHandler) error {
				resultCode := chunk.ResultCode()
				log.Infof("got chunk! chunk index: %d, chunk size: %d, result code: %d", chunk.ChunkIndex(), chunk.ChunkSize(), resultCode)

				switch resultCode {
				case reqresp.ServerErrCode, reqresp.InvalidReqCode:
					msg, err := chunk.ReadErrMsg()
					if err != nil {
						return err
					}
					log.Errorf("Got error chunk (code %d, index %d): %s", resultCode, chunk.ChunkIndex(), msg)
				case reqresp.SuccessCode:
					var block beacon.SignedBeaconBlock
					err := chunk.ReadObj(&block)
					if err != nil {
						return err
					}
					blocks = append(blocks, &block)
					blockRoot := zssz.HashTreeRoot(htr.HashFn(hashing.GetHashFn()), &block, beacon.SignedBeaconBlockSSZ)
					log.Infof("Buffered block for slot %d root %x", block.Message.Slot, blockRoot)
				}
				return nil
			})
		return blocks, err
	}

	// Sync loop
	state := genesisState
	epc, err := state.NewEpochsContext()
	check(err)
	totalTime := float64(0)
	syncLoop: for {
		slot, err := state.Slot()
		check(err)

		log.Infof("state at slot %d -- state root: %x", slot, state.HashTreeRoot(tree.GetHashFn()))

		if slot > 10000 {
			break
		}

		blocks, err := getBlocksBatch(state, 20)
		if err != nil {
			log.Errorf("failed to get blocks batch, got %d blocks, err: %v", len(blocks), err)
			if len(blocks) == 0 {
				// abort, continue loop after a small cooldown, do not progress state
				time.Sleep(time.Second * 5)
				continue
			}
			// continue, at least something to process
		}

		batchTime := float64(0)

		for _, b := range blocks {

			workState, err := beacon.AsBeaconStateView(state.Copy())
			check(err)
			workEpc := epc.Copy()

			startTime := time.Now()
			if err := workState.StateTransition(workEpc, b, true); err != nil {
				log.Errorf("failed to process block at slot %d: %v", b.Message.Slot, err)

				{
					f, err := os.Create("pre.ssz")
					check(err)
					check(state.Serialize(f))
					check(f.Close())
				}
				{
					f, err := os.Create("post.ssz")
					check(err)
					check(workState.Serialize(f))
					check(f.Close())
				}
				{
					f, err := os.Create("block.ssz")
					check(err)
					_, err = zssz.Encode(f, b, beacon.SignedBeaconBlockSSZ)
					check(err)
					check(f.Close())
				}

				// stop
				break syncLoop
			}
			processDelta := time.Since(startTime)
			batchTime += processDelta.Seconds()
			totalTime += processDelta.Seconds()

			epc = workEpc
			state = workState

			log.Infof("processed block for slot %d successfully! duration: %f ms", b.Message.Slot, batchTime*1000.0)
		}
		slotsDelta := blocks[len(blocks)-1].Message.Slot - blocks[0].Message.Slot
		log.Infof("processed batch of %d blocks (%d slots). Time: %f  (%f slots / second)", len(blocks), slotsDelta, batchTime*1000.0, float64(slotsDelta) / batchTime)
		log.Infof("total aggregate processing time: %f seconds. (%f slots / second)", totalTime, float64(slot)/totalTime)
	}

	closeHost()
}
