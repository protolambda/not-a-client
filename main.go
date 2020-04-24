package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
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
	"sync"
	"time"
)

func main() {

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
		genesisFile := "topaz_genesis.ssz"
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
			"pub":  hex.EncodeToString(pubBytes),
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

	backgroundRpcCtxFn := func() context.Context { return hostCtx }

	peerInfoStore := PeerInfos{}

	ourMetadata := MetaData{SeqNumber: 1}

	// Handle ping
	pingHandler := func(ctx context.Context, peerId peer.ID, handler reqresp.ChunkedRequestHandler) {
		var ping methods.Ping
		if err := handler.ReadRequest(&ping); err != nil {
			log.WithFields(logrus.Fields{"err": err, "peer": peerId}).Debug("got bad ping")
			_ = handler.WriteInvalidRequestChunk("bad ping")
			return
		}
		currentInfo, _ := peerInfoStore.Find(peerId)
		if newer := currentInfo.RegisterSeqClaim(uint64(ping)); !newer {
			log.WithFields(logrus.Fields{"ping": ping, "peer": peerId}).Debug("got old ping")
			// Got ping, but had better claim earlier. Bad peer
			_ = handler.WriteInvalidRequestChunk("ping is old")
			return
		} else {
			// Stop trying to fetch after too many tries
			if fetchCount := currentInfo.RegisterMetaFetch(); fetchCount < MaxMetadataTries {
				log.WithFields(logrus.Fields{
					"ping": ping, "info": currentInfo.String(), "peer": peerId,
				}).Debug("interested in ping, retrieving metadata")
				// TODO request metadata in go routine
			}
		}

		pong := methods.Pong(ourMetadata.SeqNumber)
		if err := handler.WriteResponseChunk(&pong); err != nil {
			log.WithFields(logrus.Fields{"err": err, "pong": pong, "peer": peerId}).Debug("failed to write pong")
		} else {
			log.WithFields(logrus.Fields{"pong": pong, "peer": peerId}).Debug("responded with pong")
		}
	}
	h.SetStreamHandler(methods.PingRPCv1.Protocol+"_snappy",
		methods.PingRPCv1.MakeStreamHandler(backgroundRpcCtxFn, reqresp.SnappyCompression{}, pingHandler))
	h.SetStreamHandler(methods.PingRPCv1.Protocol,
		methods.PingRPCv1.MakeStreamHandler(backgroundRpcCtxFn, nil, pingHandler))

	// Handle metadata
	metadataHandler := func(ctx context.Context, peerId peer.ID, handler reqresp.ChunkedRequestHandler) {
		if err := handler.WriteResponseChunk(&ourMetadata); err != nil {
			log.WithFields(logrus.Fields{"err": err, "md": ourMetadata, "peer": peerId}).Debug("failed to write metadata")
		} else {
			log.WithFields(logrus.Fields{"md": ourMetadata, "peer": peerId}).Debug("responded with metadata")
		}
	}
	h.SetStreamHandler(methods.MetaDataRPCv1.Protocol+"_snappy",
		methods.MetaDataRPCv1.MakeStreamHandler(backgroundRpcCtxFn, reqresp.SnappyCompression{}, metadataHandler))
	h.SetStreamHandler(methods.MetaDataRPCv1.Protocol,
		methods.MetaDataRPCv1.MakeStreamHandler(backgroundRpcCtxFn, nil, metadataHandler))

	checkFatal := func(err error) {
		if err != nil {
			log.Error(err)
			os.Exit(1)
		}
	}

	forkV, err := genesisState.Fork()
	checkFatal(err)
	fork, err := forkV.Raw()
	checkFatal(err)
	genValRoot, err := genesisState.GenesisValidatorsRoot()
	checkFatal(err)
	forkDigest := beacon.ComputeForkDigest(fork.CurrentVersion, genValRoot)

	forkVersion := [4]byte{240, 113, 198, 108}
	if forkDigest != forkVersion {
		log.Error("unexpected fork digest")
		os.Exit(1)
	}

	ourStatus := Status{
		HeadForkVersion: methods.ForkVersion(forkVersion),
		FinalizedRoot:   methods.Root{},
		FinalizedEpoch:  0,
		HeadRoot:        methods.Root(genesisState.HashTreeRoot(tree.GetHashFn())),
		HeadSlot:        0,
	}
	// Handle status
	statusHandler := func(ctx context.Context, peerId peer.ID, handler reqresp.ChunkedRequestHandler) {
		var peerStatus methods.Status
		if err := handler.ReadRequest(&peerStatus); err != nil {
			log.WithFields(logrus.Fields{"err": err, "peer": peerId}).Debug("got bad status")
			_ = handler.WriteInvalidRequestChunk("bad status")
			return
		}
		currentInfo, _ := peerInfoStore.Find(peerId)
		currentInfo.RegisterStatus(peerStatus)
		log.WithFields(logrus.Fields{"status": peerStatus, "peer": peerId}).Debug("received status update")

		sentStatus := ourStatus
		if err := handler.WriteResponseChunk(&sentStatus); err != nil {
			log.WithFields(logrus.Fields{"err": err, "sent": sentStatus.String(), "peer": peerId}).Debug("failed to write pong")
		} else {
			log.WithFields(logrus.Fields{"sent": sentStatus.String(), "peer": peerId}).Debug("responded with status")
		}
	}
	h.SetStreamHandler(methods.StatusRPCv1.Protocol+"_snappy",
		methods.StatusRPCv1.MakeStreamHandler(backgroundRpcCtxFn, reqresp.SnappyCompression{}, statusHandler))
	h.SetStreamHandler(methods.StatusRPCv1.Protocol,
		methods.StatusRPCv1.MakeStreamHandler(backgroundRpcCtxFn, nil, statusHandler))

	// connect to bootnode
	var bootID peer.ID
	{
		bootnodeEnr := "enr:-LK4QEhBFOo5fvfbxcTVPcYsbg_5qQAxuRuxLNVbgPQXA9x9H0bNwYr_-4Q2gdMW8cq4JHgv-1fLsfXes4ZMDNh6528Bh2F0dG5ldHOIAAAAAAAAAACEZXRoMpDwccZsAAAAAP__________gmlkgnY0gmlwhDZd64iJc2VjcDI1NmsxoQPxitE8Ou_ce6dW_AyFqjEeJaRB5C2ohcHev_nL2tyWSoN0Y3CCIyiDdWRwgiMo"
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

	{
		// Ask node for status
		reqCtx, _ := context.WithTimeout(hostCtx, time.Second*5)
		req := reqresp.RequestSSZInput{
			Obj: &ourStatus,
		}
		if err := methods.StatusRPCv1.RunRequest(reqCtx, h.NewStream, bootID, reqresp.SnappyCompression{}, &req, 1,
			func(chunk reqresp.ChunkedResponseHandler) error {
				var peerStatus Status
				if err := chunk.ReadObj(&peerStatus); err != nil {
					return err
				}
				currentInfo, _ := peerInfoStore.Find(bootID)
				currentInfo.RegisterStatus(peerStatus)

				log.WithFields(logrus.Fields{"status": peerStatus.String(), "peer": bootID}).Debug("retrieved peer status")
				return nil
			}); err != nil {
			log.WithFields(logrus.Fields{"err": err, "peer": bootID}).Debug("failed getting peer status")
		}
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
				StartSlot: methods.Slot(slot),
				Count:     count,
				Step:      1,
			},
		}
		log.Infof("Requesting slots %d - %d", slot, uint64(slot)+count)

		blocks := make([]*beacon.SignedBeaconBlock, 0, count)

		reqCtx, _ := context.WithTimeout(hostCtx, time.Second*10)
		err = methods.BlocksByRangeRPCv1.RunRequest(reqCtx, h.NewStream, bootID, reqresp.SnappyCompression{}, &req, count,
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
syncLoop:
	for {
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

			startTime := time.Now()
			if err := workState.StateTransition(epc, b, true); err != nil {
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

			state = workState

			log.Infof("processed block for slot %d successfully! duration: %f ms", b.Message.Slot, batchTime*1000.0)
		}
		if len(blocks) > 0 {
			slotsDelta := blocks[len(blocks)-1].Message.Slot - blocks[0].Message.Slot
			log.Infof("processed batch of %d blocks (%d slots). Time: %f  (%f slots / second)", len(blocks), slotsDelta, batchTime*1000.0, float64(slotsDelta)/batchTime)
			log.Infof("total aggregate processing time: %f seconds. (%f slots / second)", totalTime, float64(slot)/totalTime)
		} else {
			log.Infoln("got no blocks, waiting for 5 seconds")
			time.Sleep(time.Second * 5)
		}
	}

	closeHost()
}

// Stop trying to fetch after this many tries
const MaxMetadataTries = 10

type MetaData = methods.MetaData
type Status = methods.Status

type PeerInfo struct {
	md MetaData
	// highest claimed seq nr
	claimedSeq         uint64
	status             Status
	ongoingMetaFetches uint64
	sync.Mutex
}

func (pi *PeerInfo) Metadata() MetaData {
	return pi.md
}

func (pi *PeerInfo) ClaimedSeq() uint64 {
	return pi.claimedSeq
}

func (pi *PeerInfo) Status() Status {
	return pi.status
}

func (pi *PeerInfo) String() string {
	return fmt.Sprintf("info:(meta: %s, claim: %d, status: %s, fetches: %d)",
		pi.md.String(), pi.claimedSeq, pi.status.String(), pi.ongoingMetaFetches)
}

// RegisterSeqClaim updates the latest supposed seq nr of the peer
func (pi *PeerInfo) RegisterSeqClaim(seq uint64) (newer bool) {
	pi.Lock()
	defer pi.Unlock()
	newer = pi.claimedSeq < seq
	if newer {
		pi.claimedSeq = seq
	}
	return
}

// RegisterMetaFetch increments how many times we tried to get the peer metadata
// without satisfying answer, returning the counter.
func (pi *PeerInfo) RegisterMetaFetch() uint64 {
	pi.Lock()
	defer pi.Unlock()
	pi.ongoingMetaFetches++
	return pi.ongoingMetaFetches
}

// RegisterMetadata updates metadata, if newer than previous. Resetting ongoing fetch counter if it's new enough
func (pi *PeerInfo) RegisterMetadata(md MetaData) (newer bool) {
	pi.Lock()
	defer pi.Unlock()
	newer = pi.md.SeqNumber < md.SeqNumber
	if newer {
		if md.SeqNumber >= pi.claimedSeq {
			// if it is newer or equal to best, we can reset the ongoing fetches
			pi.ongoingMetaFetches = 0
		}
		pi.md = md
		if pi.md.SeqNumber > pi.claimedSeq {
			pi.claimedSeq = pi.md.SeqNumber
		}
	}
	return
}

// RegisterStatus updates latest peer status
func (pi *PeerInfo) RegisterStatus(st Status) {
	pi.Lock()
	defer pi.Unlock()
	pi.status = st
	return
}

type PeerInfos struct {
	// peer.ID -> *PeerInfo
	infos sync.Map
}

// Find looks for a peer info, and creates a new peer info if necessary
func (ps *PeerInfos) Find(id peer.ID) (pi *PeerInfo, ok bool) {
	pii, loaded := ps.infos.LoadOrStore(id, &PeerInfo{})
	return pii.(*PeerInfo), loaded
}
