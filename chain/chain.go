package chain

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/protolambda/zrnt/eth2/beacon"
	"github.com/protolambda/zrnt/eth2/forkchoice"
	"github.com/protolambda/ztyp/tree"
)

type Root = beacon.Root
type Epoch = beacon.Epoch
type Slot = beacon.Slot
type ValidatorIndex = beacon.ValidatorIndex
type Gwei = beacon.Gwei
type Checkpoint = beacon.Checkpoint


type ChainEntry interface {
	// Slot of this entry
	Slot() Slot
	// BlockRoot returns the last block root, replicating the previous block root if the current slot has none.
	// If replicated, `here` will be false.
	BlockRoot() (root Root, here bool)
	// State root (of the post-state of this entry). Should match state-root in the block at the same slot.
	StateRoot() Root
	// The context of this chain entry (shuffling, proposers, etc.)
	EpochsContext(ctx context.Context) (*beacon.EpochsContext, error)
	// State of the entry, a data-sharing view. Call .Copy() before modifying this to preserve validity.
	State(ctx context.Context) (*beacon.BeaconStateView, error)
	// Block of the entry, can be nil without error if the slot is empty.
	Block(ctx context.Context) (*beacon.BeaconBlock, error)
	// Header of the entry, can be nil without error if the slot is empty.
	Header(ctx context.Context) (*beacon.BeaconBlockHeader, error)
}

type Chain interface {
	ByStateRoot(root Root) (ChainEntry, error)
	ByBlockRoot(root Root) (ChainEntry, error)
	ClosestFrom(fromBlockRoot Root, toSlot Slot) (ChainEntry, error)
	BySlot(slot Slot) (ChainEntry, error)
}

type HotEmptyEntry struct {
	slot Slot
	epc *beacon.EpochsContext
	// state is not a full a copy, it data-shares part of the tree with other hot states.
	state *beacon.BeaconStateView
}

func (e *HotEmptyEntry) Slot() Slot {
	return e.slot
}

func (e *HotEmptyEntry) BlockRoot() (root Root, here bool) {
	return Root{}, false
}

func (e *HotEmptyEntry) StateRoot() Root {
	return e.state.HashTreeRoot(tree.GetHashFn())
}

func (e *HotEmptyEntry) EpochsContext(ctx context.Context) (*beacon.EpochsContext, error) {
	return e.epc, nil
}

func (e *HotEmptyEntry) State(ctx context.Context) (*beacon.BeaconStateView, error) {
	return e.state, nil
}

func (e *HotEmptyEntry) Block(ctx context.Context) (*beacon.BeaconBlock, error) {
	return nil, nil
}

func (e *HotEmptyEntry) Header(ctx context.Context) (*beacon.BeaconBlockHeader, error) {
	return nil, nil
}

type HotBlockEntry struct {
	slot Slot
	epc *beacon.EpochsContext
	state *beacon.BeaconStateView
	// Cached, since structural objects do not cache hash-tree-root, unlike views (like the state)
	blockRoot Root
	block *beacon.BeaconBlock
}

func (e *HotBlockEntry) Slot() Slot {
	return e.slot
}

func (e *HotBlockEntry) BlockRoot() (root Root, here bool) {
	if e.blockRoot == (Root{}) {
		e.blockRoot = e.block.HashTreeRoot()
	}
	return e.blockRoot, true
}

func (e *HotBlockEntry) StateRoot() Root {
	return e.state.HashTreeRoot(tree.GetHashFn())
}

func (e *HotBlockEntry) EpochsContext(ctx context.Context) (*beacon.EpochsContext, error) {
	return e.epc, nil
}

func (e *HotBlockEntry) State(ctx context.Context) (*beacon.BeaconStateView, error) {
	return e.state, nil
}

func (e *HotBlockEntry) Block(ctx context.Context) (*beacon.BeaconBlock, error) {
	return e.block, nil
}

func (e *HotBlockEntry) Header(ctx context.Context) (*beacon.BeaconBlockHeader, error) {
	return e.block.Header(), nil
}

type HotChain interface {
	Chain
	Justified() Checkpoint
	Finalized() Checkpoint
	Head() (ChainEntry, error)
	AddBlock(block *beacon.BeaconBlock, post *beacon.BeaconStateView) error
	AddAttestation(att *beacon.Attestation) error
}

type BlockSlotKey [32 + 8]byte

func (key *BlockSlotKey) Slot() Slot {
	return Slot(binary.LittleEndian.Uint64(key[32:40]))
}

func (key *BlockSlotKey) Root() (out Root) {
	copy(out[:], key[0:32])
	return
}

func NewBlockSlotKey(block Root, slot Slot) (out BlockSlotKey) {
	copy(out[0:32], block[:])
	binary.LittleEndian.PutUint64(out[32:40], uint64(slot))
	return
}

type UnfinalizedChain struct {
	ForkChoice *forkchoice.ForkChoice

	// block++slot -> Entry
	Entries map[BlockSlotKey]ChainEntry
	// state root -> block+slot key
	State2Key map[Root]BlockSlotKey
}

func (uc *UnfinalizedChain) ByStateRoot(root Root) (ChainEntry, error) {
	key, ok := uc.State2Key[root]
	if !ok {
		return nil, fmt.Errorf("unknown state %x", root)
	}
	return uc.ByBlockSlot(key)
}

func (uc *UnfinalizedChain) ByBlockSlot(key BlockSlotKey) (ChainEntry, error) {
	entry, ok := uc.Entries[key]
	if !ok {
		return nil, fmt.Errorf("unknown block slot, root: %x slot: %d", key.Root(), key.Slot())
	}
	return entry, nil
}

func (uc *UnfinalizedChain) ByBlockRoot(root Root) (ChainEntry, error) {
	ref, ok := uc.ForkChoice.GetBlock(root)
	if !ok {
		return nil, fmt.Errorf("unknown block %x", root)
	}
	return uc.ByBlockSlot(NewBlockSlotKey(root, ref.Slot))
}

func (uc *UnfinalizedChain) ClosestFrom(fromBlockRoot Root, toSlot Slot) (ChainEntry, error) {
	before, at, _, err := uc.ForkChoice.BlocksAroundSlot(fromBlockRoot, toSlot)
	if err != nil {
		return nil, err
	}
	if at.Root != (Root{}) {
		return uc.ByBlockSlot(NewBlockSlotKey(at.Root, at.Slot))
	}
	for slot := at.Slot; slot >= before.Slot && slot != 0; slot-- {
		key := NewBlockSlotKey(before.Root, slot)
		entry, ok := uc.Entries[key]
		if ok {
			return entry, nil
		}
	}
	return nil, fmt.Errorf("could not find closest hot block starting from root %x, up to slot %d", fromBlockRoot, toSlot)
}

func (uc *UnfinalizedChain) BySlot(slot Slot) (ChainEntry, error) {
	_, at, _, err := uc.ForkChoice.BlocksAroundSlot(uc.Finalized().Root, slot)
	if err != nil {
		return nil, err
	}
	if at.Slot == slot {
		return uc.ByBlockSlot(NewBlockSlotKey(at.Root, at.Slot))
	}
	return nil, fmt.Errorf("no hot entry known for slot %d", slot)
}

func (uc *UnfinalizedChain) Justified() Checkpoint {
	return uc.ForkChoice.Justified()
}

func (uc *UnfinalizedChain) Finalized() Checkpoint {
	return uc.ForkChoice.Finalized()
}

func (uc *UnfinalizedChain) Head() (ChainEntry, error) {
	ref, err := uc.ForkChoice.FindHead()
	if err != nil {
		return nil, err
	}
	return uc.ByBlockRoot(ref.Root)
}

func (uc *UnfinalizedChain) AddBlock(block *beacon.BeaconBlock, post *beacon.BeaconStateView) error {
	blockRoot := block.HashTreeRoot()

	var finalizedEpoch, justifiedEpoch Epoch
	{
		finalizedCh, err := post.FinalizedCheckpoint()
		if err != nil {
			return err
		}
		finalizedEpoch, err = finalizedCh.Epoch()
		if err != nil {
			return err
		}
		justifiedCh, err := post.CurrentJustifiedCheckpoint()
		if err != nil {
			return err
		}
		justifiedEpoch, err = justifiedCh.Epoch()
		if err != nil {
			return err
		}
	}

	uc.ForkChoice.ProcessBlock(
		forkchoice.BlockRef{Slot: block.Slot, Root: blockRoot},
		block.ParentRoot, justifiedEpoch, finalizedEpoch)
	return nil
}

func (uc *UnfinalizedChain) AddAttestation(att *beacon.Attestation) error {
	blockRoot := att.Data.BeaconBlockRoot
	block, err := uc.ByBlockRoot(blockRoot)
	if err != nil {
		return err
	}
	_, ok := block.(*HotBlockEntry)
	if !ok {
		return errors.New("expected HotBlockEntry, need epochs-context to be present")
	}
	// HotBlockEntry does not use a context, epochs-context is available.
	epc, err := block.EpochsContext(nil)
	if err != nil {
		return err
	}
	committee, err := epc.GetBeaconCommittee(att.Data.Slot, att.Data.Index)
	if err != nil {
		return err
	}
	indexedAtt, err := att.ConvertToIndexed(committee)
	if err != nil {
		return err
	}
	targetEpoch := att.Data.Target.Epoch
	for _, index := range indexedAtt.AttestingIndices {
		uc.ForkChoice.ProcessAttestation(index, blockRoot, targetEpoch)
	}
	return nil
}


type ColdChain interface {
	Start() Slot
	End() Slot
	AddBlock(empties []HotEmptyEntry, block HotBlockEntry)
	Chain
}

type FinalizedEntry struct {
	slot Slot

	// Zeroed if empty.
	blockRoot Root

	stateRoot Root
}

func (e *FinalizedEntry) Slot() Slot {
	return e.slot
}

func (e *FinalizedEntry) BlockRoot() (root Root, here bool) {
	return e.blockRoot, e.blockRoot == (Root{})
}

func (e *FinalizedEntry) StateRoot() Root {
	return e.stateRoot
}

type FinalizedEntryView struct {
	*FinalizedEntry
	finChain *FinalizedChain
}

func (e *FinalizedEntryView) EpochsContext(ctx context.Context) (*beacon.EpochsContext, error) {
	return e.finChain.getEpochsContext(ctx, e.slot)
}

func (e *FinalizedEntryView) State(ctx context.Context) (*beacon.BeaconStateView, error) {
	return e.finChain.getState(ctx, e.slot)
}

func (e *FinalizedEntryView) Block(ctx context.Context) (*beacon.BeaconBlock, error) {
	if e.blockRoot == (Root{}) {
		// empty slot
		return nil, nil
	}
	return e.finChain.getBlock(ctx, e.slot)
}

func (e *FinalizedEntryView) Header(ctx context.Context) (*beacon.BeaconBlockHeader, error) {
	if e.blockRoot == (Root{}) {
		// empty slot
		return nil, nil
	}
	return e.finChain.getHeader(ctx, e.slot)
}

type FinalizedChain struct {
	// Cache of pubkeys, may contain pubkeys that are not finalized,
	// but finalized state will not be in conflict with this cache.
	PubkeyCache *beacon.PubkeyCache
	// Start of the historical data
	AnchorSlot Slot
	// History stores canonical chain by slot, starting at AnchorSlot
	History []FinalizedEntry
	// Proposers, grouped by epoch, starting from AnchorSlot.ToEpoch()
	ProposerHistory [][beacon.SLOTS_PER_EPOCH]ValidatorIndex
	// BlockRoots maps the canonical chain block roots to the block slot
	BlockRoots map[Root]Slot
	// BlockRoots maps the canonical chain state roots to the state slot
	StateRoots map[Root]Slot
}

func (f *FinalizedChain) Start() Slot {
	return f.AnchorSlot
}

func (f *FinalizedChain) End() Slot {
	return f.AnchorSlot + Slot(len(f.History))
}

var UnknownRootErr = errors.New("unknown root")

func (f *FinalizedChain) ByStateRoot(root Root) (ChainEntry, error) {
	slot, ok := f.StateRoots[root]
	if !ok {
		return nil, UnknownRootErr
	}
	return f.BySlot(slot)
}

func (f *FinalizedChain) ByBlockRoot(root Root) (ChainEntry, error) {
	slot, ok := f.StateRoots[root]
	if !ok {
		return nil, UnknownRootErr
	}
	return f.BySlot(slot)
}

func (f *FinalizedChain) ClosestFrom(fromBlockRoot Root, toSlot Slot) (ChainEntry, error) {
	if start := f.Start(); toSlot < start {
		return nil, fmt.Errorf("slot %d is too early. Start is at slot %d", toSlot, start)
	}
	// check if the root is canonical
	_, ok := f.StateRoots[fromBlockRoot]
	if !ok {
		return nil, UnknownRootErr
	}
	// find the slot closest to the requested slot: whatever is still within range
	if end := f.End(); end == 0 {
		return nil, errors.New("empty chain, no data available")
	} else if toSlot >= end {
		toSlot = end - 1
	}
	return f.BySlot(toSlot)
}

func (f *FinalizedChain) BySlot(slot Slot) (ChainEntry, error) {
	if start := f.AnchorSlot; slot < start {
		return nil, fmt.Errorf("slot %d is too early. Chain starts at slot %d", slot, start)
	}
	if end := f.AnchorSlot + Slot(len(f.History)); slot >= end {
		return nil, fmt.Errorf("slot %d is too late. Chain ends at slot %d", slot, end)
	}
	return &FinalizedEntryView{
		FinalizedEntry: &f.History[slot - f.AnchorSlot],
		finChain:       f,
	}, nil
}

func (f *FinalizedChain) Proposers(epoch Epoch) ([beacon.SLOTS_PER_EPOCH]ValidatorIndex, error) {
	anchorEpoch := f.AnchorSlot.ToEpoch()
	if epoch < anchorEpoch {
		return [beacon.SLOTS_PER_EPOCH]ValidatorIndex{}, errors.New("epoch too early for finalized chain")
	}
	if max := f.End().ToEpoch(); epoch > max {
		return [beacon.SLOTS_PER_EPOCH]ValidatorIndex{}, errors.New("epoch too late for finalized chain")
	}
	return f.ProposerHistory[epoch-anchorEpoch], nil
}

func (f *FinalizedChain) getEpochsContext(ctx context.Context, slot Slot) (*beacon.EpochsContext, error) {
	// TODO: context can be partially constructed from contexts around this.
	proposers, err := f.Proposers(slot.ToEpoch())
	if err != nil {
		return nil, err
	}
	epc := &beacon.EpochsContext{
		PubkeyCache: f.PubkeyCache,
		Proposers: proposers,
	}
	// We do not store shuffling for older epochs
	// TODO: maybe store it after all, for archive node functionality?
	if err := epc.LoadShuffling(nil /* TODO shuffle load? */); err != nil {
		return nil, err
	}
	return nil, errors.New("todo")
}

func (f *FinalizedChain) getState(ctx context.Context, slot Slot) (*beacon.BeaconStateView, error) {
	return nil, errors.New("todo")
}

func (f *FinalizedChain) getBlock(ctx context.Context, slot Slot) (*beacon.BeaconBlock, error) {
	return nil, errors.New("todo")
}

func (f *FinalizedChain) getHeader(ctx context.Context, slot Slot) (*beacon.BeaconBlockHeader, error) {
	return nil, errors.New("todo")
}

type FullChain struct {
	HotChain
	ColdChain
}

// TODO: call hot/cold functions based on input + finalized slot

