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
	// The parent block root. If this is an empty slot, return ok=false.
	ParentRoot() (root Root, ok bool)
	// State root (of the post-state of this entry). Should match state-root in the block at the same slot (if any)
	StateRoot() Root
	// The context of this chain entry (shuffling, proposers, etc.)
	EpochsContext(ctx context.Context) (*beacon.EpochsContext, error)
	// State of the entry, a data-sharing view. Call .Copy() before modifying this to preserve validity.
	State(ctx context.Context) (*beacon.BeaconStateView, error)
}

type Chain interface {
	ByStateRoot(root Root) (ChainEntry, error)
	ByBlockRoot(root Root) (ChainEntry, error)
	ClosestFrom(fromBlockRoot Root, toSlot Slot) (ChainEntry, error)
	BySlot(slot Slot) (ChainEntry, error)
}

type HotEntry struct {
	slot Slot
	epc *beacon.EpochsContext
	state *beacon.BeaconStateView
	blockRoot Root
	parentRoot Root
}

func (e *HotEntry) Slot() Slot {
	return e.slot
}

func (e *HotEntry) ParentRoot() (root Root, ok bool) {
	return e.parentRoot, e.parentRoot == Root{}
}

func (e *HotEntry) BlockRoot() (root Root, here bool) {
	return e.blockRoot, e.parentRoot == Root{}
}

func (e *HotEntry) StateRoot() Root {
	return e.state.HashTreeRoot(tree.GetHashFn())
}

func (e *HotEntry) EpochsContext(ctx context.Context) (*beacon.EpochsContext, error) {
	return e.epc.Clone(), nil
}

func (e *HotEntry) State(ctx context.Context) (*beacon.BeaconStateView, error) {
	// Return a copy of the view, the state itself may not be modified
	return beacon.AsBeaconStateView(e.state.Copy())
}

type HotChain interface {
	Chain
	Justified() Checkpoint
	Finalized() Checkpoint
	Head() (ChainEntry, error)
	AddBlock(ctx context.Context, signedBlock *beacon.SignedBeaconBlock) error
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
	Entries map[BlockSlotKey]*HotEntry
	// state root -> block+slot key
	State2Key map[Root]BlockSlotKey
}

func (uc *UnfinalizedChain) OnPrunedBlock(node *forkchoice.ProtoNode) (pruned []ChainEntry) {
	blockRef := node.Block

	key := NewBlockSlotKey(blockRef.Root, blockRef.Slot)
	entry, ok := uc.Entries[key]
	if ok {
		// Return the block
		pruned = append(pruned, entry)
		// Remove block from hot state
		delete(uc.Entries, key)
		delete(uc.State2Key, entry.StateRoot())
		// There may be empty slots leading up to the block
		prevBlockRoot, _ := entry.ParentRoot()
		for slot := blockRef.Slot; true; slot-- {
			key = NewBlockSlotKey(prevBlockRoot, slot)
			entry, ok = uc.Entries[key]
			if ok {
				pruned = append(pruned, entry)
			} else {
				break
			}
		}
	}
	return
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
	for slot := toSlot; slot >= before.Slot && slot != 0; slot-- {
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

func (uc *UnfinalizedChain) AddBlock(ctx context.Context, signedBlock *beacon.SignedBeaconBlock) error {
	block := &signedBlock.Message
	blockRoot := block.HashTreeRoot()

	pre, err := uc.ClosestFrom(block.ParentRoot, block.Slot)
	if err != nil {
		return err
	}

	if root, ok := pre.BlockRoot(); !ok || root != block.ParentRoot {
		return fmt.Errorf("unknown parent root %x, found other root %x (ok: %v)", block.ParentRoot, root, ok)
	}

	epc, err := pre.EpochsContext(ctx)
	if err != nil {
		return err
	}

	state, err := pre.State(ctx)
	if err != nil {
		return err
	}

	// Process empty slots
	for slot := pre.Slot() + 1; slot < block.Slot; {
		if err := state.ProcessSlot(); err != nil {
			return err
		}
		// Per-epoch transition happens at the start of the first slot of every epoch.
		// (with the slot still at the end of the last epoch)
		isEpochEnd := (slot + 1).ToEpoch() != slot.ToEpoch()
		if isEpochEnd {
			if err := state.ProcessEpoch(epc); err != nil {
				return err
			}
		}
		slot += 1
		if err := state.SetSlot(slot); err != nil {
			return err
		}
		if isEpochEnd {
			if err := epc.RotateEpochs(state); err != nil {
				return err
			}
		}

		// Add empty slot entry
		uc.Entries[NewBlockSlotKey(block.ParentRoot, slot)] = &HotEntry{
			slot:      block.Slot,
			epc:       nil,
			state:     state,
			blockRoot: blockRoot,
			parentRoot: Root{},
		}

		state, err = beacon.AsBeaconStateView(state.Copy())
		if err != nil {
			return err
		}
		epc = epc.Clone()
	}

	if err := state.StateTransition(epc, signedBlock, true); err != nil {
		return err
	}
	// And seal the state, need the header and block/state roots to update.
	if err := state.ProcessSlot(); err != nil {
		return err
	}

	var finalizedEpoch, justifiedEpoch Epoch
	{
		finalizedCh, err := state.FinalizedCheckpoint()
		if err != nil {
			return err
		}
		finalizedEpoch, err = finalizedCh.Epoch()
		if err != nil {
			return err
		}
		justifiedCh, err := state.CurrentJustifiedCheckpoint()
		if err != nil {
			return err
		}
		justifiedEpoch, err = justifiedCh.Epoch()
		if err != nil {
			return err
		}
	}

	uc.Entries[NewBlockSlotKey(blockRoot, block.Slot)] = &HotEntry{
		slot:      block.Slot,
		epc:       nil,
		state:     state,
		blockRoot: blockRoot,
		parentRoot: block.ParentRoot,
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
	_, ok := block.(*HotEntry)
	if !ok {
		return errors.New("expected HotEntry, need epochs-context to be present")
	}
	// HotEntry does not use a context, epochs-context is available.
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
	AddBlock(empties []*HotEntry, block HotEntry)
	Chain
}

type FinalizedEntry struct {
	slot Slot

	// Can be copy of the previous root if empty
	blockRoot Root
	// Zeroed if empty.
	parentRoot Root

	stateRoot Root
}

func (e *FinalizedEntry) Slot() Slot {
	return e.slot
}

func (e *FinalizedEntry) ParentRoot() (root Root, ok bool) {
	return e.parentRoot, e.parentRoot == Root{}
}

func (e *FinalizedEntry) BlockRoot() (root Root, here bool) {
	return e.blockRoot, e.parentRoot == (Root{})
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

func (f *FinalizedChain) OnFinalizedBlock(block forkchoice.BlockRef, state *beacon.BeaconStateView, epc *beacon.EpochsContext) {
	postStateRoot := state.HashTreeRoot(tree.GetHashFn())
	f.History = append(f.History, FinalizedEntry{
		slot:      block.Slot,
		blockRoot: block.Root,
		stateRoot: postStateRoot,
	})
	// If it's a new epoch, store the proposers for the epoch
	if block.Slot.ToEpoch() > f.End().ToEpoch() {
		f.ProposerHistory = append(f.ProposerHistory, *epc.Proposers)
	}
	f.BlockRoots[block.Root] = block.Slot
	f.StateRoots[postStateRoot] = block.Slot
}

func (f *FinalizedChain) Proposers(epoch Epoch) (*[beacon.SLOTS_PER_EPOCH]ValidatorIndex, error) {
	anchorEpoch := f.AnchorSlot.ToEpoch()
	if epoch < anchorEpoch {
		return nil, errors.New("epoch too early for finalized chain")
	}
	if max := f.End().ToEpoch(); epoch > max {
		return nil, errors.New("epoch too late for finalized chain")
	}
	return &f.ProposerHistory[epoch-anchorEpoch], nil
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
	state, err := f.getState(ctx, slot)
	if err != nil {
		return nil, err
	}
	if err := epc.LoadShuffling(state); err != nil {
		return nil, err
	}
	return epc, nil
}

func (f *FinalizedChain) getState(ctx context.Context, slot Slot) (*beacon.BeaconStateView, error) {
	return nil, errors.New("todo: load state froms tate storage")
}

type FullChain struct {
	HotChain
	ColdChain
}

// TODO: call hot/cold functions based on input + finalized slot

