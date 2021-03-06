/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package tabletmanager

import (
	"flag"
	"fmt"
	"strings"
	"time"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var (
	enableSemiSync   = flag.Bool("enable_semi_sync", false, "Enable semi-sync when configuring replication, on master and replica tablets only (rdonly tablets will not ack).")
	setSuperReadOnly = flag.Bool("use_super_read_only", false, "Set super_read_only flag when performing planned failover.")
)

// ReplicationStatus returns the replication status
func (tm *TabletManager) ReplicationStatus(ctx context.Context) (*replicationdatapb.Status, error) {
	status, err := tm.MysqlDaemon.ReplicationStatus()
	if err != nil {
		return nil, err
	}
	return mysql.ReplicationStatusToProto(status), nil
}

// MasterPosition returns the master position
func (tm *TabletManager) MasterPosition(ctx context.Context) (string, error) {
	pos, err := tm.MysqlDaemon.MasterPosition()
	if err != nil {
		return "", err
	}
	return mysql.EncodePosition(pos), nil
}

// WaitForPosition returns the master position
func (tm *TabletManager) WaitForPosition(ctx context.Context, pos string) error {
	mpos, err := mysql.DecodePosition(pos)
	if err != nil {
		return err
	}
	return tm.MysqlDaemon.WaitMasterPos(ctx, mpos)
}

// StopReplication will stop the mysql. Works both when Vitess manages
// replication or not (using hook if not).
func (tm *TabletManager) StopReplication(ctx context.Context) error {
	if err := tm.lock(ctx); err != nil {
		return err
	}
	defer tm.unlock()

	return tm.stopReplicationLocked(ctx)
}

func (tm *TabletManager) stopReplicationLocked(ctx context.Context) error {

	// Remember that we were told to stop, so we don't try to
	// restart ourselves (in replication_reporter).
	tm.setReplicationStopped(true)

	// Also tell Orchestrator we're stopped on purpose for some Vitess task.
	// Do this in the background, as it's best-effort.
	go func() {
		if tm.orc == nil {
			return
		}
		if err := tm.orc.BeginMaintenance(tm.Tablet(), "vttablet has been told to StopReplication"); err != nil {
			log.Warningf("Orchestrator BeginMaintenance failed: %v", err)
		}
	}()

	return tm.MysqlDaemon.StopReplication(tm.hookExtraEnv())
}

// StopReplicationMinimum will stop the replication after it reaches at least the
// provided position. Works both when Vitess manages
// replication or not (using hook if not).
func (tm *TabletManager) StopReplicationMinimum(ctx context.Context, position string, waitTime time.Duration) (string, error) {
	if err := tm.lock(ctx); err != nil {
		return "", err
	}
	defer tm.unlock()

	pos, err := mysql.DecodePosition(position)
	if err != nil {
		return "", err
	}
	waitCtx, cancel := context.WithTimeout(ctx, waitTime)
	defer cancel()
	if err := tm.MysqlDaemon.WaitMasterPos(waitCtx, pos); err != nil {
		return "", err
	}
	if err := tm.stopReplicationLocked(ctx); err != nil {
		return "", err
	}
	pos, err = tm.MysqlDaemon.MasterPosition()
	if err != nil {
		return "", err
	}
	return mysql.EncodePosition(pos), nil
}

// StartReplication will start the mysql. Works both when Vitess manages
// replication or not (using hook if not).
func (tm *TabletManager) StartReplication(ctx context.Context) error {
	if err := tm.lock(ctx); err != nil {
		return err
	}
	defer tm.unlock()

	tm.setReplicationStopped(false)

	// Tell Orchestrator we're no longer stopped on purpose.
	// Do this in the background, as it's best-effort.
	go func() {
		if tm.orc == nil {
			return
		}
		if err := tm.orc.EndMaintenance(tm.Tablet()); err != nil {
			log.Warningf("Orchestrator EndMaintenance failed: %v", err)
		}
	}()

	if err := tm.fixSemiSync(tm.Tablet().Type); err != nil {
		return err
	}
	return tm.MysqlDaemon.StartReplication(tm.hookExtraEnv())
}

// StartReplicationUntilAfter will start the replication and let it catch up
// until and including the transactions in `position`
func (tm *TabletManager) StartReplicationUntilAfter(ctx context.Context, position string, waitTime time.Duration) error {
	if err := tm.lock(ctx); err != nil {
		return err
	}
	defer tm.unlock()

	waitCtx, cancel := context.WithTimeout(ctx, waitTime)
	defer cancel()

	pos, err := mysql.DecodePosition(position)
	if err != nil {
		return err
	}

	return tm.MysqlDaemon.StartReplicationUntilAfter(waitCtx, pos)
}

// GetReplicas returns the address of all the replicas
func (tm *TabletManager) GetReplicas(ctx context.Context) ([]string, error) {
	return mysqlctl.FindReplicas(tm.MysqlDaemon)
}

// ResetReplication completely resets the replication on the host.
// All binary and relay logs are flushed. All replication positions are reset.
func (tm *TabletManager) ResetReplication(ctx context.Context) error {
	if err := tm.lock(ctx); err != nil {
		return err
	}
	defer tm.unlock()

	tm.setReplicationStopped(true)
	return tm.MysqlDaemon.ResetReplication(ctx)
}

// InitMaster enables writes and returns the replication position.
func (tm *TabletManager) InitMaster(ctx context.Context) (string, error) {
	if err := tm.lock(ctx); err != nil {
		return "", err
	}
	defer tm.unlock()

	// Initializing as master implies undoing any previous "do not replicate".
	tm.setReplicationStopped(false)

	// we need to insert something in the binlogs, so we can get the
	// current position. Let's just use the mysqlctl.CreateReparentJournal commands.
	cmds := mysqlctl.CreateReparentJournal()
	if err := tm.MysqlDaemon.ExecuteSuperQueryList(ctx, cmds); err != nil {
		return "", err
	}

	// get the current replication position
	pos, err := tm.MysqlDaemon.MasterPosition()
	if err != nil {
		return "", err
	}

	// If using semi-sync, we need to enable it before going read-write.
	if err := tm.fixSemiSync(topodatapb.TabletType_MASTER); err != nil {
		return "", err
	}

	// Set the server read-write, from now on we can accept real
	// client writes. Note that if semi-sync replication is enabled,
	// we'll still need some replicas to be able to commit transactions.
	if err := tm.MysqlDaemon.SetReadOnly(false); err != nil {
		return "", err
	}

	if err := tm.changeTypeLocked(ctx, topodatapb.TabletType_MASTER); err != nil {
		return "", err
	}
	return mysql.EncodePosition(pos), nil
}

// PopulateReparentJournal adds an entry into the reparent_journal table.
func (tm *TabletManager) PopulateReparentJournal(ctx context.Context, timeCreatedNS int64, actionName string, masterAlias *topodatapb.TabletAlias, position string) error {
	pos, err := mysql.DecodePosition(position)
	if err != nil {
		return err
	}
	cmds := mysqlctl.CreateReparentJournal()
	cmds = append(cmds, mysqlctl.PopulateReparentJournal(timeCreatedNS, actionName, topoproto.TabletAliasString(masterAlias), pos))

	return tm.MysqlDaemon.ExecuteSuperQueryList(ctx, cmds)
}

// InitReplica sets replication master and position, and waits for the
// reparent_journal table entry up to context timeout
func (tm *TabletManager) InitReplica(ctx context.Context, parent *topodatapb.TabletAlias, position string, timeCreatedNS int64) error {
	if err := tm.lock(ctx); err != nil {
		return err
	}
	defer tm.unlock()

	// If we were a master type, switch our type to replica.  This
	// is used on the old master when using InitShardMaster with
	// -force, and the new master is different from the old master.
	if tm.Tablet().Type == topodatapb.TabletType_MASTER {
		if err := tm.changeTypeLocked(ctx, topodatapb.TabletType_REPLICA); err != nil {
			return err
		}
	}

	pos, err := mysql.DecodePosition(position)
	if err != nil {
		return err
	}
	ti, err := tm.TopoServer.GetTablet(ctx, parent)
	if err != nil {
		return err
	}

	tm.setReplicationStopped(false)

	// If using semi-sync, we need to enable it before connecting to master.
	// If we were a master type, we need to switch back to replica settings.
	// Otherwise we won't be able to commit anything.
	tt := tm.Tablet().Type
	if tt == topodatapb.TabletType_MASTER {
		tt = topodatapb.TabletType_REPLICA
	}
	if err := tm.fixSemiSync(tt); err != nil {
		return err
	}

	if err := tm.MysqlDaemon.SetReplicationPosition(ctx, pos); err != nil {
		return err
	}
	if err := tm.MysqlDaemon.SetMaster(ctx, ti.Tablet.MysqlHostname, int(ti.Tablet.MysqlPort), false /* stopReplicationBefore */, true /* stopReplicationAfter */); err != nil {
		return err
	}

	// wait until we get the replicated row, or our context times out
	return tm.MysqlDaemon.WaitForReparentJournal(ctx, timeCreatedNS)
}

// DemoteMaster prepares a MASTER tablet to give up mastership to another tablet.
//
// It attemps to idempotently ensure the following guarantees upon returning
// successfully:
//   * No future writes will be accepted.
//   * No writes are in-flight.
//   * MySQL is in read-only mode.
//   * Semi-sync settings are consistent with a REPLICA tablet.
//
// If necessary, it waits for all in-flight writes to complete or time out.
//
// It should be safe to call this on a MASTER tablet that was already demoted,
// or on a tablet that already transitioned to REPLICA.
//
// If a step fails in the middle, it will try to undo any changes it made.
func (tm *TabletManager) DemoteMaster(ctx context.Context) (string, error) {
	// The public version always reverts on partial failure.
	return tm.demoteMaster(ctx, true /* revertPartialFailure */)
}

// demoteMaster implements DemoteMaster with an additional, private option.
//
// If revertPartialFailure is true, and a step fails in the middle, it will try
// to undo any changes it made.
func (tm *TabletManager) demoteMaster(ctx context.Context, revertPartialFailure bool) (replicationPosition string, finalErr error) {
	if err := tm.lock(ctx); err != nil {
		return "", err
	}
	defer tm.unlock()

	tablet := tm.Tablet()
	wasMaster := tablet.Type == topodatapb.TabletType_MASTER
	wasServing := tm.QueryServiceControl.IsServing()
	wasReadOnly, err := tm.MysqlDaemon.IsReadOnly()
	if err != nil {
		return "", err
	}

	// If we are a master tablet and not yet read-only, stop accepting new
	// queries and wait for in-flight queries to complete. If we are not master,
	// or if we are already read-only, there's no need to stop the queryservice
	// in order to ensure the guarantee we are being asked to provide, which is
	// that no writes are occurring.
	if wasMaster && !wasReadOnly {
		// Tell Orchestrator we're stopped on purpose for demotion.
		// This is a best effort task, so run it in a goroutine.
		go func() {
			if tm.orc == nil {
				return
			}
			if err := tm.orc.BeginMaintenance(tm.Tablet(), "vttablet has been told to DemoteMaster"); err != nil {
				log.Warningf("Orchestrator BeginMaintenance failed: %v", err)
			}
		}()

		// Note that this may block until the transaction timeout if clients
		// don't finish their transactions in time. Even if some transactions
		// have to be killed at the end of their timeout, this will be
		// considered successful. If we are already not serving, this will be
		// idempotent.
		log.Infof("DemoteMaster disabling query service")
		if _ /* state changed */, err := tm.QueryServiceControl.SetServingType(tablet.Type, false, nil); err != nil {
			return "", vterrors.Wrap(err, "SetServingType(serving=false) failed")
		}
		defer func() {
			if finalErr != nil && revertPartialFailure && wasServing {
				if _ /* state changed */, err := tm.QueryServiceControl.SetServingType(tablet.Type, true, nil); err != nil {
					log.Warningf("SetServingType(serving=true) failed during revert: %v", err)
				}
			}
		}()
	}

	// Now that we know no writes are in-flight and no new writes can occur,
	// set MySQL to read-only mode. If we are already read-only because of a
	// previous demotion, or because we are not master anyway, this should be
	// idempotent.
	if *setSuperReadOnly {
		// Setting super_read_only also sets read_only
		if err := tm.MysqlDaemon.SetSuperReadOnly(true); err != nil {
			return "", err
		}
	} else {
		if err := tm.MysqlDaemon.SetReadOnly(true); err != nil {
			return "", err
		}
	}
	defer func() {
		if finalErr != nil && revertPartialFailure && !wasReadOnly {
			// setting read_only OFF will also set super_read_only OFF if it was set
			if err := tm.MysqlDaemon.SetReadOnly(false); err != nil {
				log.Warningf("SetReadOnly(false) failed during revert: %v", err)
			}
		}
	}()

	// If using semi-sync, we need to disable master-side.
	if err := tm.fixSemiSync(topodatapb.TabletType_REPLICA); err != nil {
		return "", err
	}
	defer func() {
		if finalErr != nil && revertPartialFailure && wasMaster {
			// enable master-side semi-sync again
			if err := tm.fixSemiSync(topodatapb.TabletType_MASTER); err != nil {
				log.Warningf("fixSemiSync(MASTER) failed during revert: %v", err)
			}
		}
	}()

	// Return the current replication position.
	pos, err := tm.MysqlDaemon.MasterPosition()
	if err != nil {
		return "", err
	}
	return mysql.EncodePosition(pos), nil
}

// UndoDemoteMaster reverts a previous call to DemoteMaster
// it sets read-only to false, fixes semi-sync
// and returns its master position.
func (tm *TabletManager) UndoDemoteMaster(ctx context.Context) error {
	if err := tm.lock(ctx); err != nil {
		return err
	}
	defer tm.unlock()

	// If using semi-sync, we need to enable master-side.
	if err := tm.fixSemiSync(topodatapb.TabletType_MASTER); err != nil {
		return err
	}

	// Now, set the server read-only false.
	if err := tm.MysqlDaemon.SetReadOnly(false); err != nil {
		return err
	}

	// Update serving graph
	tablet := tm.Tablet()
	log.Infof("UndoDemoteMaster re-enabling query service")
	if _ /* state changed */, err := tm.QueryServiceControl.SetServingType(tablet.Type, true, nil); err != nil {
		return vterrors.Wrap(err, "SetServingType(serving=true) failed")
	}

	return nil
}

// ReplicaWasPromoted promotes a replica to master, no questions asked.
func (tm *TabletManager) ReplicaWasPromoted(ctx context.Context) error {
	return tm.ChangeType(ctx, topodatapb.TabletType_MASTER)
}

// SetMaster sets replication master, and waits for the
// reparent_journal table entry up to context timeout
func (tm *TabletManager) SetMaster(ctx context.Context, parentAlias *topodatapb.TabletAlias, timeCreatedNS int64, waitPosition string, forceStartReplication bool) error {
	if err := tm.lock(ctx); err != nil {
		return err
	}
	defer tm.unlock()

	return tm.setMasterLocked(ctx, parentAlias, timeCreatedNS, waitPosition, forceStartReplication)
}

func (tm *TabletManager) setMasterRepairReplication(ctx context.Context, parentAlias *topodatapb.TabletAlias, timeCreatedNS int64, waitPosition string, forceStartReplication bool) (err error) {
	parent, err := tm.TopoServer.GetTablet(ctx, parentAlias)
	if err != nil {
		return err
	}

	ctx, unlock, lockErr := tm.TopoServer.LockShard(ctx, parent.Tablet.GetKeyspace(), parent.Tablet.GetShard(), fmt.Sprintf("repairReplication to %v as parent)", topoproto.TabletAliasString(parentAlias)))
	if lockErr != nil {
		return lockErr
	}

	defer unlock(&err)

	return tm.setMasterLocked(ctx, parentAlias, timeCreatedNS, waitPosition, forceStartReplication)
}

func (tm *TabletManager) setMasterLocked(ctx context.Context, parentAlias *topodatapb.TabletAlias, timeCreatedNS int64, waitPosition string, forceStartReplication bool) (err error) {
	// End orchestrator maintenance at the end of fixing replication.
	// This is a best effort operation, so it should happen in a goroutine
	defer func() {
		go func() {
			if tm.orc == nil {
				return
			}
			if err := tm.orc.EndMaintenance(tm.Tablet()); err != nil {
				log.Warningf("Orchestrator EndMaintenance failed: %v", err)
			}
		}()
	}()

	// Change our type to REPLICA if we used to be MASTER.
	// Being sent SetMaster means another MASTER has been successfully promoted,
	// so we convert to REPLICA first, since we want to do it even if other
	// steps fail below.
	// Note it is important to check for MASTER here so that we don't
	// unintentionally change the type of RDONLY tablets
	tablet := tm.Tablet()
	if tablet.Type == topodatapb.TabletType_MASTER {
		tablet.Type = topodatapb.TabletType_REPLICA
		tablet.MasterTermStartTime = nil
		tm.updateState(ctx, tablet, "setMasterLocked")
	}

	// See if we were replicating at all, and should be replicating.
	wasReplicating := false
	shouldbeReplicating := false
	status, err := tm.MysqlDaemon.ReplicationStatus()
	if err == mysql.ErrNotReplica {
		// This is a special error that means we actually succeeded in reading
		// the status, but the status is empty because replication is not
		// configured. We assume this means we used to be a master, so we always
		// try to start replicating once we are told who the new master is.
		shouldbeReplicating = true
		// Since we continue in the case of this error, make sure 'status' is
		// in a known, empty state.
		status = mysql.ReplicationStatus{}
	} else if err != nil {
		// Abort on any other non-nil error.
		return err
	}
	if status.IOThreadRunning || status.SQLThreadRunning {
		wasReplicating = true
		shouldbeReplicating = true
	}
	if forceStartReplication {
		shouldbeReplicating = true
	}

	// If using semi-sync, we need to enable it before connecting to master.
	// If we are currently MASTER, assume we are about to become REPLICA.
	tabletType := tm.Tablet().Type
	if tabletType == topodatapb.TabletType_MASTER {
		tabletType = topodatapb.TabletType_REPLICA
	}
	if err := tm.fixSemiSync(tabletType); err != nil {
		return err
	}
	// Update the master address only if needed.
	// We don't want to interrupt replication for no reason.
	parent, err := tm.TopoServer.GetTablet(ctx, parentAlias)
	if err != nil {
		return err
	}
	masterHost := parent.Tablet.MysqlHostname
	masterPort := int(parent.Tablet.MysqlPort)
	if status.MasterHost != masterHost || status.MasterPort != masterPort {
		// This handles both changing the address and starting replication.
		if err := tm.MysqlDaemon.SetMaster(ctx, masterHost, masterPort, wasReplicating, shouldbeReplicating); err != nil {
			if err := tm.handleRelayLogError(err); err != nil {
				return err
			}
		}
	} else if shouldbeReplicating {
		// The address is correct. Just start replication if needed.
		if !status.ReplicationRunning() {
			if err := tm.MysqlDaemon.StartReplication(tm.hookExtraEnv()); err != nil {
				if err := tm.handleRelayLogError(err); err != nil {
					return err
				}
			}
		}
	}

	// If needed, wait until we replicate to the specified point, or our context
	// times out. Callers can specify the point to wait for as either a
	// GTID-based replication position or a Vitess reparent journal entry,
	// or both.
	if shouldbeReplicating {
		if waitPosition != "" {
			pos, err := mysql.DecodePosition(waitPosition)
			if err != nil {
				return err
			}
			if err := tm.MysqlDaemon.WaitMasterPos(ctx, pos); err != nil {
				return err
			}
		}
		if timeCreatedNS != 0 {
			if err := tm.MysqlDaemon.WaitForReparentJournal(ctx, timeCreatedNS); err != nil {
				return err
			}
		}
	}

	return nil
}

// ReplicaWasRestarted updates the parent record for a tablet.
func (tm *TabletManager) ReplicaWasRestarted(ctx context.Context, parent *topodatapb.TabletAlias) error {
	if err := tm.lock(ctx); err != nil {
		return err
	}
	defer tm.unlock()

	// Only change type of former MASTER tablets.
	// Don't change type of RDONLY
	tablet := tm.Tablet()
	if tablet.Type != topodatapb.TabletType_MASTER {
		return nil
	}
	tablet.Type = topodatapb.TabletType_MASTER
	tablet.MasterTermStartTime = nil
	tm.updateState(ctx, tablet, "ReplicaWasRestarted")
	tm.runHealthCheckLocked()
	return nil
}

// StopReplicationAndGetStatus stops MySQL replication, and returns the
// current status.
func (tm *TabletManager) StopReplicationAndGetStatus(ctx context.Context) (*replicationdatapb.Status, error) {
	if err := tm.lock(ctx); err != nil {
		return nil, err
	}
	defer tm.unlock()

	// get the status before we stop replication
	rs, err := tm.MysqlDaemon.ReplicationStatus()
	if err != nil {
		return nil, vterrors.Wrap(err, "before status failed")
	}
	if !rs.IOThreadRunning && !rs.SQLThreadRunning {
		// no replication is running, just return what we got
		return mysql.ReplicationStatusToProto(rs), nil
	}
	if err := tm.stopReplicationLocked(ctx); err != nil {
		return nil, vterrors.Wrap(err, "failed to stop replication")
	}
	// now patch in the current position
	rs.Position, err = tm.MysqlDaemon.MasterPosition()
	if err != nil {
		return nil, vterrors.Wrap(err, "after position failed")
	}
	return mysql.ReplicationStatusToProto(rs), nil
}

// PromoteReplica makes the current tablet the master
func (tm *TabletManager) PromoteReplica(ctx context.Context) (string, error) {
	if err := tm.lock(ctx); err != nil {
		return "", err
	}
	defer tm.unlock()

	pos, err := tm.MysqlDaemon.Promote(tm.hookExtraEnv())
	if err != nil {
		return "", err
	}

	// If using semi-sync, we need to enable it before going read-write.
	if err := tm.fixSemiSync(topodatapb.TabletType_MASTER); err != nil {
		return "", err
	}

	if err := tm.changeTypeLocked(ctx, topodatapb.TabletType_MASTER); err != nil {
		return "", err
	}

	// We call SetReadOnly only after the topo has been updated to avoid
	// situations where two tablets are master at the DB level but not at the vitess level
	if err := tm.MysqlDaemon.SetReadOnly(false); err != nil {
		return "", err
	}

	return mysql.EncodePosition(pos), nil
}

func isMasterEligible(tabletType topodatapb.TabletType) bool {
	switch tabletType {
	case topodatapb.TabletType_MASTER, topodatapb.TabletType_REPLICA:
		return true
	}

	return false
}

func (tm *TabletManager) fixSemiSync(tabletType topodatapb.TabletType) error {
	if !*enableSemiSync {
		// Semi-sync handling is not enabled.
		return nil
	}

	// Only enable if we're eligible for becoming master (REPLICA type).
	// Ineligible tablets (RDONLY) shouldn't ACK because we'll never promote them.
	if !isMasterEligible(tabletType) {
		return tm.MysqlDaemon.SetSemiSyncEnabled(false, false)
	}

	// Always enable replica-side since it doesn't hurt to keep it on for a master.
	// The master-side needs to be off for a replica, or else it will get stuck.
	return tm.MysqlDaemon.SetSemiSyncEnabled(tabletType == topodatapb.TabletType_MASTER, true)
}

func (tm *TabletManager) fixSemiSyncAndReplication(tabletType topodatapb.TabletType) error {
	if !*enableSemiSync {
		// Semi-sync handling is not enabled.
		return nil
	}

	if tabletType == topodatapb.TabletType_MASTER {
		// Master is special. It is always handled at the
		// right time by the reparent operations, it doesn't
		// need to be fixed.
		return nil
	}

	if err := tm.fixSemiSync(tabletType); err != nil {
		return vterrors.Wrapf(err, "failed to fixSemiSync(%v)", tabletType)
	}

	// If replication is running, but the status is wrong,
	// we should restart replication. First, let's make sure
	// replication is running.
	status, err := tm.MysqlDaemon.ReplicationStatus()
	if err != nil {
		// Replication is not configured, nothing to do.
		return nil
	}
	if !status.IOThreadRunning {
		// IO thread is not running, nothing to do.
		return nil
	}

	shouldAck := isMasterEligible(tabletType)
	acking, err := tm.MysqlDaemon.SemiSyncReplicationStatus()
	if err != nil {
		return vterrors.Wrap(err, "failed to get SemiSyncReplicationStatus")
	}
	if shouldAck == acking {
		return nil
	}

	// We need to restart replication
	log.Infof("Restarting replication for semi-sync flag change to take effect from %v to %v", acking, shouldAck)
	if err := tm.MysqlDaemon.StopReplication(tm.hookExtraEnv()); err != nil {
		return vterrors.Wrap(err, "failed to StopReplication")
	}
	if err := tm.MysqlDaemon.StartReplication(tm.hookExtraEnv()); err != nil {
		return vterrors.Wrap(err, "failed to StartReplication")
	}
	return nil
}

func (tm *TabletManager) handleRelayLogError(err error) error {
	// attempt to fix this error:
	// Slave failed to initialize relay log info structure from the repository (errno 1872) (sqlstate HY000) during query: START SLAVE
	// see https://bugs.mysql.com/bug.php?id=83713 or https://github.com/vitessio/vitess/issues/5067
	if strings.Contains(err.Error(), "Slave failed to initialize relay log info structure from the repository") {
		// Stop, reset and start replication again to resolve this error
		if err := tm.MysqlDaemon.RestartReplication(tm.hookExtraEnv()); err != nil {
			return err
		}
		return nil
	}
	return err
}

// Deprecated delete after 7.0
func (tm *TabletManager) SlaveStatus(ctx context.Context) (*replicationdatapb.Status, error) {
	return tm.ReplicationStatus(ctx)
}

// Deprecated delete after 7.0
func (tm *TabletManager) StopSlave(ctx context.Context) error {
	return tm.StopReplication(ctx)
}

// Deprecated delete after 7.0
func (tm *TabletManager) StopSlaveMinimum(ctx context.Context, position string, waitTime time.Duration) (string, error) {
	return tm.StopReplicationMinimum(ctx, position, waitTime)
}

// Deprecated delete after 7.0
func (tm *TabletManager) StartSlave(ctx context.Context) error {
	return tm.StartReplication(ctx)
}

// Deprecated delete after 7.0
func (tm *TabletManager) StartSlaveUntilAfter(ctx context.Context, position string, waitTime time.Duration) error {
	return tm.StartReplicationUntilAfter(ctx, position, waitTime)
}

// Deprecated delete after 7.0
func (tm *TabletManager) GetSlaves(ctx context.Context) ([]string, error) {
	return tm.GetReplicas(ctx)
}

// Deprecated delete after 7.0
func (tm *TabletManager) InitSlave(ctx context.Context, parent *topodatapb.TabletAlias, position string, timeCreatedNS int64) error {
	return tm.InitReplica(ctx, parent, position, timeCreatedNS)
}

// Deprecated delete after 7.0
func (tm *TabletManager) SlaveWasPromoted(ctx context.Context) error {
	return tm.ReplicaWasPromoted(ctx)
}

// Deprecated delete after 7.0
func (tm *TabletManager) SlaveWasRestarted(ctx context.Context, parent *topodatapb.TabletAlias) error {
	return tm.ReplicaWasRestarted(ctx, parent)
}
