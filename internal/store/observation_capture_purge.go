package store

import (
	"context"
	"errors"
	"strings"
)

// The native purge has deliberately narrow ownership. A captured identity move
// invalidates a simple parent/key closure; retain it as a durable refusal rather
// than guessing lineage from adjacent withdraw/upsert observations.
func (s *Store) planCapturePurge(ctx context.Context, p ThreadPurgePlan) ([]int64, error) {
	present, err := capturePresent(ctx, s.q())
	if err != nil || !present {
		return nil, err
	}
	var moves int
	if err = s.q().QueryRowContext(ctx, `SELECT count(*) FROM capture_commits WHERE reset_reason='identity_change'`).Scan(&moves); err != nil {
		return nil, err
	}
	if moves > 0 {
		return nil, errors.New("capture purge requires proven historical ownership after identity rekey; refusing ambiguous closure")
	}
	var conditions []string
	var args []any
	for _, target := range p.Targets {
		conditions = append(conditions, `(table_name IN ('thread_revisions','pull_request_review_threads','pull_request_review_thread_syncs') AND json_extract(value_json,'$.thread_id[1]')=?)`)
		args = append(args, captureInt(target.ThreadID))
		for _, node := range target.Nodes {
			conditions = append(conditions, `(table_name='actor_identity_evidence' AND key_json=json_array(json_array('text',?)))`)
			args = append(args, node)
		}
	}
	if len(conditions) == 0 {
		return nil, nil
	}
	rows, err := s.q().QueryContext(ctx, `SELECT image_id FROM capture_images WHERE value_json IS NOT NULL AND (`+strings.Join(conditions, " OR ")+`) ORDER BY image_id`, args...)
	if err != nil {
		return nil, err
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	if len(ids) > 0 {
		var parts []string
		for _, id := range ids {
			parts = append(parts, captureInt(id))
		}
		list := strings.Join(parts, ",")
		var historicalBlobs int
		if err = s.q().QueryRowContext(ctx, `SELECT count(*) FROM capture_images WHERE image_id IN (`+list+`) AND table_name='thread_revisions' AND json_extract(value_json,'$.raw_json_blob_id[0]')<>'null'`).Scan(&historicalBlobs); err != nil {
			return nil, err
		}
		if historicalBlobs > 0 {
			return nil, errors.New("capture purge refuses historical blob-backed evidence; separate ownership repair required")
		}
		var shared int
		err = s.q().QueryRowContext(ctx, `SELECT count(*) FROM capture_supports s JOIN capture_observations o ON o.seq=s.seq WHERE s.image_id IN (`+list+`) AND (o.image_id IS NULL OR o.image_id NOT IN (`+list+`))`).Scan(&shared)
		if err != nil {
			return nil, err
		}
		if shared > 0 {
			return nil, errors.New("capture purge refuses supporting images referenced by unrelated observations")
		}
	}
	return ids, nil
}
func (s *Store) prepareCapturePurge(ctx context.Context, p ThreadPurgePlan) error {
	if s.captureTx == nil {
		return nil
	}
	changed := false
	for _, t := range p.Targets {
		if !t.AlreadyExcluded {
			changed = true
		}
	}
	if !changed {
		return nil
	}
	if err := s.rotateCaptureEpoch(ctx, "owner_purge", "owner_requested", false); err != nil {
		return err
	}
	for _, id := range p.CaptureImageIDs {
		if _, err := s.q().ExecContext(ctx, `UPDATE capture_images SET value_json=NULL,redaction_epoch=?,redaction_reason='owner_requested' WHERE image_id=?`, s.captureTx.Epoch, id); err != nil {
			return err
		}
		// Also withdraw historical no-longer-native heads. An active deleted native
		// key gets its explicit withdrawal observation in normal finalization.
		if _, err := s.q().ExecContext(ctx, `UPDATE capture_heads SET present=0,image_id=NULL,clocks_json='{}' WHERE image_id=?`, id); err != nil {
			return err
		}
	}
	return nil
}
