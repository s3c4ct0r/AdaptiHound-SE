package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"adaptihound/se/internal/merge"
	adaptix "github.com/Adaptix-Framework/axc2"
)

// Adaptix turns each deletion call into one SQLite IN query. Keep its number
// of parameters bounded even for collections with thousands of source parts.
const sourceDeleteBatchSize = 256

// Keep the exact inputs in the durable receipt so cleanup can resume without
// rebuilding the archive, even after some source records have been removed.
type sourcePart struct {
	ID     string `json:"file_id"`
	Index  uint64 `json:"index"`
	Name   string `json:"name"`
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

func sourceDigest(part sourcePart) (string, error) {
	info, err := os.Lstat(part.Path)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() || info.Size() != part.Size {
		return "", errors.New("source part is no longer the recorded regular file")
	}
	return outputDigest(part.Path, part.Size)
}

func (s *ArchiveService) verifiedOutput(result publication) (string, error) {
	output, err := s.ts.TsDownloadGet(result.DownloadID)
	if err != nil {
		return "", fmt.Errorf("merged archive is unavailable: %w", err)
	}
	if output.AgentId != result.AgentID || output.FileId != result.DownloadID ||
		downloadBasename(output.RemotePath) != result.Filename || output.TotalSize != result.Bytes {
		return "", errors.New("merged archive metadata changed; source parts were retained")
	}
	path, err := s.finishedPath(output)
	if err != nil {
		return "", err
	}
	path, err = filepath.Abs(path)
	if err != nil {
		return "", err
	}
	if result.OutputPath != "" && path != result.OutputPath {
		return "", errors.New("merged archive path changed; source parts were retained")
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() {
		return "", errors.New("merged archive is not a regular file; source parts were retained")
	}
	actual, err := outputDigest(path, result.Bytes)
	if err != nil || !validSHA256(result.OutputSHA256) || actual != result.OutputSHA256 {
		return "", errors.New("merged archive checksum changed; source parts were retained")
	}
	return path, nil
}

// Save the verified output before authorizing deletion. A failed state write
// leaves all inputs available, and the earlier publication intent still lets a
// repeated request recover the finished archive without creating a duplicate.
func (s *ArchiveService) completePublication(key string, result publication, manifest merge.Manifest, manifestSource sourcePart) (publication, string, error) {
	result.Complete = true
	if err := s.saveReceipt(key, result); err != nil {
		return result, "Final merge state could not be saved. The verified archive is available; source parts were retained.", nil
	}
	if _, err := s.verifiedOutput(result); err != nil {
		return result, "", err
	}
	if err := s.cleanupSources(result, manifest); err != nil {
		return result, "Source cleanup is incomplete: " + err.Error() + ". Retry this manifest to finish cleanup without rebuilding the archive.", nil
	}
	result.SourcesCleaned = true
	if err := s.saveReceipt(key, result); err != nil {
		return result, "Source parts were removed, but their cleanup status could not be saved. The merged archive remains available; repeating this manifest will reconcile the status.", nil
	}
	if err := s.cleanupManifest(result, manifestSource); err != nil {
		return result, "Manifest cleanup is incomplete: " + err.Error() + ". The merged archive remains available.", nil
	}
	return result, "", nil
}

func (s *ArchiveService) downloadRecords() (map[string]adaptix.DownloadData, error) {
	data, err := s.ts.TsDownloadList()
	if err != nil {
		return nil, err
	}
	var rows []adaptix.DownloadData
	if err := json.Unmarshal([]byte(data), &rows); err != nil {
		return nil, err
	}
	records := make(map[string]adaptix.DownloadData, len(rows))
	for _, row := range rows {
		if _, exists := records[row.FileId]; exists {
			return nil, fmt.Errorf("ambiguous Downloads record %s", row.FileId)
		}
		records[row.FileId] = row
	}
	return records, nil
}

func sourceRecordMatches(part sourcePart, row adaptix.DownloadData, agentID string) bool {
	return row.AgentId == agentID && row.FileId == part.ID && downloadBasename(row.RemotePath) == part.Name &&
		row.State == adaptix.DOWNLOAD_STATE_FINISHED && row.TotalSize == part.Size && row.RecvSize == part.Size
}

func (s *ArchiveService) checkSourceRecord(part sourcePart, row adaptix.DownloadData, agentID string) error {
	if !sourceRecordMatches(part, row, agentID) {
		return fmt.Errorf("source part %s metadata changed", part.Name)
	}
	path, err := s.ts.TsDownloadGetFilepath(part.ID)
	if err != nil {
		return err
	}
	path, err = filepath.Abs(path)
	if err != nil || path != part.Path {
		return fmt.Errorf("source part %s path changed", part.Name)
	}
	return nil
}

func checkSourceFile(part sourcePart) error {
	actual, err := sourceDigest(part)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect source part %s: %w", part.Name, err)
	}
	if actual != part.SHA256 {
		return fmt.Errorf("source part %s checksum changed", part.Name)
	}
	return nil
}

func (s *ArchiveService) cleanupSources(result publication, manifest merge.Manifest) error {
	if !result.Complete || result.OutputPath == "" || uint64(len(result.SourceParts)) != result.Parts || result.Parts != manifest.Parts {
		return errors.New("the saved source-part plan is unavailable")
	}
	manifestPath, err := s.ts.TsDownloadGetFilepath(result.ManifestID)
	if err != nil {
		return fmt.Errorf("resolve retained manifest: %w", err)
	}
	manifestPath, err = filepath.Abs(manifestPath)
	if err != nil {
		return err
	}
	records, err := s.downloadRecords()
	if err != nil {
		return fmt.Errorf("check Downloads before cleanup: %w", err)
	}
	if !retainedManifestPresent(records, result) {
		return errors.New("cannot confirm Downloads state; the retained manifest is missing or changed")
	}
	seenIDs, seenPaths := make(map[string]bool), make(map[string]bool)
	var ids []string
	// Validate the entire remaining set before requesting any deletion. Files
	// already removed by an earlier attempt are recognized by both ID and path.
	for index, part := range result.SourceParts {
		name, err := manifest.PartName(uint64(index) + 1)
		if err != nil || part.Index != uint64(index)+1 || part.Name != name || !validDownloadID(part.ID) ||
			part.ID == result.ManifestID || part.ID == result.DownloadID || seenIDs[part.ID] ||
			!filepath.IsAbs(part.Path) || filepath.Clean(part.Path) != part.Path || seenPaths[part.Path] ||
			part.Path == result.OutputPath || part.Path == manifestPath || part.Size <= 0 || !validSHA256(part.SHA256) {
			return errors.New("the saved source-part plan is invalid")
		}
		seenIDs[part.ID], seenPaths[part.Path] = true, true
		if row, exists := records[part.ID]; exists {
			if err := s.checkSourceRecord(part, row, result.AgentID); err != nil {
				return err
			}
			ids = append(ids, part.ID)
		}
		if err := checkSourceFile(part); err != nil {
			return err
		}
	}
	// Source validation can take time for a large collection. Verify the final
	// archive again at the boundary where cleanup becomes destructive.
	if _, err := s.verifiedOutput(result); err != nil {
		return err
	}
	for start := 0; start < len(ids); start += sourceDeleteBatchSize {
		end := min(start+sourceDeleteBatchSize, len(ids))
		// Hooks may alter their argument; never lend them the persisted plan.
		if err := s.ts.TsDownloadDelete(append([]string(nil), ids[start:end]...)); err != nil {
			return fmt.Errorf("remove source Downloads: %w", err)
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		records, err := s.downloadRecords()
		if err != nil {
			return fmt.Errorf("confirm Downloads cleanup: %w", err)
		}
		if !retainedManifestPresent(records, result) {
			return errors.New("cannot confirm Downloads cleanup; the retained manifest is missing or changed")
		}
		pending := 0
		orphanOutputVerified := false
		for _, part := range result.SourceParts {
			if row, exists := records[part.ID]; exists {
				if !sourceRecordMatches(part, row, result.AgentID) {
					return fmt.Errorf("source part %s metadata changed during cleanup", part.Name)
				}
				pending++
				continue
			}
			if _, err := os.Lstat(part.Path); os.IsNotExist(err) {
				continue
			} else if err != nil {
				return fmt.Errorf("confirm source file removal: %w", err)
			}
			// Adaptix can silently remove the DB record while unlinking the
			// file fails. An orphan can only be removed by its pinned path,
			// after rechecking that it is still the original regular file.
			if err := checkSourceFile(part); err != nil {
				return err
			}
			if !orphanOutputVerified {
				if _, err := s.verifiedOutput(result); err != nil {
					return err
				}
				orphanOutputVerified = true
			}
			if err := os.Remove(part.Path); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("remove orphan source part %s: %w", part.Name, err)
			}
		}
		if pending == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%d source Downloads are still present", pending)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// The SDK can return an empty list without an error on a database read failure.
// Require the manifest as a positive control before interpreting missing IDs.
func retainedManifestPresent(records map[string]adaptix.DownloadData, result publication) bool {
	row, ok := records[result.ManifestID]
	return ok && row.AgentId == result.AgentID && row.State == adaptix.DOWNLOAD_STATE_FINISHED &&
		row.TotalSize > 0 && row.RecvSize == row.TotalSize
}

func publishedOutputPresent(records map[string]adaptix.DownloadData, result publication) bool {
	row, ok := records[result.DownloadID]
	return ok && row.FileId == result.DownloadID && row.AgentId == result.AgentID &&
		downloadBasename(row.RemotePath) == result.Filename && row.State == adaptix.DOWNLOAD_STATE_FINISHED &&
		row.TotalSize == result.Bytes && row.RecvSize == result.Bytes
}

// Remove the pinned manifest only after the verified archive and completed
// source cleanup have been saved. The final archive remains the positive
// control while the manifest's own record disappears asynchronously.
func (s *ArchiveService) cleanupManifest(result publication, source sourcePart) error {
	if !result.Complete || !result.SourcesCleaned || result.OutputPath == "" ||
		source.Index != 0 || source.ID != result.ManifestID || !validDownloadID(source.ID) || source.ID == result.DownloadID ||
		downloadBasename(source.Name) != source.Name || !strings.HasSuffix(source.Name, ".manifest.json") || len(source.Name) <= len(".manifest.json") ||
		!filepath.IsAbs(source.Path) || filepath.Clean(source.Path) != source.Path || source.Path == result.OutputPath ||
		source.Size <= 0 || !validSHA256(source.SHA256) || source.SHA256 != result.SHA256 {
		return errors.New("the pinned manifest proof is invalid")
	}
	for _, part := range result.SourceParts {
		if source.ID == part.ID || source.Path == part.Path {
			return errors.New("the pinned manifest overlaps a source part")
		}
	}
	records, err := s.downloadRecords()
	if err != nil {
		return fmt.Errorf("check Downloads before manifest cleanup: %w", err)
	}
	if !publishedOutputPresent(records, result) {
		return errors.New("cannot confirm Downloads state; the merged archive is missing or changed")
	}
	_, exists := records[source.ID]
	if exists {
		if err := s.checkSourceRecord(source, records[source.ID], result.AgentID); err != nil {
			return fmt.Errorf("check manifest metadata: %w", err)
		}
	}
	if err := checkSourceFile(source); err != nil {
		return fmt.Errorf("check manifest file: %w", err)
	}
	if _, err := s.verifiedOutput(result); err != nil {
		return fmt.Errorf("verify merged archive before manifest cleanup: %w", err)
	}
	if exists {
		if err := s.ts.TsDownloadDelete([]string{source.ID}); err != nil {
			return fmt.Errorf("remove manifest Download: %w", err)
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		records, err := s.downloadRecords()
		if err != nil {
			return fmt.Errorf("confirm manifest cleanup: %w", err)
		}
		if !publishedOutputPresent(records, result) {
			return errors.New("cannot confirm manifest cleanup; the merged archive is missing or changed")
		}
		if row, exists := records[source.ID]; exists {
			if !sourceRecordMatches(source, row, result.AgentID) {
				return errors.New("manifest metadata changed during cleanup")
			}
			if time.Now().After(deadline) {
				return errors.New("the manifest Download is still present")
			}
			time.Sleep(25 * time.Millisecond)
			continue
		}
		if _, err := os.Lstat(source.Path); os.IsNotExist(err) {
			return nil
		} else if err != nil {
			return fmt.Errorf("confirm manifest file removal: %w", err)
		}
		// A failed SDK unlink may leave the original file after its record was
		// removed. Only the pinned, rehashed regular file may be unlinked here.
		if err := checkSourceFile(source); err != nil {
			return fmt.Errorf("check orphan manifest: %w", err)
		}
		if _, err := s.verifiedOutput(result); err != nil {
			return fmt.Errorf("verify merged archive before orphan manifest cleanup: %w", err)
		}
		if err := os.Remove(source.Path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove orphan manifest: %w", err)
		}
		return nil
	}
}
