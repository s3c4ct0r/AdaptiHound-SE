package main

import (
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	"adaptihound/se/internal/merge"
)

type manifestSelection struct {
	id, agentID, name, sha256 string
}

func pinManifest(id, agentID, name, path string) (manifestSelection, error) {
	digest, err := manifestDigest(path)
	if err != nil {
		return manifestSelection{}, fmt.Errorf("read selected manifest: %w", err)
	}
	return manifestSelection{id: id, agentID: agentID, name: name, sha256: digest}, nil
}

// Resolve by the selected part's exact run name and agent, never by the newest
// manifest or a loose filename match. Custom collector prefixes work unchanged.
func (s *ArchiveService) resolveManifest(downloadID string) (manifestSelection, error) {
	var empty manifestSelection
	if !validDownloadID(downloadID) {
		return empty, errors.New("select a valid download")
	}
	selected, err := s.ts.TsDownloadGet(downloadID)
	if err != nil {
		return empty, fmt.Errorf("selected download is unavailable: %w", err)
	}
	if selected.FileId != downloadID || selected.AgentId == "" {
		return empty, errors.New("selected download has invalid metadata")
	}
	selectedPath, err := s.finishedPath(selected)
	if err != nil {
		return empty, err
	}
	name := downloadBasename(selected.RemotePath)
	if strings.HasSuffix(name, ".manifest.json") && len(name) > len(".manifest.json") {
		return pinManifest(selected.FileId, selected.AgentId, name, selectedPath)
	}
	suffix := filepath.Ext(name)
	stem := strings.TrimSuffix(name, suffix)
	separator := strings.LastIndex(stem, "-part")
	if (suffix != ".zip" && suffix != ".json") || separator <= 0 {
		return empty, errors.New("select an AdaptiHound ZIP/JSON part or its manifest")
	}
	digits := stem[separator+len("-part"):]
	index, err := strconv.ParseUint(digits, 10, 64)
	if err != nil || index == 0 || len(digits) < 6 ||
		strings.IndexFunc(digits, func(r rune) bool { return r < '0' || r > '9' }) >= 0 {
		return empty, errors.New("selected download has an invalid AdaptiHound part index")
	}
	manifestName := stem[:separator] + ".manifest.json"
	records, err := s.downloadRecords()
	if err != nil {
		return empty, fmt.Errorf("find collection manifest in Downloads: %w", err)
	}
	manifestID := ""
	for _, row := range records {
		if row.AgentId != selected.AgentId || downloadBasename(row.RemotePath) != manifestName {
			continue
		}
		if manifestID != "" {
			return empty, errors.New("multiple manifests match this collection and agent; no archive was merged")
		}
		if !validDownloadID(row.FileId) {
			return empty, errors.New("collection manifest has an invalid download ID")
		}
		manifestID = row.FileId
	}
	if manifestID == "" {
		return empty, errors.New("the collection manifest has not been received from this agent; wait for transfer and retry this part")
	}
	manifestPath, err := s.finishedPath(records[manifestID])
	if err != nil {
		return empty, fmt.Errorf("collection manifest is not ready; wait for transfer and retry this part: %w", err)
	}
	manifest, err := merge.ReadManifest(manifestPath)
	if err != nil {
		return empty, fmt.Errorf("invalid collection manifest: %w", err)
	}
	expected, err := manifest.PartName(index)
	if err != nil || expected != name {
		return empty, errors.New("selected part does not match the collection manifest")
	}
	return pinManifest(manifestID, selected.AgentId, manifestName, manifestPath)
}
