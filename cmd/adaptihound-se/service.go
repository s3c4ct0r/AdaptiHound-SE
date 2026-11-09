package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"adaptihound/se/internal/merge"
	adaptix "github.com/Adaptix-Framework/axc2"
)

const publicationChunkSize = 1024 * 1024
const maximumManifestBytes = 1024 * 1024

// A service reload may overlap an already running Call. Serialize instances
// from this plugin as well as requests within one instance.
var serviceWorkMu sync.Mutex

// No poller, event hooks or agent tasks are created. Adaptix dispatches Call on
// a request goroutine; one merge at a time keeps server resource use bounded.
type ArchiveService struct {
	ts        Teamserver
	outputDir string
	activeMu  sync.Mutex
	active    map[string]bool
}

type mergeRequest struct {
	ManifestID string `json:"manifest_id"`
	DownloadID string `json:"download_id"`
}

type publication struct {
	ManifestID     string            `json:"manifest_id"`
	SHA256         string            `json:"sha256"`
	OutputSHA256   string            `json:"output_sha256"`
	AgentID        string            `json:"agent_id"`
	DownloadID     string            `json:"download_id"`
	Filename       string            `json:"filename"`
	Parts          uint64            `json:"parts"`
	Entries        uint64            `json:"entries"`
	Records        uint64            `json:"records"`
	Bytes          int64             `json:"bytes"`
	Datasets       map[string]uint64 `json:"datasets"`
	Complete       bool              `json:"complete"`
	OutputPath     string            `json:"output_path,omitempty"`
	SourceParts    []sourcePart      `json:"source_parts,omitempty"`
	SourcesCleaned bool              `json:"sources_cleaned,omitempty"`
}

type serviceResponse struct {
	Phase   string `json:"phase"`
	Message string `json:"message"`
	Reused  bool   `json:"reused,omitempty"`
	*publication
}

func newService(ts Teamserver, outputDir string) (*ArchiveService, error) {
	if ts == nil || outputDir == "" {
		return nil, errors.New("server and private working directory are required")
	}
	if err := os.MkdirAll(outputDir, 0700); err != nil {
		return nil, fmt.Errorf("create working directory: %w", err)
	}
	if err := os.Chmod(outputDir, 0700); err != nil {
		return nil, fmt.Errorf("protect working directory: %w", err)
	}
	return &ArchiveService{ts: ts, outputDir: outputDir, active: make(map[string]bool)}, nil
}

func (s *ArchiveService) send(operator, phase, message string, result *publication, reused bool) {
	if result != nil {
		public := *result
		public.OutputPath, public.SourceParts = "", nil
		result = &public
	}
	data, err := json.Marshal(serviceResponse{Phase: phase, Message: message, Reused: reused, publication: result})
	if err == nil {
		s.ts.TsServiceSendDataClient(operator, serviceName, string(data))
	}
}

func (s *ArchiveService) Call(operator, function, args string) {
	if function != "merge" {
		s.send(operator, "error", "Unknown archive command; use merge.", nil, false)
		return
	}
	var request mergeRequest
	if len(args) > 4096 {
		s.send(operator, "error", "Archive request is too large.", nil, false)
		return
	}
	decoder := json.NewDecoder(strings.NewReader(args))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil ||
		(request.ManifestID == "") == (request.DownloadID == "") ||
		(request.ManifestID != "" && !validDownloadID(request.ManifestID)) ||
		(request.DownloadID != "" && !validDownloadID(request.DownloadID)) {
		s.send(operator, "error", "Specify one valid download_id or manifest_id from Downloads.", nil, false)
		return
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		s.send(operator, "error", "Archive request contains trailing data.", nil, false)
		return
	}
	manifestID := request.ManifestID
	var selection *manifestSelection
	if request.DownloadID != "" {
		resolved, err := s.resolveManifest(request.DownloadID)
		if err != nil {
			s.send(operator, "error", err.Error(), nil, false)
			return
		}
		manifestID, selection = resolved.id, &resolved
	}

	s.activeMu.Lock()
	if s.active[manifestID] {
		s.activeMu.Unlock()
		s.send(operator, "busy", "This manifest is already queued or being merged.", nil, false)
		return
	}
	s.active[manifestID] = true
	s.activeMu.Unlock()
	defer func() { s.activeMu.Lock(); delete(s.active, manifestID); s.activeMu.Unlock() }()
	s.send(operator, "queued", "Archive merge queued on the C2 server.", nil, false)
	serviceWorkMu.Lock()
	defer serviceWorkMu.Unlock()
	result, reused, warning, err := s.merge(manifestID, selection)
	if err != nil {
		s.send(operator, "error", err.Error(), nil, false)
		return
	}
	phase, message := "complete", "Merged archive is available in Downloads."
	if reused {
		message = "This manifest was already merged; its existing archive is available in Downloads."
	}
	if warning != "" {
		phase, message = "warning", message+" "+warning
	} else if result.SourcesCleaned {
		message += " Original parts and their manifest were removed from Downloads and server storage."
	}
	s.send(operator, phase, message, &result, reused)
}

func validDownloadID(id string) bool {
	if len(id) == 0 || len(id) > 128 {
		return false
	}
	for _, c := range id {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_') {
			return false
		}
	}
	return true
}

func downloadBasename(name string) string {
	return filepath.Base(strings.ReplaceAll(name, `\`, "/"))
}

func (s *ArchiveService) finishedPath(download adaptix.DownloadData) (string, error) {
	if download.State != adaptix.DOWNLOAD_STATE_FINISHED || download.TotalSize < 0 || download.RecvSize != download.TotalSize {
		return "", fmt.Errorf("download %s is not completely received", download.FileId)
	}
	path, err := s.ts.TsDownloadGetFilepath(download.FileId)
	if err != nil {
		return "", fmt.Errorf("resolve download %s: %w", download.FileId, err)
	}
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() {
		return "", fmt.Errorf("download %s has no readable regular file", download.FileId)
	}
	if info.Size() != download.TotalSize {
		return "", fmt.Errorf("download %s size differs from its finished record", download.FileId)
	}
	return path, nil
}

func manifestDigest(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	count, err := io.Copy(hash, io.LimitReader(file, maximumManifestBytes+1))
	if err != nil {
		return "", err
	}
	if count > maximumManifestBytes {
		return "", errors.New("manifest exceeds the 1 MiB limit")
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func validSHA256(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && hex.EncodeToString(decoded) == value
}

// Hash archive bytes as a stream: neither a large merged ZIP nor the server's
// published copy is retained in memory for content verification.
func outputDigest(path string, expectedSize int64) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || expectedSize < 0 || info.Size() != expectedSize {
		return "", errors.New("archive size differs during content verification")
	}
	hash := sha256.New()
	count, err := io.CopyBuffer(hash, file, make([]byte, 64*1024))
	if err != nil {
		return "", err
	}
	if count != expectedSize {
		return "", errors.New("archive size changed during content verification")
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func (s *ArchiveService) merge(manifestID string, selection *manifestSelection) (publication, bool, string, error) {
	var empty publication
	download, err := s.ts.TsDownloadGet(manifestID)
	if err != nil {
		return empty, false, "", fmt.Errorf("manifest not found in Downloads: %w", err)
	}
	name := downloadBasename(download.RemotePath)
	if selection != nil && (download.FileId != selection.id || download.AgentId != selection.agentID || name != selection.name) {
		return empty, false, "", errors.New("selected collection changed while queued; retry the original part")
	}
	if !strings.HasSuffix(name, ".manifest.json") || len(name) == len(".manifest.json") || download.AgentId == "" {
		return empty, false, "", errors.New("select an AdaptiHound .manifest.json download")
	}
	if download.TotalSize > maximumManifestBytes {
		return empty, false, "", errors.New("manifest exceeds the 1 MiB limit")
	}
	manifestPath, err := s.finishedPath(download)
	if err != nil {
		return empty, false, "", err
	}
	manifestPath, err = filepath.Abs(manifestPath)
	if err != nil {
		return empty, false, "", err
	}
	digest, err := manifestDigest(manifestPath)
	if err != nil {
		return empty, false, "", fmt.Errorf("read manifest: %w", err)
	}
	if selection != nil && digest != selection.sha256 {
		return empty, false, "", errors.New("selected collection manifest changed while queued; retry the original part")
	}
	manifestSource := sourcePart{ID: manifestID, Name: name, Path: manifestPath, Size: download.TotalSize, SHA256: digest}
	manifest, err := merge.ReadManifest(manifestPath)
	if err != nil {
		return empty, false, "", fmt.Errorf("invalid manifest: %w", err)
	}
	run := strings.TrimSuffix(name, ".manifest.json")
	if manifest.PartNamePrefix != run+"-part" {
		return empty, false, "", errors.New("manifest part prefix does not match its downloaded filename")
	}
	key := manifestID + ":" + digest
	// Read authoritative state after taking the shared work lock. An instance
	// cache can be stale if the service was reloaded while a Call was active.
	stored, err := s.ts.TsExtenderDataLoad(serviceName, key)
	if err != nil {
		return empty, false, "", fmt.Errorf("read previous merge state: %w", err)
	}
	var previous publication
	exists := len(stored) != 0
	if exists {
		if err := json.Unmarshal(stored, &previous); err != nil || previous.ManifestID != manifestID || previous.SHA256 != digest || previous.AgentID != download.AgentId {
			return empty, false, "", errors.New("stored merge state is invalid")
		}
	}
	if exists {
		if !validSHA256(previous.OutputSHA256) {
			return empty, false, "", errors.New("stored merge output checksum is invalid")
		}
		output, getErr := s.ts.TsDownloadGet(previous.DownloadID)
		if getErr == nil {
			if output.AgentId != download.AgentId || downloadBasename(output.RemotePath) != previous.Filename || output.TotalSize != previous.Bytes {
				return empty, false, "", errors.New("previous merged download no longer matches its saved record")
			}
			outputPath, err := s.verifiedOutput(previous)
			if err != nil {
				return empty, false, "", err
			}
			previous.OutputPath = outputPath
			if previous.SourcesCleaned {
				if err := s.cleanupManifest(previous, manifestSource); err != nil {
					return previous, true, "Manifest cleanup is incomplete: " + err.Error(), nil
				}
				return previous, true, "", nil
			}
			previous, warning, err := s.completePublication(key, previous, manifest, manifestSource)
			return previous, true, warning, err
		}
		// Adaptix maps database read errors to "not found". A failed Get
		// alone cannot authorize publishing a replacement during cleanup retry.
		records, err := s.downloadRecords()
		if err != nil {
			return empty, false, "", fmt.Errorf("cannot confirm previous merged download is absent: %w", err)
		}
		if row, ok := records[manifestID]; !ok || row.AgentId != download.AgentId ||
			row.State != download.State || row.RemotePath != download.RemotePath ||
			row.TotalSize != download.TotalSize || row.RecvSize != download.RecvSize {
			return empty, false, "", errors.New("cannot confirm Downloads state; the retained manifest is missing or changed")
		}
		if _, exists := records[previous.DownloadID]; exists {
			return empty, false, "", fmt.Errorf("previous merged download is temporarily unavailable; retry without rebuilding: %w", getErr)
		}
		if previous.OutputPath != "" {
			if _, err := os.Lstat(previous.OutputPath); !os.IsNotExist(err) {
				return empty, false, "", errors.New("previous merged archive may still exist on disk; no replacement was published")
			}
		} else if previous.Complete {
			return empty, false, "", errors.New("previous merged archive path is unavailable; no replacement was published")
		}
		if previous.SourcesCleaned {
			return empty, false, "", errors.New("previous merged archive was removed after source cleanup; the manifest alone cannot recreate it")
		}
	}
	parts, sources, err := s.resolveParts(download.AgentId, manifest)
	if err != nil {
		return empty, false, "", err
	}
	result, err := merge.Merge(context.Background(), manifestPath, parts, s.outputDir)
	if err != nil {
		return empty, false, "", fmt.Errorf("cannot merge archive parts: %w", err)
	}
	defer os.Remove(result.Path)
	// Prevent a changed manifest from being published under its earlier digest.
	if current, err := manifestDigest(manifestPath); err != nil || current != digest {
		return empty, false, "", errors.New("manifest changed during merge; no archive published")
	}
	outputSHA256, err := outputDigest(result.Path, result.Bytes)
	if err != nil {
		return empty, false, "", fmt.Errorf("verify prepared archive: %w", err)
	}
	token, err := randomToken()
	if err != nil {
		return empty, false, "", err
	}
	receipt := publication{ManifestID: manifestID, SHA256: digest, OutputSHA256: outputSHA256, AgentID: download.AgentId,
		DownloadID: token, Filename: run + ".zip", Parts: result.Parts,
		Entries: result.Entries, Records: result.Records, Bytes: result.Bytes, Datasets: result.Datasets, SourceParts: sources}
	// A write-ahead receipt makes a failed final state save recoverable after a
	// service reload without publishing a second copy of the same archive.
	if err := s.saveReceipt(key, receipt); err != nil {
		return empty, false, "", fmt.Errorf("cannot record publication intent; no archive published: %w", err)
	}
	if err := s.publish(result.Path, &receipt); err != nil {
		return empty, false, "", err
	}
	receipt, warning, err := s.completePublication(key, receipt, manifest, manifestSource)
	return receipt, false, warning, err
}

func (s *ArchiveService) saveReceipt(key string, receipt publication) error {
	data, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	return s.ts.TsExtenderDataSave(serviceName, key, data)
}

func randomToken() (string, error) {
	var bytes [8]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", fmt.Errorf("create unique download identifier: %w", err)
	}
	return hex.EncodeToString(bytes[:]), nil
}

func (s *ArchiveService) resolveParts(agentID string, manifest merge.Manifest) ([]merge.Part, []sourcePart, error) {
	data, err := s.ts.TsDownloadList()
	if err != nil {
		return nil, nil, fmt.Errorf("list Downloads: %w", err)
	}
	var downloads []adaptix.DownloadData
	if err := json.Unmarshal([]byte(data), &downloads); err != nil {
		return nil, nil, fmt.Errorf("read Downloads list: %w", err)
	}
	if manifest.Parts > uint64(len(downloads)) {
		return nil, nil, fmt.Errorf("not all %d archive parts are available in Downloads", manifest.Parts)
	}
	candidates := make(map[string][]adaptix.DownloadData)
	for _, download := range downloads {
		if download.AgentId != agentID {
			continue
		}
		name := downloadBasename(download.RemotePath)
		if !strings.HasPrefix(name, manifest.PartNamePrefix) || !strings.HasSuffix(name, manifest.PartNameSuffix) {
			continue
		}
		digits := strings.TrimSuffix(strings.TrimPrefix(name, manifest.PartNamePrefix), manifest.PartNameSuffix)
		if digits == "" || strings.IndexFunc(digits, func(r rune) bool { return r < '0' || r > '9' }) >= 0 {
			continue
		}
		index, err := strconv.ParseUint(digits, 10, 64)
		if err != nil || index == 0 || index > manifest.Parts {
			return nil, nil, fmt.Errorf("unexpected archive part %s for this manifest", name)
		}
		expected, err := manifest.PartName(index)
		if err != nil || name != expected {
			return nil, nil, fmt.Errorf("non-canonical archive part filename %s", name)
		}
		candidates[name] = append(candidates[name], download)
	}
	parts := make([]merge.Part, 0, int(manifest.Parts))
	sources := make([]sourcePart, 0, int(manifest.Parts))
	for index := uint64(1); index <= manifest.Parts; index++ {
		name, err := manifest.PartName(index)
		if err != nil {
			return nil, nil, err
		}
		choices := candidates[name]
		if len(choices) == 0 {
			return nil, nil, fmt.Errorf("missing archive part %s from the manifest's agent; retry when every part is finished", name)
		}
		if len(choices) != 1 {
			return nil, nil, fmt.Errorf("ambiguous archive part %s: multiple Downloads records exist", name)
		}
		path, err := s.finishedPath(choices[0])
		if err != nil {
			return nil, nil, fmt.Errorf("archive part %s: %w", name, err)
		}
		path, err = filepath.Abs(path)
		if err != nil {
			return nil, nil, err
		}
		source := sourcePart{ID: choices[0].FileId, Index: index, Name: name, Path: path, Size: choices[0].TotalSize}
		source.SHA256, err = sourceDigest(source)
		if err != nil {
			return nil, nil, fmt.Errorf("record archive part %s: %w", name, err)
		}
		parts = append(parts, merge.Part{Index: index, Path: path, Name: name})
		sources = append(sources, source)
	}
	return parts, sources, nil
}

func (s *ArchiveService) publish(path string, result *publication) (err error) {
	if !validSHA256(result.OutputSHA256) {
		return errors.New("prepared archive content checksum is invalid")
	}
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open merged archive: %w", err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() != result.Bytes || result.Bytes < 0 {
		return errors.New("merged archive size is invalid")
	}
	if err := s.ts.TsDownloadAdd(result.AgentID, result.DownloadID, result.Filename, result.Bytes); err != nil {
		return fmt.Errorf("create merged Download: %w", err)
	}
	closed := false
	defer func() {
		if err == nil {
			return
		}
		var cleanupErr error
		if closed {
			// Only this newly published output is removed. A tampered existing
			// output encountered by reuse is left for the operator to inspect.
			cleanupErr = s.ts.TsDownloadDelete([]string{result.DownloadID})
		} else {
			cleanupErr = s.ts.TsDownloadClose(result.DownloadID, adaptix.DOWNLOAD_STATE_CANCELED)
		}
		if cleanupErr != nil {
			err = fmt.Errorf("%w; invalid Download cleanup failed: %v", err, cleanupErr)
		}
	}()
	buffer := make([]byte, publicationChunkSize)
	written := int64(0)
	for {
		count, readErr := file.Read(buffer)
		if count > 0 {
			if int64(count) > result.Bytes-written {
				return errors.New("merged archive grew during publication")
			}
			if err := s.ts.TsDownloadUpdate(result.DownloadID, adaptix.DOWNLOAD_STATE_RUNNING, buffer[:count]); err != nil {
				return fmt.Errorf("write merged Download: %w", err)
			}
			written += int64(count)
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return fmt.Errorf("read merged archive: %w", readErr)
		}
	}
	if written != result.Bytes {
		return errors.New("merged archive size changed during publication")
	}
	if err := s.ts.TsDownloadClose(result.DownloadID, adaptix.DOWNLOAD_STATE_FINISHED); err != nil {
		return fmt.Errorf("finish merged Download: %w", err)
	}
	closed = true
	output, err := s.ts.TsDownloadGet(result.DownloadID)
	if err != nil {
		return fmt.Errorf("merged Download could not be verified after publication: %w", err)
	}
	if output.AgentId != result.AgentID || downloadBasename(output.RemotePath) != result.Filename || output.TotalSize != result.Bytes {
		return errors.New("merged Download metadata differs after publication")
	}
	outputPath, err := s.finishedPath(output)
	if err != nil {
		return fmt.Errorf("merged Download could not be verified after publication: %w", err)
	}
	actual, err := outputDigest(outputPath, result.Bytes)
	if err != nil || actual != result.OutputSHA256 {
		return errors.New("merged Download content checksum differs after publication")
	}
	result.OutputPath, err = filepath.Abs(outputPath)
	if err != nil {
		return fmt.Errorf("resolve published archive path: %w", err)
	}
	return nil
}
