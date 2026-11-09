// Package merge validates and combines independent AdaptiHound output parts.
package merge

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"unicode/utf8"
)

const checksumFormat = "crc32(index_le64 || size_le64 || bytes)"

var datasetNames = [...]string{
	"users", "computers", "groups", "domains", "ous", "gpos", "containers", "rootcas",
	"aiacas", "enterprisecas", "ntauthstores", "certtemplates", "issuancepolicies", "sites", "siteservers", "sitesubnets",
}

// Manifest is the version 1 manifest emitted by ah_archive_parts.
type Manifest struct {
	Format            string            `json:"format"`
	Version           int               `json:"version"`
	Complete          bool              `json:"complete"`
	Status            string            `json:"status"`
	Parts             uint64            `json:"parts"`
	Records           uint64            `json:"records"`
	AcceptedRecords   uint64            `json:"accepted_records"`
	PartNamePrefix    string            `json:"part_name_prefix"`
	PartNameSuffix    string            `json:"part_name_suffix"`
	PartNameDigits    int               `json:"part_name_digits"`
	PartRawTarget     uint64            `json:"part_raw_target"`
	SingleRecordLimit uint64            `json:"single_record_limit"`
	Methods           int64             `json:"methods"`
	PartsCRC32        uint32            `json:"parts_crc32"`
	ChecksumFormat    string            `json:"checksum_format"`
	Failure           *string           `json:"failure"`
	Datasets          map[string]uint64 `json:"datasets"`
}

// PartName returns the original download name. Six digits is a minimum width,
// so collections with more than 999999 parts do not wrap their numbering.
func (m Manifest) PartName(index uint64) (string, error) {
	if index == 0 || index > m.Parts {
		return "", fmt.Errorf("part index %d is outside 1..%d", index, m.Parts)
	}
	if m.PartNameDigits != 6 || len(m.PartNamePrefix) > 1029 || !safeName(m.PartNamePrefix) ||
		(m.PartNameSuffix != ".zip" && m.PartNameSuffix != ".json") {
		return "", errors.New("invalid part naming contract")
	}
	return fmt.Sprintf("%s%0*d%s", m.PartNamePrefix, m.PartNameDigits, index, m.PartNameSuffix), nil
}

// ReadManifest accepts a complete collection only. A manifest is deliberately
// bounded: the collector itself reserves an 8 KiB buffer for this document.
func ReadManifest(path string) (Manifest, error) {
	var m Manifest
	f, err := os.Open(path)
	if err != nil {
		return m, fmt.Errorf("open manifest: %w", err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return m, errors.New("manifest is not a regular file")
	}
	const limit = 64 * 1024
	data, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return m, fmt.Errorf("read manifest: %w", err)
	}
	if len(data) > limit {
		return m, errors.New("manifest exceeds 64 KiB")
	}
	if err := rejectDuplicateFields(data); err != nil {
		return m, fmt.Errorf("invalid manifest: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&m); err != nil {
		return m, fmt.Errorf("decode manifest: %w", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return m, fmt.Errorf("decode manifest fields: %w", err)
	}
	for _, name := range []string{"format", "version", "complete", "status", "parts", "records", "accepted_records",
		"part_name_prefix", "part_name_suffix", "part_name_digits", "part_raw_target",
		"methods", "parts_crc32", "checksum_format", "failure", "datasets"} {
		value, present := fields[name]
		if !present || (name != "failure" && bytes.Equal(bytes.TrimSpace(value), []byte("null"))) {
			return m, fmt.Errorf("manifest field %q is missing or null", name)
		}
	}
	// Earlier version 1 writers rejected any row above the raw part target and
	// did not emit a separate single_record_limit field.
	if value, present := fields["single_record_limit"]; !present {
		m.SingleRecordLimit = m.PartRawTarget
	} else if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
		return m, errors.New("manifest single_record_limit must not be null")
	}
	if m.Format != "AdaptiHoundParts" || m.Version != 1 {
		return m, errors.New("unsupported AdaptiHound manifest format or version")
	}
	if !m.Complete || m.Status != "complete" || m.Failure != nil {
		return m, errors.New("collection manifest is incomplete or reports a failure")
	}
	if m.ChecksumFormat != checksumFormat {
		return m, errors.New("unsupported manifest checksum format")
	}
	if !safeName(m.PartNamePrefix) || len(m.PartNamePrefix) > 1029 || !strings.HasSuffix(m.PartNamePrefix, "-part") || m.PartNameDigits != 6 ||
		(m.PartNameSuffix != ".zip" && m.PartNameSuffix != ".json") {
		return m, errors.New("invalid manifest part naming contract")
	}
	if m.PartRawTarget == 0 || m.SingleRecordLimit < m.PartRawTarget || m.SingleRecordLimit > math.MaxInt64-4096 ||
		m.Methods < 0 || m.Methods > math.MaxInt32 {
		return m, errors.New("invalid manifest resource bounds or methods")
	}
	if m.Parts > math.MaxInt64 || m.Records > math.MaxInt64 || m.AcceptedRecords != m.Records {
		return m, errors.New("manifest accepted/emitted record accounting differs")
	}
	if len(m.Datasets) != len(datasetNames) {
		return m, errors.New("manifest must account for all 16 datasets")
	}
	var datasetFields map[string]json.RawMessage
	if err := json.Unmarshal(fields["datasets"], &datasetFields); err != nil {
		return m, fmt.Errorf("decode manifest dataset counts: %w", err)
	}
	var total uint64
	for _, name := range datasetNames {
		count, found := m.Datasets[name]
		if !found || bytes.Equal(bytes.TrimSpace(datasetFields[name]), []byte("null")) ||
			count > math.MaxInt64 || total > math.MaxInt64-count {
			return m, fmt.Errorf("invalid dataset count for %q", name)
		}
		total += count
	}
	if total != m.Records || (m.Parts == 0 && (m.Records != 0 || m.PartsCRC32 != 0)) ||
		(m.Parts != 0 && m.Records < m.Parts) {
		return m, errors.New("manifest part and dataset totals differ")
	}
	return m, nil
}

func safeName(name string) bool {
	if name == "" || name == "." || name == ".." || !utf8.ValidString(name) || strings.ContainsAny(name, "/\\:") {
		return false
	}
	for _, c := range name {
		if c < 32 || c == 127 {
			return false
		}
	}
	return true
}

// encoding/json otherwise silently accepts duplicate object keys.
func rejectDuplicateFields(data []byte) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	var walk func(int) error
	walk = func(depth int) error {
		if depth > 32 {
			return errors.New("manifest nesting is too deep")
		}
		token, err := d.Token()
		if err != nil {
			return err
		}
		delim, isDelim := token.(json.Delim)
		if !isDelim {
			return nil
		}
		switch delim {
		case '{':
			seen := make(map[string]bool)
			for d.More() {
				key, err := d.Token()
				if err != nil {
					return err
				}
				name, ok := key.(string)
				if !ok || seen[name] {
					return fmt.Errorf("duplicate or invalid JSON key %q", name)
				}
				seen[name] = true
				if err := walk(depth + 1); err != nil {
					return err
				}
			}
		case '[':
			for d.More() {
				if err := walk(depth + 1); err != nil {
					return err
				}
			}
		default:
			return errors.New("unexpected closing JSON delimiter")
		}
		_, err = d.Token()
		return err
	}
	if err := walk(0); err != nil {
		return err
	}
	if _, err := d.Token(); !errors.Is(err, io.EOF) {
		return errors.New("manifest has trailing JSON or invalid trailing bytes")
	}
	return nil
}
