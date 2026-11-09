package merge

import (
	"archive/zip"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"math"
	"os"
	"sort"
	"strings"
)

// Part identifies a finished source download. Name is its original basename;
// Path is the server's registered local download file, never a client path.
type Part struct {
	Index uint64
	Path  string
	Name  string
}

// Result describes a private temporary ZIP. Only the caller publishes it in
// Downloads, after Merge has validated the complete collection successfully.
type Result struct {
	Path                    string
	Parts, Entries, Records uint64
	Bytes                   int64
	Datasets                map[string]uint64
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}

type contextWriter struct {
	ctx context.Context
	w   io.Writer
}

func (w contextWriter) Write(p []byte) (int, error) {
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	return w.w.Write(p)
}

func hashPart(ctx context.Context, h hash.Hash32, f *os.File, index uint64, size int64, buffer []byte) error {
	var frame [16]byte
	binary.LittleEndian.PutUint64(frame[:8], index)
	binary.LittleEndian.PutUint64(frame[8:], uint64(size))
	_, _ = h.Write(frame[:])
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	n, err := io.CopyBuffer(h, contextReader{ctx, f}, buffer)
	if err != nil {
		return err
	}
	if n != size {
		return errors.New("part size changed while reading")
	}
	return nil
}

// Merge preserves each complete JSON entry and every row, including repeated
// object identifiers. It never loads a graph array or recompresses ZIP inputs.
// A failed or cancelled merge removes its temporary output.
func Merge(ctx context.Context, manifestPath string, parts []Part, outputDir string) (result Result, err error) {
	if ctx == nil {
		return result, errors.New("merge context is nil")
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	m, err := ReadManifest(manifestPath)
	if err != nil {
		return result, err
	}
	if uint64(len(parts)) != m.Parts {
		return result, fmt.Errorf("expected %d parts, got %d", m.Parts, len(parts))
	}
	ordered := append([]Part(nil), parts...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Index < ordered[j].Index })
	seenPaths := make(map[string]bool, len(parts))
	for i, part := range ordered {
		index := uint64(i) + 1
		name, err := m.PartName(index)
		if err != nil || part.Index != index || part.Name != name || part.Path == "" || seenPaths[part.Path] {
			return result, fmt.Errorf("missing, duplicate or mismatched part at index %d", index)
		}
		seenPaths[part.Path] = true
	}
	out, err := os.CreateTemp(outputDir, "adaptihound-merged-*.zip")
	if err != nil {
		return result, fmt.Errorf("create temporary archive: %w", err)
	}
	outputPath := out.Name()
	success := false
	defer func() {
		_ = out.Close()
		if !success {
			_ = os.Remove(outputPath)
		}
	}()
	writer := zip.NewWriter(contextWriter{ctx, out})
	rolling := crc32.NewIEEE()
	counts := make(map[string]uint64, len(datasetNames))
	for _, name := range datasetNames {
		counts[name] = 0
	}
	entryNames := make(map[string]bool, len(parts))
	buffer := make([]byte, 64*1024)
	for _, part := range ordered {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		meta, entryName, err := mergePart(ctx, writer, rolling, part, m, buffer)
		if err != nil {
			return result, fmt.Errorf("part %d: %w", part.Index, err)
		}
		if entryNames[entryName] {
			return result, fmt.Errorf("duplicate output JSON entry %q", entryName)
		}
		entryNames[entryName] = true
		if meta.Count > m.Datasets[meta.Type]-counts[meta.Type] {
			return result, fmt.Errorf("dataset %s count exceeds manifest", meta.Type)
		}
		counts[meta.Type] += meta.Count
		result.Records += meta.Count
		result.Entries++
	}
	if rolling.Sum32() != m.PartsCRC32 {
		return result, errors.New("parts rolling CRC32 differs from manifest")
	}
	if result.Records != m.Records {
		return result, errors.New("merged record count differs from manifest")
	}
	for _, name := range datasetNames {
		if counts[name] != m.Datasets[name] {
			return result, fmt.Errorf("dataset %s count differs from manifest", name)
		}
	}
	if err := writer.Close(); err != nil {
		return result, fmt.Errorf("finish merged ZIP: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if err := out.Sync(); err != nil {
		return result, fmt.Errorf("flush merged ZIP: %w", err)
	}
	info, err := out.Stat()
	if err != nil {
		return result, err
	}
	if err := out.Close(); err != nil {
		return result, fmt.Errorf("close merged ZIP: %w", err)
	}
	result.Path, result.Bytes = outputPath, info.Size()
	result.Parts, result.Datasets = m.Parts, counts
	success = true
	return result, nil
}

func mergePart(ctx context.Context, writer *zip.Writer, rolling hash.Hash32, part Part, m Manifest, buffer []byte) (datasetMeta, string, error) {
	var meta datasetMeta
	f, err := os.Open(part.Path)
	if err != nil {
		return meta, "", err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > math.MaxInt32 {
		return meta, "", errors.New("part is not a nonempty regular file")
	}
	if err := hashPart(ctx, rolling, f, part.Index, info.Size(), buffer); err != nil {
		return meta, "", fmt.Errorf("checksum part: %w", err)
	}
	var entryName string
	if m.PartNameSuffix == ".json" {
		if uint64(info.Size()) > m.SingleRecordLimit+4096 {
			return meta, "", errors.New("JSON part exceeds declared resource bounds")
		}
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return meta, "", err
		}
		meta, err = validateJSON(ctx, contextReader{ctx, f}, m)
		if err != nil {
			return meta, "", fmt.Errorf("validate JSON: %w", err)
		}
		entryName = part.Name
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return meta, "", err
		}
		entry, err := writer.CreateHeader(&zip.FileHeader{Name: entryName, Method: zip.Deflate})
		if err != nil {
			return meta, "", err
		}
		if _, err := io.CopyBuffer(entry, contextReader{ctx, f}, buffer); err != nil {
			return meta, "", err
		}
	} else {
		reader, err := zip.NewReader(f, info.Size())
		if err != nil {
			return meta, "", fmt.Errorf("open ZIP: %w", err)
		}
		if len(reader.File) != 1 {
			return meta, "", errors.New("ZIP part must contain exactly one JSON entry")
		}
		entry := reader.File[0]
		if entry.FileInfo().IsDir() || !safeName(entry.Name) || !strings.HasSuffix(entry.Name, ".json") {
			return meta, "", errors.New("ZIP entry is not a safe JSON basename")
		}
		if entry.Flags&1 != 0 {
			return meta, "", errors.New("encrypted ZIP parts are unsupported; collect without --zip-password")
		}
		if entry.Method != zip.Store && entry.Method != zip.Deflate {
			return meta, "", errors.New("unsupported ZIP compression method")
		}
		if entry.UncompressedSize64 > m.SingleRecordLimit+4096 {
			return meta, "", errors.New("ZIP JSON part exceeds declared resource bounds")
		}
		body, err := entry.Open()
		if err != nil {
			return meta, "", err
		}
		meta, err = validateJSON(ctx, contextReader{ctx, body}, m)
		closeErr := body.Close()
		if err != nil {
			return meta, "", fmt.Errorf("validate ZIP JSON: %w", err)
		}
		if closeErr != nil {
			return meta, "", closeErr
		}
		stem := strings.TrimSuffix(m.PartNamePrefix, "-part")
		expected := fmt.Sprintf("%s-%s-part%0*d.json", stem, meta.Type, m.PartNameDigits, part.Index)
		if entry.Name != expected {
			return meta, "", errors.New("ZIP JSON entry name differs from dataset and run")
		}
		entryName = entry.Name
		// Copy preserves the already-validated compressed stream and metadata.
		// The writer retains only entry headers, not the source ZIP contents.
		if err := copyEntry(writer, entry); err != nil {
			return meta, "", fmt.Errorf("copy ZIP entry: %w", err)
		}
	}
	after, err := f.Stat()
	if err != nil || after.Size() != info.Size() || !after.ModTime().Equal(info.ModTime()) {
		return meta, "", errors.New("source part changed while merging")
	}
	return meta, entryName, nil
}

func copyEntry(writer *zip.Writer, entry *zip.File) error {
	// Source parts use ZIP64 even for small entries. Their ZIP64 extra fields
	// describe source sizes/offsets and must not precede newly generated ZIP64
	// fields when an entry's merged output offset crosses 4 GiB.
	var extra []byte
	for remaining := entry.Extra; len(remaining) != 0; {
		if len(remaining) < 4 {
			return errors.New("malformed ZIP extra field")
		}
		size := int(binary.LittleEndian.Uint16(remaining[2:4])) + 4
		if size > len(remaining) {
			return errors.New("truncated ZIP extra field")
		}
		if binary.LittleEndian.Uint16(remaining[:2]) != 0x0001 {
			extra = append(extra, remaining[:size]...)
		}
		remaining = remaining[size:]
	}
	copy := *entry
	copy.Extra = extra
	return writer.Copy(&copy)
}
