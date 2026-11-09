package merge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
)

type datasetMeta struct {
	Type             string
	Count            uint64
	Version, Methods int64
	CollectorVersion string
}

func token(ctx context.Context, d *json.Decoder) (json.Token, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return d.Token()
}

func delimiter(ctx context.Context, d *json.Decoder, want json.Delim) error {
	t, err := token(ctx, d)
	if err != nil {
		return err
	}
	if t != want {
		return fmt.Errorf("expected JSON delimiter %q", want)
	}
	return nil
}

// skipValue walks JSON tokens, retaining neither a graph row nor an array.
func skipValue(ctx context.Context, d *json.Decoder, first json.Token, depth int) error {
	if depth > 256 {
		return errors.New("JSON nesting exceeds 256 levels")
	}
	start, compound := first.(json.Delim)
	if !compound {
		return nil
	}
	if start != '{' && start != '[' {
		return errors.New("unexpected closing JSON delimiter")
	}
	for d.More() {
		if start == '{' {
			key, err := token(ctx, d)
			if err != nil {
				return err
			}
			if _, ok := key.(string); !ok {
				return errors.New("JSON object key is not a string")
			}
		}
		value, err := token(ctx, d)
		if err != nil {
			return err
		}
		if err := skipValue(ctx, d, value, depth+1); err != nil {
			return err
		}
	}
	closing := json.Delim(']')
	if start == '{' {
		closing = '}'
	}
	return delimiter(ctx, d, closing)
}

func readMeta(ctx context.Context, d *json.Decoder) (datasetMeta, error) {
	var meta datasetMeta
	if err := delimiter(ctx, d, '{'); err != nil {
		return meta, err
	}
	seen := make(map[string]bool)
	for d.More() {
		t, err := token(ctx, d)
		if err != nil {
			return meta, err
		}
		key, ok := t.(string)
		if !ok || seen[key] {
			return meta, fmt.Errorf("duplicate or invalid metadata key %q", key)
		}
		seen[key] = true
		value, err := token(ctx, d)
		if err != nil {
			return meta, err
		}
		switch key {
		case "type", "collectorversion":
			text, ok := value.(string)
			if !ok {
				return meta, fmt.Errorf("metadata %s must be a string", key)
			}
			if key == "type" {
				meta.Type = text
			} else {
				meta.CollectorVersion = text
			}
		case "count", "version", "methods":
			number, ok := value.(json.Number)
			if !ok {
				return meta, fmt.Errorf("metadata %s must be an integer", key)
			}
			if key == "count" {
				meta.Count, err = strconv.ParseUint(string(number), 10, 64)
			} else if key == "version" {
				meta.Version, err = strconv.ParseInt(string(number), 10, 64)
			} else {
				meta.Methods, err = strconv.ParseInt(string(number), 10, 64)
			}
			if err != nil {
				return meta, fmt.Errorf("invalid metadata %s: %w", key, err)
			}
		default:
			return meta, fmt.Errorf("unexpected metadata key %q", key)
		}
	}
	if err := delimiter(ctx, d, '}'); err != nil {
		return meta, err
	}
	if len(seen) != 5 {
		return meta, errors.New("dataset metadata is missing required fields")
	}
	return meta, nil
}

func validateJSON(ctx context.Context, reader io.Reader, m Manifest) (datasetMeta, error) {
	var meta datasetMeta
	d := json.NewDecoder(reader)
	d.UseNumber()
	if err := delimiter(ctx, d, '{'); err != nil {
		return meta, err
	}
	seen := make(map[string]bool)
	var rows uint64
	for d.More() {
		t, err := token(ctx, d)
		if err != nil {
			return meta, err
		}
		key, ok := t.(string)
		if !ok || seen[key] {
			return meta, fmt.Errorf("duplicate or invalid dataset root key %q", key)
		}
		seen[key] = true
		switch key {
		case "data":
			if err := delimiter(ctx, d, '['); err != nil {
				return meta, err
			}
			for d.More() {
				before := d.InputOffset()
				start, err := token(ctx, d)
				if err != nil {
					return meta, err
				}
				if start != json.Delim('{') {
					return meta, errors.New("dataset data must contain JSON objects")
				}
				if err := skipValue(ctx, d, start, 2); err != nil {
					return meta, err
				}
				// InputOffset includes at most the separating comma and whitespace.
				// The source manifest's limit applies to emitted row text; the
				// parser does not impose a smaller, unrelated buffer limit.
				if uint64(d.InputOffset()-before) > m.SingleRecordLimit+1 {
					return meta, errors.New("JSON object exceeds the manifest single-record limit")
				}
				rows++
				if rows > m.Records {
					return meta, errors.New("dataset row count exceeds manifest total")
				}
			}
			if err := delimiter(ctx, d, ']'); err != nil {
				return meta, err
			}
		case "meta":
			meta, err = readMeta(ctx, d)
			if err != nil {
				return meta, err
			}
		default:
			return meta, fmt.Errorf("unexpected dataset root key %q", key)
		}
	}
	if err := delimiter(ctx, d, '}'); err != nil {
		return meta, err
	}
	// Reading through EOF also makes archive/zip verify the entry CRC.
	if _, err := token(ctx, d); !errors.Is(err, io.EOF) {
		if err != nil {
			return meta, fmt.Errorf("dataset trailing bytes or ZIP checksum: %w", err)
		}
		return meta, errors.New("dataset contains trailing JSON")
	}
	if len(seen) != 2 || !seen["data"] || !seen["meta"] || rows == 0 || meta.Count != rows {
		return meta, errors.New("dataset metadata count differs from actual rows")
	}
	if _, ok := m.Datasets[meta.Type]; !ok || meta.Version != 6 || meta.Methods != m.Methods || meta.CollectorVersion != "AdaptiHound-BOF" {
		return meta, errors.New("dataset type, version, methods or collector differs from manifest")
	}
	return meta, nil
}
