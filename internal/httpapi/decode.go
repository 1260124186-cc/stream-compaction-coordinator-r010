package httpapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/domain"
)

func decodeJSON(
	writer http.ResponseWriter,
	request *http.Request,
	maximumBytes int64,
	target any,
) error {
	contentType := request.Header.Get("Content-Type")
	if contentType != "" && !strings.HasPrefix(strings.ToLower(contentType), "application/json") {
		return domain.NewError(
			domain.CodeInvalidInput,
			"Content-Type must be application/json",
		)
	}
	request.Body = http.MaxBytesReader(writer, request.Body, maximumBytes)
	data, err := io.ReadAll(request.Body)
	if err != nil {
		return decodeError(err)
	}

	// Every JSON action endpoint expects a request object; a top-level null
	// (or array/scalar) would otherwise decode into the zero-value target and
	// silently execute the action with the wrong body shape.
	decoder := json.NewDecoder(bytes.NewReader(data))
	first, err := decoder.Token()
	if err != nil {
		return decodeError(err)
	}
	delimiter, ok := first.(json.Delim)
	if !ok || delimiter != '{' {
		return domain.NewError(domain.CodeInvalidInput, "request body must be a JSON object").
			WithDetail("received", jsonTokenDescription(first))
	}

	objectDecoder := json.NewDecoder(bytes.NewReader(data))
	objectDecoder.DisallowUnknownFields()
	if err := objectDecoder.Decode(target); err != nil {
		return decodeError(err)
	}
	if err := ensureNoExtraJSON(objectDecoder); err != nil {
		return err
	}
	return nil
}

func jsonTokenDescription(token json.Token) string {
	switch value := token.(type) {
	case json.Delim:
		switch value {
		case '[':
			return "array"
		case '{':
			return "object"
		default:
			return "json"
		}
	case nil:
		return "null"
	case bool:
		return "boolean"
	case string:
		return "string"
	case json.Number, float64:
		return "number"
	default:
		return fmt.Sprintf("%T", value)
	}
}

func decodeError(err error) error {
	var maximumErr *http.MaxBytesError
	if errors.As(err, &maximumErr) {
		return domain.NewError(domain.CodeLimitExceeded, "request body exceeds the configured limit").
			WithDetail("maximum_bytes", maximumErr.Limit)
	}
	var syntaxErr *json.SyntaxError
	if errors.As(err, &syntaxErr) {
		return domain.NewError(domain.CodeInvalidInput, "request body is not valid JSON").
			WithDetail("offset", syntaxErr.Offset)
	}
	var typeErr *json.UnmarshalTypeError
	if errors.As(err, &typeErr) {
		return domain.NewError(domain.CodeInvalidInput, "request field has the wrong JSON type").
			WithDetail("field", typeErr.Field).
			WithDetail("offset", typeErr.Offset)
	}
	if strings.Contains(err.Error(), "unknown field") {
		return domain.NewError(domain.CodeInvalidInput, err.Error())
	}
	return domain.NewError(domain.CodeInvalidInput, "request body could not be decoded")
}

func ensureNoExtraJSON(decoder *json.Decoder) error {
	var extra any
	err := decoder.Decode(&extra)
	if err == io.EOF {
		return nil
	}
	if err == nil {
		return domain.NewError(domain.CodeInvalidInput, "request body contains trailing JSON content")
	}
	return domain.NewError(
		domain.CodeInvalidInput,
		fmt.Sprintf("request body could not be decoded: %v", err),
	)
}
