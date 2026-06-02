package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

type responseFormatKind string

const (
	responseFormatNone       responseFormatKind = ""
	responseFormatText       responseFormatKind = "text"
	responseFormatJSONObject responseFormatKind = "json_object"
	responseFormatJSONSchema responseFormatKind = "json_schema"
)

type structuredResponseFormat struct {
	Kind        responseFormatKind
	Name        string
	Description string
	Strict      bool
	Schema      any
}

func (f structuredResponseFormat) RequiresValidation() bool {
	return f.Kind == responseFormatJSONObject || f.Kind == responseFormatJSONSchema
}

func (f structuredResponseFormat) JSONInstruction() string {
	base := "Return exactly one valid JSON object. Do not include Markdown fences, prose, comments, or any text outside the JSON object."
	if f.Kind != responseFormatJSONSchema {
		return base
	}

	parts := make([]string, 0, 4)
	if f.Name != "" {
		parts = append(parts, "name: "+f.Name)
	}
	if f.Description != "" {
		parts = append(parts, "description: "+f.Description)
	}
	if f.Strict {
		parts = append(parts, "strict: true")
	}
	if schema := compactJSONValue(f.Schema); schema != "" {
		parts = append(parts, "schema: "+schema)
	}
	if len(parts) == 0 {
		return base
	}
	return "Return exactly one valid JSON object matching this JSON schema response_format (" + strings.Join(parts, "; ") + "). Do not include Markdown fences, prose, comments, or any text outside the JSON object."
}

func parseResponseFormat(raw any) (structuredResponseFormat, error) {
	if raw == nil {
		return structuredResponseFormat{Kind: responseFormatNone}, nil
	}

	responseFormat := mapValue(raw)
	if responseFormat == nil {
		return structuredResponseFormat{}, fmt.Errorf("response_format must be an object")
	}

	formatType := responseFormatKind(strings.ToLower(strings.TrimSpace(stringValue(responseFormat["type"]))))
	switch formatType {
	case "", responseFormatText:
		return structuredResponseFormat{Kind: responseFormatText}, nil
	case responseFormatJSONObject:
		return structuredResponseFormat{Kind: responseFormatJSONObject}, nil
	case responseFormatJSONSchema:
		jsonSchema := mapValue(responseFormat["json_schema"])
		spec := structuredResponseFormat{
			Kind:   responseFormatJSONSchema,
			Schema: responseFormat["schema"],
		}
		if jsonSchema != nil {
			spec.Name = strings.TrimSpace(stringValue(jsonSchema["name"]))
			spec.Description = strings.TrimSpace(stringValue(jsonSchema["description"]))
			spec.Strict = boolValue(jsonSchema["strict"])
			if jsonSchema["schema"] != nil {
				spec.Schema = jsonSchema["schema"]
			}
		}
		if spec.Schema == nil {
			return structuredResponseFormat{}, fmt.Errorf("response_format json_schema.schema is required")
		}
		return spec, nil
	default:
		return structuredResponseFormat{}, fmt.Errorf("unsupported response_format type %q", formatType)
	}
}

func validateResponseFormatSchema(responseFormat structuredResponseFormat) error {
	if responseFormat.Kind != responseFormatJSONSchema {
		return nil
	}
	if _, err := compileResponseJSONSchema(responseFormat.Schema); err != nil {
		return fmt.Errorf("response_format json_schema.schema is invalid: %w", err)
	}
	return nil
}

func normalizeResponseFormatForUpstream(payload map[string]any) error {
	responseFormat, err := parseResponseFormat(payload["response_format"])
	if err != nil {
		return err
	}

	switch responseFormat.Kind {
	case responseFormatNone, responseFormatText:
		delete(payload, "response_format")
	case responseFormatJSONObject, responseFormatJSONSchema:
		payload["response_format"] = map[string]any{"type": string(responseFormatJSONObject)}
		prependSystemInstruction(payload, responseFormat.JSONInstruction())
	}
	return nil
}

type structuredOutputValidationError struct {
	Reason string
}

func (e *structuredOutputValidationError) Error() string {
	return "upstream response did not satisfy response_format: " + e.Reason
}

func (e *structuredOutputValidationError) RetryInstruction() string {
	return "The previous response did not satisfy the requested response_format because " + e.Reason + ". Return only one corrected JSON object that satisfies the response_format. Do not include Markdown fences, prose, comments, or any text outside the JSON object."
}

func validateOpenAIChatCompletionStructuredOutput(body []byte, responseFormat structuredResponseFormat) error {
	var payload struct {
		Choices []struct {
			Message struct {
				Content any `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return fmt.Errorf("decode upstream response: %w", err)
	}
	if len(payload.Choices) == 0 {
		return &structuredOutputValidationError{Reason: "response has no choices to validate"}
	}

	var schema *jsonschema.Schema
	if responseFormat.Kind == responseFormatJSONSchema {
		compiled, err := compileResponseJSONSchema(responseFormat.Schema)
		if err != nil {
			return fmt.Errorf("response_format json_schema.schema is invalid: %w", err)
		}
		schema = compiled
	}

	for index, choice := range payload.Choices {
		content, ok := choice.Message.Content.(string)
		if !ok {
			return &structuredOutputValidationError{Reason: fmt.Sprintf("choice %d message content is not a string", index)}
		}
		value, err := parseStructuredOutputContent(content)
		if err != nil {
			return &structuredOutputValidationError{Reason: fmt.Sprintf("choice %d %s", index, err.Error())}
		}
		if responseFormat.Kind == responseFormatJSONObject {
			if _, ok := value.(map[string]any); !ok {
				return &structuredOutputValidationError{Reason: fmt.Sprintf("choice %d message content is valid JSON but not a JSON object", index)}
			}
			continue
		}
		if schema != nil {
			if err := schema.Validate(value); err != nil {
				return &structuredOutputValidationError{Reason: fmt.Sprintf("choice %d message content does not match json_schema: %v", index, err)}
			}
		}
	}
	return nil
}

func parseStructuredOutputContent(content string) (any, error) {
	trimmed := strings.TrimSpace(content)
	if trimmed == "" {
		return nil, fmt.Errorf("message content is empty")
	}
	value, err := jsonschema.UnmarshalJSON(strings.NewReader(trimmed))
	if err != nil {
		return nil, fmt.Errorf("message content is not valid JSON: %w", err)
	}
	return value, nil
}

func compileResponseJSONSchema(schemaDoc any) (*jsonschema.Schema, error) {
	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft2020)
	if err := compiler.AddResource("schema.json", schemaDoc); err != nil {
		return nil, err
	}
	return compiler.Compile("schema.json")
}

func compactJSONValue(value any) string {
	if value == nil {
		return ""
	}
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return ""
	}
	return strings.TrimSpace(buf.String())
}
