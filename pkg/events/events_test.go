/*
Copyright paskal.maksim@gmail.com
Licensed under the Apache License, Version 2.0 (the "License")
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package events_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/maksim-paskal/aks-node-termination-handler/pkg/events"
	"github.com/maksim-paskal/aks-node-termination-handler/pkg/types"
	"github.com/maksim-paskal/aks-node-termination-handler/pkg/utils"
	log "github.com/sirupsen/logrus"
)

func TestReadingEvents(t *testing.T) { //nolint:funlen
	t.Parallel()

	log.SetLevel(log.DebugLevel)

	ctx := context.TODO()

	handler := http.NewServeMux()
	handler.HandleFunc("/badjson", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`!!!{"DocumentIncarnation":1,"Events":[]}`))
	})
	handler.HandleFunc("/emptyjson", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(``))
	})
	handler.HandleFunc("/incorrectcontentlen", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Add("Content-Length", "50")

		_, _ = w.Write([]byte("a"))
	})
	handler.HandleFunc("/timeout", func(w http.ResponseWriter, r *http.Request) {
		utils.SleepWithContext(r.Context(), 5*time.Second)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(``))
	})
	handler.HandleFunc("/document", func(w http.ResponseWriter, _ *http.Request) {
		message, _ := json.Marshal(types.ScheduledEventsType{
			DocumentIncarnation: 1,
			Events: []types.ScheduledEventsEvent{
				{
					EventId:      time.Now().String(),
					EventType:    types.EventTypeFreeze,
					ResourceType: "resourceType",
					Resources:    []string{"resource1", "resource2"},
				},
			},
		})

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(message)
	})

	type acknowledgeRequest struct {
		method      string
		metadata    string
		contentType string
		body        string
	}

	acknowledgeRequests := make(chan acknowledgeRequest, 1)

	handler.HandleFunc("/acknowledge", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		acknowledgeRequests <- acknowledgeRequest{
			method:      r.Method,
			metadata:    r.Header.Get("Metadata"),
			contentType: r.Header.Get("Content-Type"),
			body:        string(body),
		}

		w.WriteHeader(http.StatusOK)
	})

	handler.HandleFunc("/internalerror", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})

	testServer := httptest.NewServer(handler)

	t.Run("badjson", func(t *testing.T) {
		t.Parallel()

		eventReader := events.NewReader()
		eventReader.Endpoint = testServer.URL + "/badjson"

		if _, err := eventReader.ReadEndpoint(ctx); err == nil {
			t.Error("expected error")
		}
	})

	t.Run("badhttp", func(t *testing.T) {
		t.Parallel()

		eventReader := events.NewReader()
		eventReader.Method = "bad method"
		eventReader.Endpoint = "fake://fake"

		if _, err := eventReader.ReadEndpoint(ctx); err == nil {
			t.Error("expected error")
		}

		ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
		defer cancel()

		eventReader.ReadEvents(ctx)
	})

	t.Run("badhttpcontent", func(t *testing.T) {
		t.Parallel()

		eventReader := events.NewReader()
		eventReader.Endpoint = testServer.URL + "/incorrectcontentlen"

		if _, err := eventReader.ReadEndpoint(ctx); err == nil {
			t.Error("expected error")
		}
	})

	t.Run("emptyjson", func(t *testing.T) {
		t.Parallel()

		eventReader := events.NewReader()
		eventReader.Endpoint = testServer.URL + "/emptyjson"

		if _, err := eventReader.ReadEndpoint(ctx); err != nil {
			t.Error(err)
		}
	})

	t.Run("timeout", func(t *testing.T) {
		t.Parallel()

		eventReader := events.NewReader()
		eventReader.Endpoint = testServer.URL + "/timeout"
		eventReader.RequestTimeout = 1 * time.Second

		if _, err := eventReader.ReadEndpoint(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Error(err)
		}
	})

	t.Run("acknowledge", func(t *testing.T) {
		t.Parallel()

		if err := events.AcknowledgeEvent(ctx, testServer.URL+"/acknowledge", "test-event-id"); err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		select {
		case req := <-acknowledgeRequests:
			if req.method != http.MethodPost {
				t.Errorf("expected POST, got %s", req.method)
			}

			if req.metadata != "true" {
				t.Errorf("expected Metadata header 'true', got %q", req.metadata)
			}

			if req.contentType != "application/json" {
				t.Errorf("expected Content-Type 'application/json', got %q", req.contentType)
			}

			var parsed struct {
				StartRequests []struct {
					EventID string `json:"EventId"`
				} `json:"StartRequests"`
			}

			if err := json.Unmarshal([]byte(req.body), &parsed); err != nil {
				t.Fatalf("body is not valid JSON: %v (body: %s)", err, req.body)
			}

			if len(parsed.StartRequests) != 1 || parsed.StartRequests[0].EventID != "test-event-id" {
				t.Errorf("unexpected parsed body: %+v", parsed)
			}
		case <-time.After(2 * time.Second):
			t.Error("timed out waiting for acknowledge request")
		}
	})

	t.Run("acknowledge_badstatus", func(t *testing.T) {
		t.Parallel()

		if err := events.AcknowledgeEvent(ctx, testServer.URL+"/internalerror", "test-event-id"); err == nil {
			t.Error("expected error for non-200 response")
		}
	})

	t.Run("acknowledge_badurl", func(t *testing.T) {
		t.Parallel()

		if err := events.AcknowledgeEvent(ctx, "fake://invalid", "test-event-id"); err == nil {
			t.Error("expected error for bad url")
		}
	})

	t.Run("document", func(t *testing.T) {
		t.Parallel()

		receivedDocument := types.ScheduledEventsEvent{}

		eventReader := events.NewReader()
		eventReader.Endpoint = testServer.URL + "/document"
		eventReader.AzureResource = "resource1"
		eventReader.BeforeReading = func(_ context.Context) error {
			return errors.New("error in BeforeReading") //nolint:goerr113
		}
		eventReader.EventReceived = func(_ context.Context, event types.ScheduledEventsEvent) (bool, error) {
			receivedDocument = event

			return true, nil
		}

		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		eventReader.ReadEvents(ctx)

		t.Logf("%+v", receivedDocument)

		if receivedDocument.EventId == "" {
			t.Error("unexpected event id")
		}
	})
}
