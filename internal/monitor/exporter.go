// Copyright 2021 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package monitor

import (
	"fmt"
	"time"

	"contrib.go.opencensus.io/exporter/stackdriver"
	"github.com/googlecloudplatform/gcsfuse/internal/logger"
)

var exporter *stackdriver.Exporter

// EnableStackdriverExporter starts to collect monitoring metrics and exports
// them to Stackdriver iff the given interval is positive.
func EnableStackdriverExporter(interval time.Duration) error {
	if interval <= 0 {
		return nil
	}

	var err error
	if exporter, err = stackdriver.NewExporter(stackdriver.Options{
		ReportingInterval: interval,
		OnError: func(err error) {
			logger.Infof("Fail to send metric: %v", err)
		},
	}); err != nil {
		return fmt.Errorf("create exporter: %w", err)
	}
	if err = exporter.StartMetricsExporter(); err != nil {
		return fmt.Errorf("start exporter: %w", err)
	}
	return nil
}

// CloseStackdriverExporter ensures all collected metrics are sent to
// Stackdriver and closes the exporter.
func CloseStackdriverExporter() {
	if exporter != nil {
		exporter.StopMetricsExporter()
		exporter.Flush()
	}
	exporter = nil
}
