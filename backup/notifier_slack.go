package backup

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/hashicorp/go-multierror"
)

type SlackNotifier struct {
	Notifier
	webhooks []string
}

func NewSlackNotifier(webhooks ...string) *SlackNotifier {
	return &SlackNotifier{webhooks: webhooks}
}

func (n SlackNotifier) Format(o *OperationResult) map[string]interface{} {
	switch o.Status {
	case StatusSuccess:
		return map[string]interface{}{
			"type": "section",
			"text": map[string]string{
				"type": "mrkdwn",
				"text": fmt.Sprintf(":white_checkmark: Backup operation `%s` completed successfully.", o.Name()),
			},
		}

	case StatusFailure:
		return map[string]interface{}{
			"type": "section",
			"text": map[string]string{
				"type": "mrkdwn",
				"text": fmt.Sprintf(":rotating_light: *Error running backup operation `%s`!* @channel", o.Name()),
			},
			"fields": []map[string]string{
				{
					"type": "mrkdwn",
					"text": fmt.Sprintf("*Command:*\n```\n%s\n```", o.Command()),
				},
				{
					"type": "mrkdwn",
					"text": fmt.Sprintf("*Working directory:*\n```\n%s\n```", o.ActualCwd()),
				},
				{
					"type": "mrkdwn",
					"text": fmt.Sprintf("*Error:*\n```\n%s\n```", strings.TrimSpace(o.Error.Error())),
				},
				{
					"type": "mrkdwn",
					"text": fmt.Sprintf("*Log lines (written to stderr):*\n```\n%s\n```", strings.TrimSpace(strings.Join(o.Logs, "\n"))),
				},
			},
		}
	}

	return nil
}

func (n SlackNotifier) Notify(results ...OperationResult) error {
	type payload struct {
		Blocks []interface{}
	}

	body := payload{}
	for _, result := range results {
		body.Blocks = append(body.Blocks, n.Format(&result))
	}

	buf := bytes.NewBuffer(nil)
	if err := json.NewEncoder(buf).Encode(body); err != nil {
		return err
	}

	wg := sync.WaitGroup{}
	mutex := sync.Mutex{}
	var errors *multierror.Error
	for _, webhook := range n.webhooks {
		wg.Add(1)
		go func(url string) {
			defer wg.Done()

			body := bytes.NewBuffer(buf.Bytes())

			response, err := http.Post(url, "application/json", body)
			if err != nil {
				mutex.Lock()
				defer mutex.Unlock()
				errors = multierror.Append(errors, err)

				return
			}
			defer response.Body.Close()

			if response.StatusCode != 200 {
				err := fmt.Errorf("error sending notification to %s: %s", url, response.Status)

				mutex.Lock()
				defer mutex.Unlock()
				errors = multierror.Append(errors, err)
			}
		}(webhook)
	}
	wg.Wait()

	return errors.ErrorOrNil()
}
