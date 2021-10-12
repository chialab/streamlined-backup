package notifier

import (
	"bytes"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/chialab/streamlined-backup/backup"
	"github.com/hashicorp/go-multierror"
)

type SlackNotifier struct {
	webhooks []string
}

func NewSlackNotifier(webhooks ...string) *SlackNotifier {
	return &SlackNotifier{webhooks: webhooks}
}

func (n SlackNotifier) Format(o *backup.Result) map[string]interface{} {
	switch o.Status {
	case backup.StatusSuccess:
		return map[string]interface{}{
			"type": "section",
			"text": map[string]string{
				"type": "mrkdwn",
				"text": fmt.Sprintf(":white_check_mark: Backup task `%s` completed successfully.", o.Name()),
			},
		}

	case backup.StatusFailure:
		return map[string]interface{}{
			"type": "section",
			"text": map[string]string{
				"type": "mrkdwn",
				"text": fmt.Sprintf(":rotating_light: *Error running backup task `%s`!* @channel", o.Name()),
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

func (n SlackNotifier) Notify(results ...backup.Result) error {
	type payload struct {
		Blocks []interface{} `json:"blocks"`
	}

	body := payload{}
	for _, result := range results {
		if block := n.Format(&result); block != nil {
			body.Blocks = append(body.Blocks, block)
		}
	}

	if len(body.Blocks) == 0 {
		return nil
	}

	return n.Send(body)
}

func (n SlackNotifier) Error(err error) error {
	body := map[string]interface{}{
		"blocks": []map[string]interface{}{
			{
				"type": "section",
				"text": map[string]string{
					"type": "mrkdwn",
					"text": fmt.Sprintf(":rotating_light: *Error running backup task!* @channel\n```\n%v\n```", err),
				},
			},
		},
	}

	return n.Send(body)
}

func (n SlackNotifier) Send(body interface{}) error {
	jsonBody := MustToJSON(body)

	wg := sync.WaitGroup{}
	mutex := sync.Mutex{}
	var errors *multierror.Error
	for _, webhook := range n.webhooks {
		wg.Add(1)
		go func(url string) {
			defer wg.Done()

			body := bytes.NewBuffer(jsonBody)

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
