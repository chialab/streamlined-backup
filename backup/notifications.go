package backup

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

type Notifications struct {
	slackWebhooks listOfStrings
}

func NewNotifications(slackWebhooks listOfStrings) *Notifications {
	return &Notifications{slackWebhooks}
}

func (n *Notifications) Notify(results ...OperationResult) error {
	successes := []OperationResult{}
	failures := []OperationResult{}
	for _, result := range results {
		switch result.Status {
		case StatusSuccess:
			successes = append(successes, result)
		case StatusFailure:
			failures = append(failures, result)
		}
	}

	return n.notifySlack(successes, failures)
}

func (n *Notifications) notifySlack(successes []OperationResult, failures []OperationResult) error {
	payload := map[string]interface{}{
		"blocks": []interface{}{},
	}

	if len(successes) > 0 {
		lines := []string{}
		for _, result := range successes {
			lines = append(lines, fmt.Sprintf("- %s :white_check_mark:", result.Name))
		}
		payload["blocks"] = append(payload["blocks"].([]interface{}), map[string]interface{}{
			"type": "section",
			"text": map[string]string{
				"type": "mrkdwn",
				"text": fmt.Sprintf("*Backup operation(s) completed:*\n\n%s", strings.Join(lines, "\n")),
			},
		})
	}

	if len(failures) > 0 {
		if len(payload["blocks"].([]interface{})) > 0 {
			payload["blocks"] = append(payload["blocks"].([]interface{}), map[string]interface{}{
				"type": "divider",
			})
		}
		payload["blocks"] = append(payload["blocks"].([]interface{}), map[string]interface{}{
			"type": "header",
			"text": map[string]string{
				"type": "plain_text",
				"text": "@channel Backup operation(s) failed",
			},
		})

		for _, result := range failures {
			payload["blocks"] = append(payload["blocks"].([]interface{}), map[string]interface{}{
				"type": "section",
				"text": map[string]string{
					"type": "mrkdwn",
					"text": fmt.Sprintf(
						"*%s*\n\nError:\n```\n%s\n```\n\nLog lines (writen to stderr):\n```\n%s\n```\n\n",
						result.Name,
						result.Error,
						strings.Join(result.Logs, "\n"),
					),
				},
			})
		}
	}

	buf := bytes.NewBuffer(nil)
	enc := json.NewEncoder(buf)
	if err := enc.Encode(payload); err != nil {
		return err
	}

	pool := make(chan bool, 3)
	errors := make(chan error)
	for _, webhook := range n.slackWebhooks {
		pool <- true
		go func(url string) {
			defer func() { <-pool }()

			if _, err := http.Post(url, "application/json", buf); err != nil {
				errors <- err
				fmt.Printf("Error sending notification to %s: %s\n", url, err)
			}
		}(webhook)
	}
	for i := 0; i < cap(pool); i++ {
		pool <- true
	}

	if len(errors) > 0 {
		return <-errors
	}

	return nil
}
