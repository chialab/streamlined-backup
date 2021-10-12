package notifier

import "testing"

func TestMustToJson(t *testing.T) {
	t.Parallel()

	type testCase struct {
		input    interface{}
		expected string
	}
	testCases := []testCase{
		{
			input:    nil,
			expected: `null` + "\n",
		},
		{
			input:    "",
			expected: `""` + "\n",
		},
		{
			input:    "test",
			expected: `"test"` + "\n",
		},
		{
			input:    1,
			expected: `1` + "\n",
		},
		{
			input:    1.1,
			expected: `1.1` + "\n",
		},
		{
			input:    true,
			expected: `true` + "\n",
		},
		{
			input: map[string]interface{}{
				"test": "test",
			},
			expected: `{"test":"test"}` + "\n",
		},
		{
			input: []interface{}{
				"test",
				"test",
			},
			expected: `["test","test"]` + "\n",
		},
	}

	for _, testCase := range testCases {
		result := string(MustToJSON(testCase.input))
		if result != testCase.expected {
			t.Errorf("expected %s, got %s", testCase.expected, result)
		}
	}
}
