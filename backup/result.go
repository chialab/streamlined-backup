package backup

type Status string

const (
	StatusSkipped Status = "skipped"
	StatusSuccess Status = "success"
	StatusFailed  Status = "failed"
	StatusTimeout Status = "timeout"
)

func (status Status) Priority() uint {
	switch status {
	case StatusSuccess:
		return 10
	case StatusFailed:
		return 20
	case StatusTimeout:
		return 30
	default:
		return 0
	}
}

func NewResultSkipped(task *Task) Result {
	return Result{
		status: StatusSkipped,
		task:   task,
	}
}

func NewResultSuccess(task *Task, logs []string) Result {
	return Result{
		status: StatusSuccess,
		task:   task,
		logs:   logs,
	}
}

func NewResultFailed(task *Task, err error, logs []string) Result {
	return Result{
		status: StatusFailed,
		task:   task,
		err:    err,
		logs:   logs,
	}
}

func NewResultTimeout(task *Task, logs []string) Result {
	return Result{
		status: StatusTimeout,
		task:   task,
	}
}

const UNKNOWN_TASK = "(unknown)"

type Result struct {
	status Status
	task   *Task
	err    error
	logs   []string
}

func (r Result) Status() Status {
	return r.status
}

func (r Result) Name() string {
	if r.task == nil {
		return UNKNOWN_TASK
	}

	return r.task.Name()
}

func (r Result) Command() string {
	if r.task != nil {
		if cmd := r.task.CommandString(); cmd != "" {
			return cmd
		}
	}

	return UNKNOWN_TASK
}

func (r Result) ActualCwd() string {
	if r.task != nil {
		if cwd := r.task.ActualCwd(); cwd != "" {
			return cwd
		}
	}

	return UNKNOWN_TASK
}

func (r Result) Error() error {
	return r.err
}

func (r Result) Logs() []string {
	return r.logs
}

type Results []Result

func (r Results) Len() int {
	return len(r)
}
func (r Results) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}
func (r Results) Less(i, j int) bool {
	if r[i].status != r[j].status {
		return r[i].status.Priority() < r[j].status.Priority()
	}
	return r[i].Name() < r[j].Name()
}
