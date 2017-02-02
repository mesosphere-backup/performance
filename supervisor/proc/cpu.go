package proc

import (
	"os"
	"runtime"
	"time"

	"github.com/fatih/structs"
	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/process"
)


type Reporter interface {
	Data() map[string]interface{}
}

// CPUPidUsage returns a cpu usage by pid.
type CPUPidUsage struct {
	UserCPU_Usage   float64
	SystemCPU_Usage float64
	TotalCPU_Usage  float64

	Hostname string
	Timestamp time.Time
}

func (c *CPUPidUsage) Data() map[string]interface{} {
	return structs.Map(c)
}

// LoadByPID returns a CPU load (user, system) by a pid within the interval
func LoadByPID(pid int32, interval time.Duration) (*CPUPidUsage, error) {
	if interval <= 0 {
		panic("Interval cannot be negative or zero")
	}

	p, err := process.NewProcess(int32(pid))
	if err != nil {
		return nil, err
	}

	before, err := getCPUTimes(p)
	if err != nil {
		return nil, err
	}

	time.Sleep(interval)

	after, err := getCPUTimes(p)
	if err != nil {
		return nil, err
	}

	return calcCPULoad(before, after)
}

type cpuTimes struct {
	pidUser   float64
	pidSystem float64
	cpuTotal  float64
}

func calcCPULoad(before *cpuTimes, after *cpuTimes) (*CPUPidUsage, error) {
	if before == nil || after == nil {
		panic("Arguments cannot be nil")
	}

	// http://stackoverflow.com/questions/1420426/how-to-calculate-the-cpu-usage-of-a-process-by-pid-in-linux-from-c
	user := float64(runtime.NumCPU()) * (after.pidUser - before.pidUser) * 100 / (after.cpuTotal - before.cpuTotal)
	system := float64(runtime.NumCPU()) * (after.pidSystem - before.pidSystem) * 100 / (after.cpuTotal - before.cpuTotal)

	hostname, err := os.Hostname()
	if err != nil {
		hostname = "localhost"
	}

	return &CPUPidUsage{
		UserCPU_Usage:   user,
		SystemCPU_Usage: system,
		TotalCPU_Usage:  user + system,
		Hostname: hostname,
		Timestamp: time.Now(),
	}, nil
}

func getCPUTimes(p *process.Process) (*cpuTimes, error) {
	times, err := p.Times()
	if err != nil {
		return nil, err
	}

	c, err := cpu.Times(false)
	if err != nil {
		return nil, err
	}

	return &cpuTimes{
		pidUser:   times.User,
		pidSystem: times.System,

		cpuTotal: c[0].Total(),
	}, nil
}
