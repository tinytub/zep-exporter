package system

import (
	"fmt"
	"os"
	"time"

	sigar "github.com/elastic/gosigar"
	"github.com/tinytub/zep-exporter/utils/match"
)

type Process struct {
	Pid        int    `json:"pid"`
	Ppid       int    `json:"ppid"`
	Pgid       int    `json:"pgid"`
	Name       string `json:"name"`
	Username   string `json:"username"`
	State      string `json:"state"`
	CmdLine    string `json:"cmdline"`
	Cwd        string `json:"cwd"`
	Mem        sigar.ProcMem
	Cpu        sigar.ProcTime
	SampleTime time.Time
	FD         sigar.ProcFDUsage
	//Env             common.MapStr
	cpuSinceStart   float64
	cpuTotalPct     float64
	cpuTotalPctNorm float64
}

type ProcsMap map[int]*Process

type Stats struct {
	Procs    []string
	ProcsMap ProcsMap
	//CpuTicks     bool
	EnvWhitelist []string
	CacheCmdLine bool
	//IncludeTop   IncludeTopConfig

	procRegexps []match.Matcher // List of regular expressions used to whitelist processes.
	//envRegexps  []match.Matcher // List of regular expressions used to whitelist env vars.
}

func (procStats *Stats) Init() error {
	procStats.ProcsMap = make(ProcsMap)

	if len(procStats.Procs) == 0 {
		return nil
	}

	//matcher 拆出来
	procStats.procRegexps = []match.Matcher{}

	for _, pattern := range procStats.Procs {
		reg, err := match.Compile(pattern)
		if err != nil {
			return fmt.Errorf("Failed to compile regexp [%s]: %v", pattern, err)
		}
		procStats.procRegexps = append(procStats.procRegexps, reg)
	}

	return nil
}

func (procStats *Stats) GetProcMap() {
	if len(procStats.Procs) == 0 {
		fmt.Println("Procs == 0")
		return
	}

	pids, err := Pids()
	if err != nil {
		fmt.Println(err, "failed to fetch the list of PIDs")
		return
	}

	var processes []Process
	newProcs := make(ProcsMap, len(pids))

	for _, pid := range pids {
		process := procStats.getSingleProcess(pid, newProcs)
		if process == nil {
			continue
		}
		processes = append(processes, *process)
	}
	procStats.ProcsMap = newProcs
}

func (procStats *Stats) getSingleProcess(pid int, newProcs ProcsMap) *Process {
	var cmdline string
	process, err := newProcess(pid, cmdline)
	if err != nil {
		return nil
	}

	if !procStats.matchProcess(process.Name) {
		return nil
	}

	newProcs[process.Pid] = process
	return process
}

func (procStats *Stats) matchProcess(name string) bool {
	for _, reg := range procStats.procRegexps {
		if reg.MatchString(name) {
			return true
		}
	}
	return false
}

func Pids() ([]int, error) {
	pids := sigar.ProcList{}
	err := pids.Get()
	if err != nil {
		return nil, err
	}
	return pids.List, nil
}

func newProcess(pid int, cmdline string) (*Process, error) {
	state := sigar.ProcState{}
	if err := state.Get(pid); err != nil {
		return nil, fmt.Errorf("error getting process state for pid=%d: %v", pid, err)
	}

	exe := sigar.ProcExe{}
	if err := exe.Get(pid); err != nil && !sigar.IsNotImplemented(err) && !os.IsPermission(err) {
		return nil, fmt.Errorf("error getting process exe for pid=%d: %v", pid, err)
	}

	proc := Process{
		Pid:      pid,
		Ppid:     state.Ppid,
		Pgid:     state.Pgid,
		Name:     state.Name,
		Username: state.Username,
		State:    getProcState(byte(state.State)),
		CmdLine:  cmdline,
		Cwd:      exe.Cwd,
		//Env:      env,
	}

	return &proc, nil
}

func getProcState(b byte) string {
	switch b {
	case 'S':
		return "sleeping"
	case 'R':
		return "running"
	case 'D':
		return "idle"
	case 'T':
		return "stopped"
	case 'Z':
		return "zombie"
	}
	return "unknown"
}
