# go-scale-in-protection

Monitor workers' statuses to enable or disable instance scale-in protection accordingly.

```bash
go get github.com/nguyengg/go-scale-in-protection
```

## Inspiration

The need for this monitor arises
from https://docs.aws.amazon.com/autoscaling/ec2/userguide/as-using-sqs-queue.html#scale-sqs-queue-scale-in-protection:

```
while (true)
{
  SetInstanceProtection(False);
  Work = GetNextWorkUnit();
  SetInstanceProtection(True);
  ProcessWorkUnit(Work);
  SetInstanceProtection(False);
}
```

Essentially, if you have any number of workers who can be either ACTIVE or IDLE, you generally want to enable scale-in
protection when any of your worker is actively doing some work, while once all the workers have become idle, you would
want to disable scale-in protection to let the Auto Scaling group reclaim our instance naturally.

## Usage

```go
package main

import (
	"context"
	sip "github.com/nguyengg/go-scale-in-protection"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Kill, os.Interrupt, syscall.SIGTERM)
	defer stop()

	s := &sip.ScaleInProtector{
		// both InstanceId and AutoScalingGroupName are optional.
		// the monitor knows how to use the default aws.Config to figure out its own instance Id (via IMDSv2) and the
		// Auto Scaling group name containing that instance id.
		InstanceId:           "i-1234",
		AutoScalingGroupName: "my-asg",
		// when the last worker becomes idle, wait for another 15" before marking the instance as safe to terminate.
		IdleAtLeast: 15 * time.Minute,
	}

	// start the workers first. the monitor doesn't need to know beforehand how many workers are there but you should
	// signal active at least once because the monitor starts out assuming all workers are idle.
	workerCount := 5
	var wg sync.WaitGroup
	for i := range workerCount {
		wg.Add(1)
		go func() {
			defer wg.Done()

			id := strconv.Itoa(i)
			s.SignalActive(id)

			for {
				// get some work.

				// mark active.
				s.SignalActive(id)

				// do the work.

				// mark idle.
				s.SignalIdle(id)
			}
		}()
	}

	// always starts the monitor's main loop in a goroutine because it doesn't return until context is cancelled, or it
	// runs into an error.
	go func() {
		if err := s.StartMonitoring(ctx); err != nil {
			log.Fatal(err)
		}
	}()
}
```
