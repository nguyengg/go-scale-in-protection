package sip

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/ec2/imds"
	"github.com/aws/aws-sdk-go-v2/service/autoscaling"
	"io"
	"log"
	"sync"
	"time"
)

// ScaleInProtector monitors active statuses of several workers to enable or disable scale-in protection accordingly.
//
// The zero value ScaleInProtector is ready for use. StartMonitoring should be called in a separate goroutine to start
// the monitoring loop. Each worker then calls either SignalActive or SignalIdle at the appropriate time, passing the
// worker identifier as the sole argument.
type ScaleInProtector struct {
	// InstanceId is the instance Id to enable or disable scale-in protection.
	//
	// If not specified, an imds.Client created from the default aws.Config (config.LoadDefaultConfig) will be used to
	// detect the instance Id of the host. If one cannot be detected, StartMonitoring will return a non-nil error.
	InstanceId string
	// AutoScalingGroupName is the name of the Auto Scaling group that contains the instance specified by InstanceId.
	//
	// If not specified, the given AutoScaling will be used to find the Auto Scaling group that contains the instance
	// specified by InstanceId. If one cannot be found, StartMonitoring will return a non-nil error. If both InstanceId
	// and AutoScalingGroupName are given but the Auto Scaling group does not contain the instance, an error is also
	// returned (you cannot have the monitor effects an instance different from the one it's running on for safety). If
	// you want an option to disable this check, send a PR.
	AutoScalingGroupName string
	// AutoScaling is the client that will be used to make Auto Scaling service calls.
	//
	// If not given, an autoscaling.Client created from the default aws.Config (config.LoadDefaultConfig) will be used.
	AutoScaling AutoScalingAPIClient
	// IdleAtLeast specifies the amount of time all workers must have been idle before scale-in protection may be
	// disabled.
	//
	// The delay starts from when the last worker becomes idle. If you want to measure from the moment the first worker
	// becomes idle, send a PR (trailing vs. leading delay).
	IdleAtLeast time.Duration
	// Logger is used to log whenever the scale-in protection changes.
	//
	// Defaults to log.Default.
	Logger *log.Logger
	// Verbose enables detailed logging while development is in progress.
	Verbose bool

	// only need one mutex to guard protected and started. the remaining fields will only be accessed from the goroutine
	// that calls StartMonitoring so they don't need concurrency control.
	mu        sync.Mutex
	protected bool
	started   bool
	ach       chan string
	ich       chan string
	active    map[string]bool
}

// AutoScalingAPIClient extracts the subset of autoscaling.Client APIs that ScaleInProtector uses.
type AutoScalingAPIClient interface {
	DescribeAutoScalingInstances(context.Context, *autoscaling.DescribeAutoScalingInstancesInput, ...func(*autoscaling.Options)) (*autoscaling.DescribeAutoScalingInstancesOutput, error)
	SetInstanceProtection(context.Context, *autoscaling.SetInstanceProtectionInput, ...func(*autoscaling.Options)) (*autoscaling.SetInstanceProtectionOutput, error)
}

// StartMonitoring starts the monitoring loop.
//
// The method should be called in a separate goroutine because it will not return until the given context is cancelled;
// [context.Context.Err] is always returned in this case.
//
// The method panics if it has been called more than once.
func (s *ScaleInProtector) StartMonitoring(ctx context.Context) (err error) {
	if err = s.init(ctx); err != nil {
		return err
	}

	var delay *time.Timer

	for {
		// TODO there's probably a way to use an infinite time.Timer or a no-op case to simplify the two select blocks into one.
		/// but for now, two are used. the first one is when there is no pending timer to disable scale-in protection,
		// while the latter is entered only when all workers are idle with a pending disable scale-in protection timer.

		if delay == nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case id := <-s.ach:
				// enabling scale-in protection takes place right away.
				if s.active[id] {
					if s.Verbose {
						s.Logger.Printf("worker %s remains active", id)
					}
					continue
				}
				if s.Verbose {
					s.Logger.Printf("worker %s becomes active", id)
				}
				s.active[id] = true

				if err = s.toggle(ctx, true); err != nil {
					return err
				}
			case id := <-s.ich:
				// if all workers are idle then scale-in protection may be delayed or may take effect right away.
				// unlike active which immediately enable scale-in protection, all workers must be idle before scale-in
				// protection is eligible for disabling.
				if s.Verbose {
					if s.active[id] {
						s.Logger.Printf("worker %s becomes idle", id)
					} else {
						s.Logger.Printf("worker %s remains idle", id)
					}
				}

				delete(s.active, id)
				if n := len(s.active); n > 0 {
					if s.Verbose {
						s.Logger.Printf("%d workers are still active, will not disable scale-in protection", n)
					}
					continue
				}

				if s.IdleAtLeast > 0 {
					if s.IsProtectedFromScaleIn() {
						s.Logger.Printf("all workers idle, will disable scale-in protection at %s (in %.4f seconds)", time.Now().Add(s.IdleAtLeast).Format(time.RFC3339), s.IdleAtLeast.Seconds())
						delay = time.NewTimer(s.IdleAtLeast)
					}

					continue
				}

				if err = s.toggle(ctx, false); err != nil {
					return err
				}
			}

			continue
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-delay.C:
			delay.Stop()
			delay = nil

			if err = s.toggle(ctx, false); err != nil {
				return err
			}
		case id := <-s.ach:
			// a worker becomes active so do not disable scale-in protection.
			delay.Stop()
			delay = nil

			if s.Verbose {
				s.Logger.Printf("worker %s becomes active", id)
			}
			s.active[id] = true

			if err = s.toggle(ctx, true); err != nil {
				return err
			}
		case id := <-s.ich:
			// all workers should still be idle so do nothing here.
			if s.Verbose {
				s.Logger.Printf("worker %s remains idle", id)
			}
		}
	}
}

// IsProtectedFromScaleIn returns the internal flag reflecting whether scale-in protection is enabled or not.
//
// It is entirely possible for the monitor to think it has scale-in protection enabled while an external action may have
// disabled it and vice versa.
func (s *ScaleInProtector) IsProtectedFromScaleIn() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.protected
}

// SignalActive should be called by a worker passing its identifier when it has an active job.
func (s *ScaleInProtector) SignalActive(id string) {
	s.ach <- id
}

// SignalIdle should be called by a worker passing its identifier when it has become idle.
func (s *ScaleInProtector) SignalIdle(id string) {
	s.ich <- id
}

func (s *ScaleInProtector) init(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.started {
		panic("monitor has already been started once")
	}

	if s.Logger == nil {
		s.Logger = log.Default()
	}

	if s.IdleAtLeast < 0 {
		return fmt.Errorf("cannot specify negative IdleAtLeast: %d", s.IdleAtLeast)
	}

	if s.InstanceId == "" {
		cfg, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			return fmt.Errorf("create default config error: %w", err)
		}

		imdsClient := imds.NewFromConfig(cfg)
		if output, err := imdsClient.GetMetadata(ctx, &imds.GetMetadataInput{Path: "instance-id"}); err != nil {
			return fmt.Errorf("get IMDS metadata error: %w", err)
		} else if data, err := io.ReadAll(output.Content); err != nil {
			return fmt.Errorf("read IMDS metadata error: %w", err)
		} else {
			s.InstanceId = string(data)
		}
	}

	if s.AutoScaling == nil {
		cfg, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			return fmt.Errorf("create default config error: %w", err)
		}

		s.AutoScaling = autoscaling.NewFromConfig(cfg)
	}

	if output, err := s.AutoScaling.DescribeAutoScalingInstances(ctx, &autoscaling.DescribeAutoScalingInstancesInput{
		InstanceIds: []string{s.InstanceId},
	}); err != nil {
		return fmt.Errorf("describe Auto Scaling instances error: %w", err)
	} else {
		for _, instance := range output.AutoScalingInstances {
			if s.InstanceId == aws.ToString(instance.InstanceId) {
				if s.AutoScalingGroupName != "" && s.AutoScalingGroupName != aws.ToString(instance.AutoScalingGroupName) {
					return fmt.Errorf("mismatched Auto Scaling group name; given %s but instance actually belongs to %s", s.AutoScaling, aws.ToString(instance.AutoScalingGroupName))
				}
				s.AutoScalingGroupName = aws.ToString(instance.AutoScalingGroupName)
				s.protected = aws.ToBool(instance.ProtectedFromScaleIn)
				break
			}
		}

		if s.AutoScalingGroupName == "" {
			return fmt.Errorf("instance is detached from any Auto Scaling group")
		}
	}

	s.ach = make(chan string)
	s.ich = make(chan string)
	s.active = make(map[string]bool)

	return nil
}

func (s *ScaleInProtector) toggle(ctx context.Context, protected bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.protected == protected {
		if s.Verbose {
			s.Logger.Printf("no changes to scale-in protection (%t)", protected)
		}
		return nil
	}

	s.Logger.Printf("setting scale-in protection to %t", protected)
	if _, err := s.AutoScaling.SetInstanceProtection(ctx, &autoscaling.SetInstanceProtectionInput{
		AutoScalingGroupName: &s.AutoScalingGroupName,
		InstanceIds:          []string{s.InstanceId},
		ProtectedFromScaleIn: &protected,
	}); err != nil {
		return fmt.Errorf("set scale-in protection to %t error: %w", protected, err)
	}

	s.protected = protected
	return nil
}
